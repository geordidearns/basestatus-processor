import "dotenv/config";
import express from "express";
import { createClient } from "@supabase/supabase-js";
import Parser from "rss-parser";
import Anthropic from "@anthropic-ai/sdk";
import cron from "node-cron";
import { LogSnag } from "@logsnag/node";

import { processEvent, processBatch } from "./utils.js";

const app = express();
const port = process.env.PORT || 8080;

app.use(express.json());

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY ?? "",
});

const logsnag = new LogSnag({
  token: process.env.LOGSNAG_API_KEY,
  project: "basestatus",
});

const parser = new Parser();

let lastSuccessfulCronJob = null;

cron.schedule("* * * * *", async () => {
  console.log("Running feed processing cron job");
  try {
    const response = await fetch(
      `${process.env.BASESTATUS_PROCESSOR_URL}/process-feeds`,
      {
        method: "POST",
      }
    );

    if (!response.ok) {
      console.log(response.json());
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    lastSuccessfulCronJob = new Date();
    console.log(
      `Last successful cron job: ${lastSuccessfulCronJob.toISOString()}`
    );
  } catch (error) {
    console.error("Error in cron job:", error.message);
    // await logsnag.track({
    //   channel: "processing-errors",
    //   event: "Error in cron job",
    //   icon: "🚨",
    //   notify: true,
    //   tags: {
    //     source: "cron-scheduler",
    //   },
    // });
  }
});

app.post("/summarize-event", async (req, res) => {
  const eventId = req.body.eventId;

  console.log(`Started summarizing event ${eventId}`);

  try {
    const { data: eventData, error: eventError } = await supabase
      .from("service_events")
      .select("*")
      .eq("id", eventId);

    if (eventError || !eventData || eventData.length === 0) {
      // await logsnag.track({
      //   channel: "processing-errors",
      //   event: "Failed to fetch service event to summarize",
      //   icon: "🚨",
      //   notify: true,
      //   tags: {
      //     source: "summarize-event",
      //     eventId: eventId,
      //   },
      // });

      throw new Error(
        `Failed to fetch service event: ${
          eventError?.message || "Service event not found"
        }`
      );
    }

    const msg = await anthropic.messages.create({
      model: "claude-3-5-sonnet-20240620",
      max_tokens: 2000,
      temperature: 0,
      system: `
        You parse HTML and text and return only valid JSON.

        Do not add properties besides status, translated_description, accumulated_time_minutes and severity.

        Do not change the original event information or original description except for the dates which will be converted.

        The dates should be converted into UTC 24 hour time and UTC timezone.
        
        Convert the weekday to be correct for the UTC timezone also.

        Status should be one of the following: investigating, ongoing, resolved or maintenance. Events that are identified, monitoring or update should have the status ongoing.

        Severity should be determined as accurately as possible based on the description, and given one of the following values:

        "critical", "major", "minor", "maintenance"

        these are defined as:

        critical: a critical incident with very high impact. Examples of this can be a complete outage, confidentiality or privacy is breached or a customer data loss incident.

        major: a major incident with high impact. Examples of this can be a partial outage, a significant performance degradation or a data integrity issue, a customer-facing service is unavailable for a large subset of customers.

        minor: a minor incident with low impact. Examples of this can be a minor performance degradation, a non-customer facing service is unavailable for a smaller subset of customers.

        maintenance: a planned maintenance event. Examples of this can be a deployment, a database migration or a network configuration change.

        The translated_description should include all individual event updates from the provided HTML.

        You will calculate the accumulated time between event updates and return the value in minutes extremely accurately. 

        Only calculate the accumulated_time_minutes if the event is resolved,or if the event is maintenance and contains the scheduled time range which should be used to accurately calculate the accumulated_time_minutes.

        If the event description contains the range of time the event occurred, use that to calculate the accumulated time.
        
        If the event is scheduled maintenance, it is important to only calculate between progress and completed or resolved updates.
      `,
      messages: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "-",
            },
          ],
        },
        {
          role: "assistant",
          content: [
            {
              type: "text",
              text: JSON.stringify({
                status: "resolved",
                translated_description:
                  "<p><small>Jul <var data-var='date'>19</var>, <var data-var='time'>17:57</var> UTC</small><br><strong>Resolved</strong> - This incident has been resolved.</p><p><small>Jul <var data-var='date'>19</var>, <var data-var='time'>14:29</var> UTC</small><br><strong>Investigating</strong> - Records are not being enriched for some customers using Salesforce Enrichment on Platform</p>",
                accumulated_time_minutes: 5,
                severity: "minor",
              }),
            },
          ],
        },
        {
          role: "user",
          content: [
            {
              type: "text",
              text: eventData[0].description,
            },
          ],
        },
      ],
    });

    const result = JSON.parse(msg.content[0].text);

    if (!result) {
      // await logsnag.track({
      //   channel: "processing-errors",
      //   event: "Failed to generate a result from Anthropic API",
      //   icon: "🚨",
      //   notify: true,
      //   tags: {
      //     source: "summarize-event",
      //     eventId: eventId,
      //   },
      // });

      throw new Error("Failed to generate message from Anthropic API");
    }

    const { error: upsertError } = await supabase
      .from("service_events")
      .update({ ...result })
      .eq("id", eventId)
      .select();

    if (upsertError) {
      throw new Error(`Failed to upsert data: ${upsertError.message}`);
    }

    console.log(`Successfully summarized event ${eventId}`);
    return res.status(200).send(`Successfully summarized event ${eventId}`);
  } catch (error) {
    console.error(error);
    // await logsnag.track({
    //   channel: "processing-errors",
    //   event: "Failed to summarize a event",
    //   icon: "🚨",
    //   notify: true,
    //   tags: {
    //     source: "summarize-event",
    //     eventId: eventId,
    //   },
    // });
    return res.status(500).send(`Error: Unable to summarize ${eventId}`);
  }
});

app.post("/process-events", async (req, res) => {
  const BATCH_SIZE = 10;

  try {
    const { data: events, error: fetchError } = await supabase
      .from("service_events")
      .select("*")
      .is("translated_description", null);

    if (fetchError) {
      throw new Error(`Failed to fetch service events: ${fetchError.message}`);
    }

    for (let i = 0; i < events.length; i += BATCH_SIZE) {
      const batch = events.slice(i, i + BATCH_SIZE);
      // @ts-ignore
      await Promise.all(batch.map((event) => processEvent(event, supabase)));
    }

    return new Response(`Successfully processed ${events.length} events`);
  } catch (error) {
    console.error(error);
    // await logsnag.track({
    //   channel: "processing-events",
    //   event: "Failed to process the events",
    //   icon: "🚨",
    //   notify: true,
    //   tags: {
    //     source: "process-events",
    //   },
    // });
    res.status(500).send("Error while processing the events");
  }
});

app.post("/process-feeds", async (req, res) => {
  try {
    const { data: serviceData, error: serviceError } = await supabase
      .from("services")
      .select("id, feed_url");

    if (serviceError) {
      // await logsnag.track({
      //   channel: "processing-errors",
      //   event: "Failed to fetch services",
      //   icon: "🚨",
      //   notify: true,
      //   tags: {
      //     source: "process-feeds",
      //   },
      // });

      throw new Error(serviceError.message);
    }

    const servicePromises = serviceData.map((service) =>
      parser
        .parseURL(service.feed_url)
        .then((result) => ({ ...result, serviceId: service.id }))
    );

    const serviceResults = await Promise.all(servicePromises);

    const batchSize = 10;
    const allProcessingPromises = serviceResults.map((service) =>
      processBatch(service.serviceId, service.items, batchSize)
    );

    await Promise.all(allProcessingPromises);

    res.status(200).send("Successfully parsed RSS feeds and items");
  } catch (error) {
    console.error(error);

    // await logsnag.track({
    //   channel: "processing-errors",
    //   event: "Failed to process feeds",
    //   icon: "🚨",
    //   notify: true,
    //   tags: {
    //     source: "process-feeds",
    //   },
    // });

    res.status(500).send("Error while processing the feeds");
  }
});

app.listen(port, () => {
  console.log(`🛜 Basestatus processor is running on port ${port}`);
});
