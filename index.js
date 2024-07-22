import "dotenv/config";
import express from "express";
import { createClient } from "@supabase/supabase-js";
import Parser from "rss-parser";
import Anthropic from "@anthropic-ai/sdk";
import cron from "node-cron";

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

const parser = new Parser();

// Helper function to convert bytes to MB
function bytesToMB(bytes) {
  return (bytes / 1024 / 1024).toFixed(2);
}

// Helper function to process items in batches
async function processBatch(serviceId, items, batchSize) {
  const results = [];
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const batchPromises = batch.map((item) =>
      supabase.rpc("upsert_service_event", {
        p_service_id: serviceId,
        p_guid: item.guid,
        p_title: item.title,
        p_description: item.content,
        p_pub_date: item.isoDate,
      })
    );
    const batchResults = await Promise.all(batchPromises);
    results.push(...batchResults);
  }
  return { serviceId, results };
}

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
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const result = await response.text();
    console.log("Cron job completed:", result);
  } catch (error) {
    console.error("Error in cron job:", error.message);
  }
});

app.post("/summarize-event", async (req, res) => {
  const eventId = req.body.eventId;
  try {
    const { data: eventData, error: eventError } = await supabase
      .from("service_events")
      .select("*")
      .eq("id", eventId);

    if (eventError || !eventData || eventData.length === 0) {
      throw new Error(
        `Failed to fetch service event: ${
          eventError?.message || "Service event not found"
        }`
      );
    }

    /* Removed severity
        If the description contains the word "major", "major outage" or "downtime" the severity should be set to "major".
        
        If the description contains "partial" or "partial outage" the severity should be "partial". 
        
        If the description contains the word "degraded" the severity should be set to "degraded". 

        If it contains "minor" the severity should be set to "minor".
        
        If the description contains "scheduled" or "maintenance" the severity should be set to "scheduled".

        If the accumulated time is greater than 1440 minutes, and the description does not include "scheduled" or "maintenance", the severity should be set to "major".

        If it cannot be determined, use best judgement - for example, "delayed" in the description would mean "partial" severity.
    */

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

    return res.status(200).send(`Successfully summarized event ${eventId}`);
  } catch (error) {
    console.error(error);
    return res.status(500).send(`Error: Unable to summarize ${eventId}`);
  }
});

app.post("/process-feeds", async (req, res) => {
  const startMemory = process.memoryUsage();

  try {
    const { data: serviceData, error: serviceError } = await supabase
      .from("services")
      .select("id, feed_url");

    if (serviceError) {
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

    const allResults = await Promise.all(allProcessingPromises);

    allResults.forEach(({ results, batchAmount }) => {
      console.log({ results });
      const successfulOps = results.filter((result) => result !== null).length;

      console.log(`${successfulOps}/${batchAmount} operations successful`);
    });

    res.status(200).send("Successfully parsed RSS feeds and items");
  } catch (error) {
    console.error(error);
    res.status(500).send("Error while processing the feeds");
  } finally {
    const endMemory = process.memoryUsage();
    const memoryDiff = {
      rss: bytesToMB(endMemory.rss - startMemory.rss),
      heapTotal: bytesToMB(endMemory.heapTotal - startMemory.heapTotal),
      heapUsed: bytesToMB(endMemory.heapUsed - startMemory.heapUsed),
      external: bytesToMB(endMemory.external - startMemory.external),
    };

    console.log("Memory usage difference (in MB):");
    console.log(memoryDiff);
  }
});

app.listen(port, () => {
  console.log(`🛜 Basestatus processor is running on port ${port}`);
});
