export async function processBatch(serviceId, items, batchSize) {
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

export async function processEvent(event, supabase) {
  try {
    const msg = await anthropic.messages.create({
      model: "claude-3-5-sonnet-20240620",
      max_tokens: 4096,
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
          content: [{ type: "text", text: "-" }],
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
          content: [{ type: "text", text: event.description }],
        },
      ],
    });

    const res = JSON.parse(msg.content[0].text);

    if (!res) {
      throw new Error("Failed to generate message from Anthropic API");
    }

    const { error: upsertError } = await supabase
      .from("service_events")
      .update({ ...res })
      .eq("id", event.id)
      .select();

    if (upsertError) {
      throw new Error(`Failed to upsert data: ${upsertError.message}`);
    }

    console.log(`Successfully summarized event ${event.id}: ${res}`);
  } catch (error) {
    console.error(`Error processing event ${event.id}:`, error);
    throw new Error(`Error processing event`);
  }
}
