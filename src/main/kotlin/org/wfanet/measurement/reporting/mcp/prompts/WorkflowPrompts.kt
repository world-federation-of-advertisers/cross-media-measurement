/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.mcp.prompts

import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.types.GetPromptResult
import io.modelcontextprotocol.kotlin.sdk.types.PromptArgument
import io.modelcontextprotocol.kotlin.sdk.types.PromptMessage
import io.modelcontextprotocol.kotlin.sdk.types.Role
import io.modelcontextprotocol.kotlin.sdk.types.TextContent

fun Server.registerWorkflowPrompts() {
  addPrompt(
    name = "create-reach-report",
    description = "Walk through creation of a reach-oriented BasicReport step by step.",
    arguments =
      listOf(
        PromptArgument(
          name = "measurement_consumer",
          description = "MeasurementConsumer resource name",
          required = true,
        ),
        PromptArgument(
          name = "campaign_description",
          description = "Natural-language description of the target campaign",
          required = false,
        ),
      ),
  ) { request ->
    val mc = request.arguments?.get("measurement_consumer") ?: "{measurement_consumer}"
    val campaignDesc = request.arguments?.get("campaign_description")
    val campaignContext =
      if (campaignDesc != null) {
        "\nThe user is looking for a campaign matching: \"$campaignDesc\"\n"
      } else {
        ""
      }

    GetPromptResult(
      description = "Create a reach-oriented BasicReport",
      messages =
        listOf(
          PromptMessage(
            role = Role.User,
            content =
              TextContent(
                text =
                  """Help me create a reach report for measurement consumer: $mc
$campaignContext
Follow these steps:
1. Call list_event_groups with parent "$mc" to discover available publisher data.
2. Call list_impression_qualification_filters to see available IQF options.
3. Create a primitive reporting set using create_reporting_set with the selected cmms_event_groups.
4. Create a basic report using create_basic_report with a generated request_id (UUID4).
5. Poll get_basic_report until state becomes SUCCEEDED or FAILED, then present the results.""",
              ),
          ),
        ),
    )
  }

  addPrompt(
    name = "check-report-status",
    description = "Summarize recent in-progress BasicReports.",
    arguments =
      listOf(
        PromptArgument(
          name = "measurement_consumer",
          description = "MeasurementConsumer resource name",
          required = true,
        ),
        PromptArgument(
          name = "created_after",
          description = "RFC 3339 timestamp; only check reports created after this time",
          required = false,
        ),
      ),
  ) { request ->
    val mc = request.arguments?.get("measurement_consumer") ?: "{measurement_consumer}"
    val createdAfter = request.arguments?.get("created_after")
    val filterClause =
      if (createdAfter != null) " and create_time_after \"$createdAfter\"" else ""

    GetPromptResult(
      description = "Check status of recent BasicReports",
      messages =
        listOf(
          PromptMessage(
            role = Role.User,
            content =
              TextContent(
                text =
                  """Check the status of recent reports for measurement consumer: $mc

Follow these steps:
1. Call list_basic_reports with parent "$mc"$filterClause.
2. Filter returned reports client-side to show RUNNING reports.
3. Summarize each report's name, create time, campaign group, and state.
4. For any newly completed reports (SUCCEEDED), fetch with get_basic_report and present results.

Note: The API supports filtering by create_time_after but not by report state.""",
              ),
          ),
        ),
    )
  }

  addPrompt(
    name = "explore-available-data",
    description = "Help understand what data is available before creating a report.",
    arguments =
      listOf(
        PromptArgument(
          name = "measurement_consumer",
          description = "MeasurementConsumer resource name",
          required = true,
        ),
      ),
  ) { request ->
    val mc = request.arguments?.get("measurement_consumer") ?: "{measurement_consumer}"

    GetPromptResult(
      description = "Explore available measurement data",
      messages =
        listOf(
          PromptMessage(
            role = Role.User,
            content =
              TextContent(
                text =
                  """Help me understand what data is available for measurement consumer: $mc

Follow these steps:
1. Call list_event_groups with parent "$mc" to discover publisher data. Use view WITH_ACTIVITY_SUMMARY.
2. Call list_reporting_sets with parent "$mc" to see existing reporting sets.
3. Call list_impression_qualification_filters to see available IQF options.
4. Summarize: which publishers have data, what media types, what date ranges, which reporting sets exist.""",
              ),
          ),
        ),
    )
  }
}
