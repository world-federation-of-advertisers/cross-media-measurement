/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.mcp.tools

import com.google.protobuf.util.JsonFormat
import org.wfanet.measurement.reporting.mcp.McpServer
import org.wfanet.measurement.reporting.mcp.McpTool
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest

private val JSON_PRINTER: JsonFormat.Printer =
  JsonFormat.printer().omittingInsignificantWhitespace()

fun McpServer.registerEventGroupTools(
  client: ReportingPublicApiClient,
  getBearerToken: () -> String,
) {
  addTool(
    McpTool(
      name = "list_event_groups",
      description =
        "List EventGroups for a MeasurementConsumer. Use to discover available publisher " +
          "data before creating reports. Supports structured filtering by data provider, " +
          "media type, date range, and metadata search.",
      inputSchema = inputSchema {
        stringProperty("parent", "MeasurementConsumer resource name, e.g. measurementConsumers/{mc_id}")
        intProperty("page_size", "Maximum event groups to return (max 1000, default 50)")
        stringProperty("page_token", "Token from a previous list response for pagination")
        objectProperty("structured_filter", "Structured filter with cmms_data_provider_in, media_types_intersect, etc.")
        objectProperty("order_by", "Order by with field (DATA_AVAILABILITY_START_TIME) and descending (bool)")
        stringProperty("view", "View: BASIC or WITH_ACTIVITY_SUMMARY")
        required("parent")
      },
    ) { args ->
      val stubs = client.withBearerToken(getBearerToken())
      val builder = ListEventGroupsRequest.newBuilder()
      builder.parent = args.get("parent").asString

      if (args.has("page_size")) builder.pageSize = args.get("page_size").asInt
      if (args.has("page_token")) builder.pageToken = args.get("page_token").asString

      if (args.has("structured_filter")) {
        val filterBuilder = ListEventGroupsRequest.Filter.newBuilder()
        JsonFormat.parser().merge(args.getAsJsonObject("structured_filter").toString(), filterBuilder)
        builder.structuredFilter = filterBuilder.build()
      }

      if (args.has("order_by")) {
        val orderByBuilder = ListEventGroupsRequest.OrderBy.newBuilder()
        JsonFormat.parser().merge(args.getAsJsonObject("order_by").toString(), orderByBuilder)
        builder.orderBy = orderByBuilder.build()
      }

      if (args.has("view")) {
        builder.view = EventGroup.View.valueOf(args.get("view").asString)
      }

      JSON_PRINTER.print(stubs.eventGroups.listEventGroups(builder.build()))
    },
  )
}
