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

import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.types.CallToolResult
import io.modelcontextprotocol.kotlin.sdk.types.TextContent
import io.modelcontextprotocol.kotlin.sdk.types.ToolSchema
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest


fun Server.registerEventGroupTools(
  client: ReportingPublicApiClient,
  getBearerToken: () -> String,
) {
  addTool(
    name = "list_event_groups",
    description =
      "List EventGroups for a MeasurementConsumer. Discover available publisher data " +
        "before creating reports. Supports structured filtering by data provider, " +
        "media type, date range, and metadata search.",
    inputSchema =
      ToolSchema(
        properties =
          buildJsonObject {
            putJsonObject("parent") {
              put("type", "string")
              put("description", "MeasurementConsumer resource name")
            }
            putJsonObject("page_size") {
              put("type", "integer")
              put("description", "Maximum event groups to return (max 1000, default 50)")
            }
            putJsonObject("page_token") {
              put("type", "string")
              put("description", "Pagination token from a previous response")
            }
            putJsonObject("structured_filter") {
              put("type", "object")
              put(
                "description",
                "Structured filter with cmms_data_provider_in, media_types_intersect, etc.",
              )
            }
            putJsonObject("order_by") {
              put("type", "object")
              put(
                "description",
                "Order by with field (DATA_AVAILABILITY_START_TIME) and descending (bool)",
              )
            }
            putJsonObject("view") {
              put("type", "string")
              put("description", "View: BASIC or WITH_ACTIVITY_SUMMARY")
            }
          },
        required = listOf("parent"),
      ),
  ) { request ->
    val args = request.arguments!!
    val stubs = client.withBearerToken(getBearerToken())
    val builder = ListEventGroupsRequest.newBuilder()
    builder.parent = args.getString("parent")

    args.getIntOrNull("page_size")?.let { builder.pageSize = it }
    args.getStringOrNull("page_token")?.let { builder.pageToken = it }

    args["structured_filter"]?.let { filter ->
      val filterBuilder = ListEventGroupsRequest.Filter.newBuilder()
      PROTO_JSON_PARSER.merge(filter.toString(), filterBuilder)
      builder.structuredFilter = filterBuilder.build()
    }

    args["order_by"]?.let { orderBy ->
      val orderByBuilder = ListEventGroupsRequest.OrderBy.newBuilder()
      PROTO_JSON_PARSER.merge(orderBy.toString(), orderByBuilder)
      builder.orderBy = orderByBuilder.build()
    }

    args.getStringOrNull("view")?.let { builder.view = EventGroup.View.valueOf(it) }

    val result = stubs.eventGroups.listEventGroups(builder.build())
    CallToolResult(content = listOf(TextContent(PROTO_JSON_PRINTER.print(result))))
  }
}
