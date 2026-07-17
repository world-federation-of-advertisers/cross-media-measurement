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

package org.wfanet.measurement.reporting.mcp.tools

import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.types.ToolAnnotations
import io.modelcontextprotocol.kotlin.sdk.types.ToolSchema
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.getEventGroupRequest

fun Server.registerEventGroupTools(client: ReportingPublicApiClient, getBearerToken: () -> String) {
  addTool(
    name = "get_event_group",
    description = "Get an EventGroup by resource name.",
    inputSchema =
      ToolSchema(
        properties =
          buildJsonObject {
            putJsonObject("name") {
              put("type", "string")
              put(
                "description",
                "EventGroup resource name, e.g. measurementConsumers/{mc_id}/eventGroups/{eg_id}",
              )
            }
          },
        required = listOf("name"),
      ),
    toolAnnotations = ToolAnnotations(readOnlyHint = true),
  ) { request ->
    ToolSupport.handleToolCall {
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = getEventGroupRequest {
        name = ToolSupport.getString(request.arguments!!, "name")
      }
      ToolSupport.PROTO_JSON_PRINTER.print(stubs.eventGroups.getEventGroup(grpcRequest))
    }
  }

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
                "Structured filter. Fields: cmms_data_provider_in (list of DataProvider names), " +
                  "media_types_intersect (VIDEO, DISPLAY, etc.), " +
                  "data_availability_start_time_on_or_after/before, metadata_search_query.",
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
    toolAnnotations = ToolAnnotations(readOnlyHint = true),
  ) { request ->
    ToolSupport.handleToolCall {
      val args = request.arguments!!
      val stubs = client.withBearerToken(getBearerToken())
      val builder = ListEventGroupsRequest.newBuilder()
      builder.parent = ToolSupport.getString(args, "parent")

      ToolSupport.getIntOrNull(args, "page_size")?.let { builder.pageSize = it }
      ToolSupport.getStringOrNull(args, "page_token")?.let { builder.pageToken = it }

      args["structured_filter"]?.let { filter ->
        val filterBuilder = ListEventGroupsRequest.Filter.newBuilder()
        ToolSupport.PROTO_JSON_PARSER.merge(ToolSupport.encodeJsonElement(filter), filterBuilder)
        builder.structuredFilter = filterBuilder.build()
      }

      args["order_by"]?.let { orderBy ->
        val orderByBuilder = ListEventGroupsRequest.OrderBy.newBuilder()
        ToolSupport.PROTO_JSON_PARSER.merge(ToolSupport.encodeJsonElement(orderBy), orderByBuilder)
        builder.orderBy = orderByBuilder.build()
      }

      ToolSupport.getStringOrNull(args, "view")?.let { viewName ->
        builder.view =
          try {
            EventGroup.View.valueOf(viewName)
          } catch (e: IllegalArgumentException) {
            throw IllegalArgumentException(
              "Invalid view: $viewName. Use BASIC or WITH_ACTIVITY_SUMMARY."
            )
          }
      }

      ToolSupport.PROTO_JSON_PRINTER.print(stubs.eventGroups.listEventGroups(builder.build()))
    }
  }
}
