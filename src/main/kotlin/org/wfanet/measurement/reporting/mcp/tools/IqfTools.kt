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
import org.wfanet.measurement.reporting.v2alpha.getImpressionQualificationFilterRequest
import org.wfanet.measurement.reporting.v2alpha.listImpressionQualificationFiltersRequest

fun Server.registerIqfTools(client: ReportingPublicApiClient, getBearerToken: () -> String) {
  addTool(
    name = "get_impression_qualification_filter",
    description = "Get an ImpressionQualificationFilter by resource name.",
    inputSchema =
      ToolSchema(
        properties =
          buildJsonObject {
            putJsonObject("name") {
              put("type", "string")
              put("description", "IQF resource name, e.g. impressionQualificationFilters/{id}")
            }
          },
        required = listOf("name"),
      ),
    toolAnnotations = ToolAnnotations(readOnlyHint = true),
  ) { request ->
    ToolSupport.handleToolCall {
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = getImpressionQualificationFilterRequest {
        name = ToolSupport.getString(request.arguments!!, "name")
      }
      ToolSupport.PROTO_JSON_PRINTER.print(
        stubs.impressionQualificationFilters.getImpressionQualificationFilter(grpcRequest)
      )
    }
  }

  addTool(
    name = "list_impression_qualification_filters",
    description =
      "List all ImpressionQualificationFilters. Top-level resources, not scoped to a " +
        "MeasurementConsumer. Supports pagination.",
    inputSchema =
      ToolSchema(
        properties =
          buildJsonObject {
            putJsonObject("page_size") {
              put("type", "integer")
              put("description", "Maximum filters to return (max 100, default 50)")
            }
            putJsonObject("page_token") {
              put("type", "string")
              put("description", "Pagination token from a previous response")
            }
          }
      ),
    toolAnnotations = ToolAnnotations(readOnlyHint = true),
  ) { request ->
    ToolSupport.handleToolCall {
      val args = request.arguments
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = listImpressionQualificationFiltersRequest {
        if (args != null) {
          ToolSupport.getIntOrNull(args, "page_size")?.let { pageSize = it }
          ToolSupport.getStringOrNull(args, "page_token")?.let { pageToken = it }
        }
      }
      ToolSupport.PROTO_JSON_PRINTER.print(
        stubs.impressionQualificationFilters.listImpressionQualificationFilters(grpcRequest)
      )
    }
  }
}
