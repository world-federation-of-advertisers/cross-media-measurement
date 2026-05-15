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
import org.wfanet.measurement.reporting.v2alpha.getImpressionQualificationFilterRequest
import org.wfanet.measurement.reporting.v2alpha.listImpressionQualificationFiltersRequest

private val JSON_PRINTER: JsonFormat.Printer =
  JsonFormat.printer().omittingInsignificantWhitespace()

fun McpServer.registerIqfTools(
  client: ReportingPublicApiClient,
  getBearerToken: () -> String,
) {
  addTool(
    McpTool(
      name = "get_impression_qualification_filter",
      description = "Get an ImpressionQualificationFilter by resource name.",
      inputSchema = inputSchema {
        stringProperty("name", "IQF resource name, e.g. impressionQualificationFilters/{iqf_id}")
        required("name")
      },
    ) { args ->
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = getImpressionQualificationFilterRequest {
        name = args.get("name").asString
      }
      JSON_PRINTER.print(stubs.impressionQualificationFilters.getImpressionQualificationFilter(grpcRequest))
    },
  )

  addTool(
    McpTool(
      name = "list_impression_qualification_filters",
      description =
        "List all ImpressionQualificationFilters. These are top-level resources " +
          "(not scoped to a MeasurementConsumer). Supports pagination.",
      inputSchema = inputSchema {
        intProperty("page_size", "Maximum filters to return (max 100, default 50)")
        stringProperty("page_token", "Token from a previous list response for pagination")
      },
    ) { args ->
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = listImpressionQualificationFiltersRequest {
        if (args.has("page_size")) pageSize = args.get("page_size").asInt
        if (args.has("page_token")) pageToken = args.get("page_token").asString
      }
      JSON_PRINTER.print(stubs.impressionQualificationFilters.listImpressionQualificationFilters(grpcRequest))
    },
  )
}
