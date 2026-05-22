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
import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.types.CallToolResult
import io.modelcontextprotocol.kotlin.sdk.types.TextContent
import io.modelcontextprotocol.kotlin.sdk.types.ToolSchema
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.getReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest

private val JSON_PRINTER: JsonFormat.Printer =
  JsonFormat.printer().omittingInsignificantWhitespace()

fun Server.registerReportingSetTools(
  client: ReportingPublicApiClient,
  getBearerToken: () -> String,
) {
  addTool(
    name = "create_reporting_set",
    description =
      "Create a ReportingSet. Provide either a primitive (with cmms_event_groups) or " +
        "composite (with a set expression). Set campaign_group to the ReportingSet's own " +
        "name to mark it as a campaign group for BasicReports.",
    inputSchema =
      ToolSchema(
        properties =
          buildJsonObject {
            putJsonObject("parent") {
              put("type", "string")
              put("description", "MeasurementConsumer resource name")
            }
            putJsonObject("reporting_set_id") {
              put("type", "string")
              put("description", "RFC-1034 lower-case identifier for the reporting set")
            }
            putJsonObject("reporting_set") {
              put("type", "object")
              put("description", "ReportingSet message in JSON")
            }
          },
        required = listOf("parent", "reporting_set_id", "reporting_set"),
      ),
  ) { request ->
    val args = request.arguments!!
    val stubs = client.withBearerToken(getBearerToken())

    val rsBuilder = ReportingSet.newBuilder()
    JsonFormat.parser().merge(args.getValue("reporting_set").toString(), rsBuilder)

    val grpcRequest = createReportingSetRequest {
      parent = args.getString("parent")
      reportingSetId = args.getString("reporting_set_id")
      reportingSet = rsBuilder.build()
    }

    val result = stubs.reportingSets.createReportingSet(grpcRequest)
    CallToolResult(content = listOf(TextContent(JSON_PRINTER.print(result))))
  }

  addTool(
    name = "get_reporting_set",
    description = "Get a ReportingSet by resource name.",
    inputSchema =
      ToolSchema(
        properties =
          buildJsonObject {
            putJsonObject("name") {
              put("type", "string")
              put("description", "ReportingSet resource name")
            }
          },
        required = listOf("name"),
      ),
  ) { request ->
    val stubs = client.withBearerToken(getBearerToken())
    val grpcRequest = getReportingSetRequest { name = request.arguments!!.getString("name") }
    val result = stubs.reportingSets.getReportingSet(grpcRequest)
    CallToolResult(content = listOf(TextContent(JSON_PRINTER.print(result))))
  }

  addTool(
    name = "list_reporting_sets",
    description = "List ReportingSets for a MeasurementConsumer. Supports pagination.",
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
              put("description", "Maximum reporting sets to return (max 1000, default 50)")
            }
            putJsonObject("page_token") {
              put("type", "string")
              put("description", "Pagination token from a previous response")
            }
          },
        required = listOf("parent"),
      ),
  ) { request ->
    val args = request.arguments!!
    val stubs = client.withBearerToken(getBearerToken())
    val grpcRequest = listReportingSetsRequest {
      parent = args.getString("parent")
      args.getIntOrNull("page_size")?.let { pageSize = it }
      args.getStringOrNull("page_token")?.let { pageToken = it }
    }

    val result = stubs.reportingSets.listReportingSets(grpcRequest)
    CallToolResult(content = listOf(TextContent(JSON_PRINTER.print(result))))
  }
}
