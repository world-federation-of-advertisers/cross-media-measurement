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

import com.google.protobuf.util.Timestamps
import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.types.ToolAnnotations
import io.modelcontextprotocol.kotlin.sdk.types.ToolSchema
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequestKt
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsRequest

fun Server.registerBasicReportTools(
  client: ReportingPublicApiClient,
  getBearerToken: () -> String,
) {
  addTool(
    name = "create_basic_report",
    description =
      "Create a BasicReport. The report runs asynchronously; poll with get_basic_report " +
        "until state is SUCCEEDED or FAILED. Supply request_id for idempotent retries.",
    inputSchema =
      ToolSchema(
        properties =
          buildJsonObject {
            putJsonObject("parent") {
              put("type", "string")
              put("description", "MeasurementConsumer resource name")
            }
            putJsonObject("basic_report_id") {
              put("type", "string")
              put("description", "RFC-1034 lower-case identifier for the report")
            }
            putJsonObject("request_id") {
              put("type", "string")
              put("description", "Optional UUID4 for idempotent retries")
            }
            putJsonObject("basic_report") {
              put("type", "object")
              put(
                "description",
                "BasicReport message in JSON. Key fields: campaign_group (resource name), " +
                  "reporting_interval (report_start, report_end), " +
                  "impression_qualification_filters, result_group_specs.",
              )
            }
          },
        required = listOf("parent", "basic_report_id", "basic_report"),
      ),
    toolAnnotations = ToolAnnotations(readOnlyHint = false),
  ) { request ->
    ToolSupport.handleToolCall {
      val args = request.arguments!!
      val stubs = client.withBearerToken(getBearerToken())

      val basicReportBuilder = BasicReport.newBuilder()
      ToolSupport.PROTO_JSON_PARSER.merge(
        ToolSupport.encodeJsonElement(args.getValue("basic_report")),
        basicReportBuilder
      )

      val grpcRequest = createBasicReportRequest {
        parent = ToolSupport.getString(args, "parent")
        basicReportId = ToolSupport.getString(args, "basic_report_id")
        basicReport = basicReportBuilder.build()
        ToolSupport.getStringOrNull(args, "request_id")?.let { requestId = it }
      }

      ToolSupport.PROTO_JSON_PRINTER.print(stubs.basicReports.createBasicReport(grpcRequest))
    }
  }

  addTool(
    name = "get_basic_report",
    description =
      "Get a BasicReport by resource name. Use to poll report status until " +
        "state is SUCCEEDED or FAILED.",
    inputSchema =
      ToolSchema(
        properties =
          buildJsonObject {
            putJsonObject("name") {
              put("type", "string")
              put("description", "BasicReport resource name")
            }
          },
        required = listOf("name"),
      ),
    toolAnnotations = ToolAnnotations(readOnlyHint = true),
  ) { request ->
    ToolSupport.handleToolCall {
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = getBasicReportRequest {
        name = ToolSupport.getString(request.arguments!!, "name")
      }
      ToolSupport.PROTO_JSON_PRINTER.print(stubs.basicReports.getBasicReport(grpcRequest))
    }
  }

  addTool(
    name = "list_basic_reports",
    description =
      "List BasicReports for a MeasurementConsumer. Supports pagination and " +
        "filtering by create time.",
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
              put("description", "Maximum reports to return (max 25, default 10)")
            }
            putJsonObject("page_token") {
              put("type", "string")
              put("description", "Pagination token from a previous response")
            }
            putJsonObject("create_time_after") {
              put("type", "string")
              put("description", "RFC 3339 timestamp; only return reports created after this time")
            }
          },
        required = listOf("parent"),
      ),
    toolAnnotations = ToolAnnotations(readOnlyHint = true),
  ) { request ->
    ToolSupport.handleToolCall {
      val args = request.arguments!!
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = listBasicReportsRequest {
        parent = ToolSupport.getString(args, "parent")
        ToolSupport.getIntOrNull(args, "page_size")?.let { pageSize = it }
        ToolSupport.getStringOrNull(args, "page_token")?.let { pageToken = it }
        ToolSupport.getStringOrNull(args, "create_time_after")?.let { timestamp ->
          filter =
            ListBasicReportsRequestKt.filter { createTimeAfter = Timestamps.parse(timestamp) }
        }
      }

      ToolSupport.PROTO_JSON_PRINTER.print(stubs.basicReports.listBasicReports(grpcRequest))
    }
  }
}
