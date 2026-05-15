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

import com.google.gson.JsonObject
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.reporting.mcp.McpServer
import org.wfanet.measurement.reporting.mcp.McpTool
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequestKt
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsRequest

private val JSON_PRINTER: JsonFormat.Printer =
  JsonFormat.printer().omittingInsignificantWhitespace()

fun McpServer.registerBasicReportTools(
  client: ReportingPublicApiClient,
  getBearerToken: () -> String,
) {
  addTool(
    McpTool(
      name = "create_basic_report",
      description =
        "Create a BasicReport. The report runs asynchronously; poll with get_basic_report " +
          "until state is SUCCEEDED or FAILED. Supply request_id for idempotent retries.",
      inputSchema = inputSchema {
        stringProperty("parent", "MeasurementConsumer resource name, e.g. measurementConsumers/{mc_id}")
        stringProperty("basic_report_id", "RFC-1034 lower-case identifier for the report")
        stringProperty("request_id", "Optional UUID4 for idempotent retries")
        objectProperty("basic_report", "BasicReport message in JSON")
        required("parent", "basic_report_id", "basic_report")
      },
    ) { args ->
      val stubs = client.withBearerToken(getBearerToken())
      val basicReportBuilder = BasicReport.newBuilder()
      JsonFormat.parser().merge(args.getAsJsonObject("basic_report").toString(), basicReportBuilder)

      val grpcRequest = createBasicReportRequest {
        parent = args.get("parent").asString
        basicReportId = args.get("basic_report_id").asString
        basicReport = basicReportBuilder.build()
        if (args.has("request_id")) {
          requestId = args.get("request_id").asString
        }
      }

      JSON_PRINTER.print(stubs.basicReports.createBasicReport(grpcRequest))
    },
  )

  addTool(
    McpTool(
      name = "get_basic_report",
      description =
        "Get a BasicReport by resource name. Use to poll report status until " +
          "state is SUCCEEDED or FAILED.",
      inputSchema = inputSchema {
        stringProperty(
          "name",
          "BasicReport resource name, e.g. measurementConsumers/{mc_id}/basicReports/{report_id}",
        )
        required("name")
      },
    ) { args ->
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = getBasicReportRequest { name = args.get("name").asString }
      JSON_PRINTER.print(stubs.basicReports.getBasicReport(grpcRequest))
    },
  )

  addTool(
    McpTool(
      name = "list_basic_reports",
      description =
        "List BasicReports for a MeasurementConsumer. Supports pagination and " +
          "filtering by create time.",
      inputSchema = inputSchema {
        stringProperty("parent", "MeasurementConsumer resource name, e.g. measurementConsumers/{mc_id}")
        intProperty("page_size", "Maximum reports to return (max 25, default 10)")
        stringProperty("page_token", "Token from a previous list response for pagination")
        stringProperty("create_time_after", "RFC 3339 timestamp; only return reports created after this time")
        required("parent")
      },
    ) { args ->
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = listBasicReportsRequest {
        parent = args.get("parent").asString
        if (args.has("page_size")) pageSize = args.get("page_size").asInt
        if (args.has("page_token")) pageToken = args.get("page_token").asString
        if (args.has("create_time_after")) {
          filter = ListBasicReportsRequestKt.filter {
            createTimeAfter = Timestamps.parse(args.get("create_time_after").asString)
          }
        }
      }
      JSON_PRINTER.print(stubs.basicReports.listBasicReports(grpcRequest))
    },
  )
}
