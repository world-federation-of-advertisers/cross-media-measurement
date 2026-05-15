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
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.getReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest

private val JSON_PRINTER: JsonFormat.Printer =
  JsonFormat.printer().omittingInsignificantWhitespace()

fun McpServer.registerReportingSetTools(
  client: ReportingPublicApiClient,
  getBearerToken: () -> String,
) {
  addTool(
    McpTool(
      name = "create_reporting_set",
      description =
        "Create a ReportingSet. Provide either a primitive (with cmms_event_groups) or " +
          "composite (with a set expression). Set campaign_group to the ReportingSet's own " +
          "name to mark it as a campaign group for BasicReports.",
      inputSchema = inputSchema {
        stringProperty("parent", "MeasurementConsumer resource name, e.g. measurementConsumers/{mc_id}")
        stringProperty("reporting_set_id", "RFC-1034 lower-case identifier for the reporting set")
        objectProperty("reporting_set", "ReportingSet message in JSON")
        required("parent", "reporting_set_id", "reporting_set")
      },
    ) { args ->
      val stubs = client.withBearerToken(getBearerToken())
      val rsBuilder = ReportingSet.newBuilder()
      JsonFormat.parser().merge(args.getAsJsonObject("reporting_set").toString(), rsBuilder)

      val grpcRequest = createReportingSetRequest {
        parent = args.get("parent").asString
        reportingSetId = args.get("reporting_set_id").asString
        reportingSet = rsBuilder.build()
      }
      JSON_PRINTER.print(stubs.reportingSets.createReportingSet(grpcRequest))
    },
  )

  addTool(
    McpTool(
      name = "get_reporting_set",
      description = "Get a ReportingSet by resource name.",
      inputSchema = inputSchema {
        stringProperty("name", "ReportingSet resource name, e.g. measurementConsumers/{mc_id}/reportingSets/{rs_id}")
        required("name")
      },
    ) { args ->
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = getReportingSetRequest { name = args.get("name").asString }
      JSON_PRINTER.print(stubs.reportingSets.getReportingSet(grpcRequest))
    },
  )

  addTool(
    McpTool(
      name = "list_reporting_sets",
      description = "List ReportingSets for a MeasurementConsumer. Supports pagination.",
      inputSchema = inputSchema {
        stringProperty("parent", "MeasurementConsumer resource name, e.g. measurementConsumers/{mc_id}")
        intProperty("page_size", "Maximum reporting sets to return (max 1000, default 50)")
        stringProperty("page_token", "Token from a previous list response for pagination")
        required("parent")
      },
    ) { args ->
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = listReportingSetsRequest {
        parent = args.get("parent").asString
        if (args.has("page_size")) pageSize = args.get("page_size").asInt
        if (args.has("page_token")) pageToken = args.get("page_token").asString
      }
      JSON_PRINTER.print(stubs.reportingSets.listReportingSets(grpcRequest))
    },
  )
}
