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
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.getReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest

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
              put(
                "description",
                "ReportingSet message in JSON. Use primitive.cmms_event_groups for " +
                  "direct event groups, or composite.expression for set operations. " +
                  "Optional: display_name, campaign_group, filter, tags.",
              )
            }
          },
        required = listOf("parent", "reporting_set_id", "reporting_set"),
      ),
    toolAnnotations = ToolAnnotations(readOnlyHint = false),
  ) { request ->
    ToolSupport.handleToolCall {
      val args = request.arguments!!
      val stubs = client.withBearerToken(getBearerToken())

      val rsBuilder = ReportingSet.newBuilder()
      ToolSupport.PROTO_JSON_PARSER.merge(
        ToolSupport.encodeJsonElement(args.getValue("reporting_set")),
        rsBuilder,
      )

      val grpcRequest = createReportingSetRequest {
        parent = ToolSupport.getString(args, "parent")
        reportingSetId = ToolSupport.getString(args, "reporting_set_id")
        reportingSet = rsBuilder.build()
      }

      ToolSupport.PROTO_JSON_PRINTER.print(stubs.reportingSets.createReportingSet(grpcRequest))
    }
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
    toolAnnotations = ToolAnnotations(readOnlyHint = true),
  ) { request ->
    ToolSupport.handleToolCall {
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = getReportingSetRequest {
        name = ToolSupport.getString(request.arguments!!, "name")
      }
      ToolSupport.PROTO_JSON_PRINTER.print(stubs.reportingSets.getReportingSet(grpcRequest))
    }
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
    toolAnnotations = ToolAnnotations(readOnlyHint = true),
  ) { request ->
    ToolSupport.handleToolCall {
      val args = request.arguments!!
      val stubs = client.withBearerToken(getBearerToken())
      val grpcRequest = listReportingSetsRequest {
        parent = ToolSupport.getString(args, "parent")
        ToolSupport.getIntOrNull(args, "page_size")?.let { pageSize = it }
        ToolSupport.getStringOrNull(args, "page_token")?.let { pageToken = it }
      }

      ToolSupport.PROTO_JSON_PRINTER.print(stubs.reportingSets.listReportingSets(grpcRequest))
    }
  }
}
