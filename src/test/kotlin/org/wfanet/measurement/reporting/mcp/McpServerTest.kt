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

package org.wfanet.measurement.reporting.mcp

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.modelcontextprotocol.kotlin.sdk.client.Client
import io.modelcontextprotocol.kotlin.sdk.testing.ChannelTransport
import io.modelcontextprotocol.kotlin.sdk.types.Implementation
import io.modelcontextprotocol.kotlin.sdk.types.TextContent
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.mcp.testing.FakeReportingPublicApiClient
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.CreateBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.GetBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.reporting.v2alpha.GetImpressionQualificationFilterRequest
import org.wfanet.measurement.reporting.v2alpha.GetReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequest
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsResponse
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.ListImpressionQualificationFiltersRequest
import org.wfanet.measurement.reporting.v2alpha.ListImpressionQualificationFiltersResponse
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsResponse
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt

@RunWith(JUnit4::class)
class McpServerTest {

  @Test
  fun registersAllExpectedTools() {
    val server = createMcpServer(FakeReportingPublicApiClient.create()) { "test-token" }
    assertThat(server.tools.keys)
      .containsExactly(
        "get_event_group",
        "list_event_groups",
        "create_basic_report",
        "get_basic_report",
        "list_basic_reports",
        "create_reporting_set",
        "get_reporting_set",
        "list_reporting_sets",
        "get_impression_qualification_filter",
        "list_impression_qualification_filters",
      )
  }

  @Test
  fun allToolsHaveDescriptions() {
    val server = createMcpServer(FakeReportingPublicApiClient.create()) { "test-token" }
    for ((name, tool) in server.tools) {
      assertWithMessage("tool '$name' should have a description")
        .that(tool.tool.description)
        .isNotEmpty()
    }
  }

  @Test
  fun callToolByNameReturnsProtoJsonResponse() = runBlocking {
    val apiClient = createFakeApiClientWithServices()
    val server = createMcpServer(apiClient) { "test-token" }

    val (clientTransport, serverTransport) = ChannelTransport.createLinkedPair()
    val client = Client(clientInfo = Implementation(name = "test-client", version = "0.1"))

    server.createSession(serverTransport)
    client.connect(clientTransport)

    val result =
      client.callTool(
        "get_basic_report",
        buildJsonObject { put("name", "measurementConsumers/mc1/basicReports/br1") },
      )

    assertThat(result.isError).isNotEqualTo(true)
    val text = (result.content[0] as TextContent).text
    assertThat(text).contains("basicReports/br1")

    client.close()
  }

  @Test
  fun callToolWithNotFoundReturnsError() = runBlocking {
    val apiClient = createFakeApiClientWithServices()
    val server = createMcpServer(apiClient) { "test-token" }

    val (clientTransport, serverTransport) = ChannelTransport.createLinkedPair()
    val client = Client(clientInfo = Implementation(name = "test-client", version = "0.1"))

    server.createSession(serverTransport)
    client.connect(clientTransport)

    val result =
      client.callTool(
        "get_basic_report",
        buildJsonObject { put("name", "measurementConsumers/mc1/basicReports/nonexistent") },
      )

    assertThat(result.isError).isTrue()
    val text = (result.content[0] as TextContent).text
    assertThat(text).contains("NOT_FOUND")

    client.close()
  }

  @Test
  fun listEventGroupsWithStructuredFilterViaMcpClient() = runBlocking {
    val apiClient = createFakeApiClientWithServices()
    val mcpServer = createMcpServer(apiClient) { "test-token" }

    val (clientTransport, serverTransport) = ChannelTransport.createLinkedPair()
    val mcpClient = Client(clientInfo = Implementation(name = "test-client", version = "1.0.0"))

    mcpServer.createSession(serverTransport)
    mcpClient.connect(clientTransport)

    // Exercises the full path: MCP args → args["structured_filter"] →
    // ToolSupport.encodeJsonElement → PROTO_JSON_PARSER.merge → gRPC call
    val result =
      mcpClient.callTool(
        name = "list_event_groups",
        arguments =
          buildJsonObject {
            put("parent", "measurementConsumers/mc1")
            putJsonObject("structured_filter") {
              putJsonArray("cmms_data_provider_in") { add("dataProviders/dp1") }
            }
          },
      )

    assertThat(result.isError).isNotEqualTo(true)

    mcpClient.close()
  }

  private fun createFakeApiClientWithServices(): ReportingPublicApiClient {
    val serverName = InProcessServerBuilder.generateName()
    InProcessServerBuilder.forName(serverName)
      .directExecutor()
      .addService(
        object : BasicReportsGrpcKt.BasicReportsCoroutineImplBase() {
          override suspend fun createBasicReport(
            request: CreateBasicReportRequest,
          ): BasicReport =
            BasicReport.newBuilder()
              .setName("${request.parent}/basicReports/${request.basicReportId}")
              .build()

          override suspend fun getBasicReport(request: GetBasicReportRequest): BasicReport {
            if (request.name.contains("nonexistent")) {
              throw StatusException(Status.NOT_FOUND.withDescription("Not found"))
            }
            return BasicReport.newBuilder().setName(request.name).build()
          }

          override suspend fun listBasicReports(
            request: ListBasicReportsRequest,
          ): ListBasicReportsResponse = ListBasicReportsResponse.getDefaultInstance()
        }
      )
      .addService(
        object : EventGroupsGrpcKt.EventGroupsCoroutineImplBase() {
          override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup =
            EventGroup.newBuilder().setName(request.name).build()

          override suspend fun listEventGroups(
            request: ListEventGroupsRequest,
          ): ListEventGroupsResponse = ListEventGroupsResponse.getDefaultInstance()
        }
      )
      .addService(
        object : ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase() {
          override suspend fun getReportingSet(request: GetReportingSetRequest): ReportingSet =
            ReportingSet.newBuilder().setName(request.name).build()

          override suspend fun listReportingSets(
            request: ListReportingSetsRequest,
          ): ListReportingSetsResponse = ListReportingSetsResponse.getDefaultInstance()
        }
      )
      .addService(
        object :
          ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineImplBase() {
          override suspend fun getImpressionQualificationFilter(
            request: GetImpressionQualificationFilterRequest,
          ): ImpressionQualificationFilter =
            ImpressionQualificationFilter.newBuilder().setName(request.name).build()

          override suspend fun listImpressionQualificationFilters(
            request: ListImpressionQualificationFiltersRequest,
          ): ListImpressionQualificationFiltersResponse =
            ListImpressionQualificationFiltersResponse.getDefaultInstance()
        }
      )
      .build()
      .start()

    val channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
    return ReportingPublicApiClient(
      basicReports = BasicReportsGrpcKt.BasicReportsCoroutineStub(channel),
      eventGroups = EventGroupsGrpcKt.EventGroupsCoroutineStub(channel),
      reportingSets = ReportingSetsGrpcKt.ReportingSetsCoroutineStub(channel),
      impressionQualificationFilters =
        ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub(channel),
    )
  }
}
