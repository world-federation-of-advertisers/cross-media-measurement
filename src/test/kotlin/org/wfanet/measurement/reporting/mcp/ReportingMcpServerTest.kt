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
import io.grpc.testing.GrpcCleanupRule
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.modelcontextprotocol.kotlin.sdk.client.Client
import io.modelcontextprotocol.kotlin.sdk.testing.ChannelTransport
import io.modelcontextprotocol.kotlin.sdk.types.Implementation
import io.modelcontextprotocol.kotlin.sdk.types.TextContent
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
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
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.eventGroup
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingSet

@RunWith(JUnit4::class)
class ReportingMcpServerTest {
  @get:Rule val grpcCleanup = GrpcCleanupRule()

  @Test
  fun registersAllExpectedTools() {
    val server = ReportingMcpServer.createServer(createFakeApiClientWithServices()) { "test-token" }
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
    val server = ReportingMcpServer.createServer(createFakeApiClientWithServices()) { "test-token" }
    for ((name, tool) in server.tools) {
      assertWithMessage("tool '$name' should have a description")
        .that(tool.tool.description)
        .isNotEmpty()
    }
  }

  @Test
  fun callToolByNameReturnsProtoJsonResponse() = runBlocking {
    val apiClient = createFakeApiClientWithServices()
    val server = ReportingMcpServer.createServer(apiClient) { "test-token" }

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
    val server = ReportingMcpServer.createServer(apiClient) { "test-token" }

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
  fun callToolWithMissingBearerTokenReturnsError() = runBlocking {
    val apiClient = createFakeApiClientWithServices()
    // A missing bearer token surfaces as an IllegalArgumentException from getBearerToken, which the
    // tool error handler turns into a clean tool error rather than crashing the request.
    val server =
      ReportingMcpServer.createServer(apiClient) {
        throw IllegalArgumentException("Missing bearer token in Authorization header")
      }

    val (clientTransport, serverTransport) = ChannelTransport.createLinkedPair()
    val client = Client(clientInfo = Implementation(name = "test-client", version = "0.1"))

    server.createSession(serverTransport)
    client.connect(clientTransport)

    val result =
      client.callTool(
        "get_basic_report",
        buildJsonObject { put("name", "measurementConsumers/mc1/basicReports/br1") },
      )

    assertThat(result.isError).isTrue()
    val text = (result.content[0] as TextContent).text
    assertThat(text).contains("Missing bearer token")

    client.close()
  }

  @Test
  fun listEventGroupsWithStructuredFilterViaMcpClient() = runBlocking {
    val apiClient = createFakeApiClientWithServices()
    val mcpServer = ReportingMcpServer.createServer(apiClient) { "test-token" }

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

  /**
   * Exercises the real HTTP surface (CIO engine, CORS, bearer extraction, `/healthz`, and the
   * stateless Streamable HTTP route) that the in-process [ChannelTransport] tests bypass.
   */
  @Test
  fun servesHealthzAndInitializeOverHttp() = runBlocking {
    val server =
      embeddedServer(CIO, host = "127.0.0.1", port = 0) {
        installReportingMcp(createFakeApiClientWithServices())
      }
    server.start(wait = false)
    try {
      val port = server.engine.resolvedConnectors().first().port
      val httpClient = HttpClient.newHttpClient()

      val healthResponse =
        httpClient.send(
          HttpRequest.newBuilder(URI("http://127.0.0.1:$port/healthz")).GET().build(),
          HttpResponse.BodyHandlers.ofString(),
        )
      assertThat(healthResponse.statusCode()).isEqualTo(200)
      assertThat(healthResponse.body()).isEqualTo("OK")

      val initializeResponse =
        httpClient.send(
          HttpRequest.newBuilder(URI("http://127.0.0.1:$port/mcp"))
            .header("Authorization", "Bearer test-token")
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .POST(
              HttpRequest.BodyPublishers.ofString(
                """{"jsonrpc":"2.0","method":"initialize","id":1,"params":{""" +
                  """"protocolVersion":"2025-03-26","capabilities":{},""" +
                  """"clientInfo":{"name":"test","version":"1.0"}}}"""
              )
            )
            .build(),
          HttpResponse.BodyHandlers.ofString(),
        )
      assertThat(initializeResponse.statusCode()).isEqualTo(200)
      assertThat(initializeResponse.body()).contains("ReportingMcpServer")
    } finally {
      server.stop()
    }
  }

  /**
   * Exercises the real `bearerToken()` extraction over HTTP: a POST with no `Authorization` header
   * surfaces as a clean tool error (not a 500).
   */
  @Test
  fun rejectsToolCallWithoutBearerTokenOverHttp() = runBlocking {
    val server =
      embeddedServer(CIO, host = "127.0.0.1", port = 0) {
        installReportingMcp(createFakeApiClientWithServices())
      }
    server.start(wait = false)
    try {
      val port = server.engine.resolvedConnectors().first().port
      val response =
        HttpClient.newHttpClient()
          .send(
            HttpRequest.newBuilder(URI("http://127.0.0.1:$port/mcp"))
              .header("Content-Type", "application/json")
              .header("Accept", "application/json, text/event-stream")
              .POST(
                HttpRequest.BodyPublishers.ofString(
                  """{"jsonrpc":"2.0","method":"tools/call","id":1,"params":{""" +
                    """"name":"get_basic_report",""" +
                    """"arguments":{"name":"measurementConsumers/mc1/basicReports/br1"}}}"""
                )
              )
              .build(),
            HttpResponse.BodyHandlers.ofString(),
          )
      assertThat(response.statusCode()).isEqualTo(200)
      assertThat(response.body()).contains("Missing bearer token")
    } finally {
      server.stop()
    }
  }

  /**
   * When an authorization server is configured, the server publishes OAuth 2.0 Protected Resource
   * Metadata (RFC 9728) so MCP clients can discover where to obtain a token.
   */
  @Test
  fun servesOAuthProtectedResourceMetadataWhenConfigured() = runBlocking {
    val server =
      embeddedServer(CIO, host = "127.0.0.1", port = 0) {
        installReportingMcp(
          createFakeApiClientWithServices(),
          oauthProtectedResource = "https://mcp.example.test/",
          oauthAuthorizationServers = listOf("https://auth.example.test/"),
        )
      }
    server.start(wait = false)
    try {
      val port = server.engine.resolvedConnectors().first().port
      val response =
        HttpClient.newHttpClient()
          .send(
            HttpRequest.newBuilder(
                URI("http://127.0.0.1:$port/.well-known/oauth-protected-resource")
              )
              .GET()
              .build(),
            HttpResponse.BodyHandlers.ofString(),
          )
      assertThat(response.statusCode()).isEqualTo(200)
      assertThat(response.body()).contains("https://mcp.example.test/")
      assertThat(response.body()).contains("https://auth.example.test/")
      assertThat(response.body()).contains("authorization_servers")
    } finally {
      server.stop()
    }
  }

  /** Without OAuth configuration, the metadata endpoint is absent and behavior is unchanged. */
  @Test
  fun omitsOAuthProtectedResourceMetadataByDefault() = runBlocking {
    val server =
      embeddedServer(CIO, host = "127.0.0.1", port = 0) {
        installReportingMcp(createFakeApiClientWithServices())
      }
    server.start(wait = false)
    try {
      val port = server.engine.resolvedConnectors().first().port
      val response =
        HttpClient.newHttpClient()
          .send(
            HttpRequest.newBuilder(
                URI("http://127.0.0.1:$port/.well-known/oauth-protected-resource")
              )
              .GET()
              .build(),
            HttpResponse.BodyHandlers.ofString(),
          )
      assertThat(response.statusCode()).isEqualTo(404)
    } finally {
      server.stop()
    }
  }

  private fun createFakeApiClientWithServices(): ReportingPublicApiClient {
    val serverName = InProcessServerBuilder.generateName()
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(
          object : BasicReportsGrpcKt.BasicReportsCoroutineImplBase() {
            override suspend fun createBasicReport(request: CreateBasicReportRequest): BasicReport =
              basicReport {
                name = "${request.parent}/basicReports/${request.basicReportId}"
              }

            override suspend fun getBasicReport(request: GetBasicReportRequest): BasicReport {
              if (request.name.contains("nonexistent")) {
                throw StatusException(Status.NOT_FOUND.withDescription("Not found"))
              }
              return basicReport { name = request.name }
            }

            override suspend fun listBasicReports(
              request: ListBasicReportsRequest
            ): ListBasicReportsResponse = ListBasicReportsResponse.getDefaultInstance()
          }
        )
        .addService(
          object : EventGroupsGrpcKt.EventGroupsCoroutineImplBase() {
            override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup =
              eventGroup {
                name = request.name
              }

            override suspend fun listEventGroups(
              request: ListEventGroupsRequest
            ): ListEventGroupsResponse = ListEventGroupsResponse.getDefaultInstance()
          }
        )
        .addService(
          object : ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase() {
            override suspend fun getReportingSet(request: GetReportingSetRequest): ReportingSet =
              reportingSet {
                name = request.name
              }

            override suspend fun listReportingSets(
              request: ListReportingSetsRequest
            ): ListReportingSetsResponse = ListReportingSetsResponse.getDefaultInstance()
          }
        )
        .addService(
          object :
            ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineImplBase() {
            override suspend fun getImpressionQualificationFilter(
              request: GetImpressionQualificationFilterRequest
            ): ImpressionQualificationFilter = impressionQualificationFilter { name = request.name }

            override suspend fun listImpressionQualificationFilters(
              request: ListImpressionQualificationFiltersRequest
            ): ListImpressionQualificationFiltersResponse =
              ListImpressionQualificationFiltersResponse.getDefaultInstance()
          }
        )
        .build()
        .start()
    )

    val channel =
      grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build())
    return ReportingPublicApiClient(
      basicReports = BasicReportsGrpcKt.BasicReportsCoroutineStub(channel),
      eventGroups = EventGroupsGrpcKt.EventGroupsCoroutineStub(channel),
      reportingSets = ReportingSetsGrpcKt.ReportingSetsCoroutineStub(channel),
      impressionQualificationFilters =
        ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub(channel),
    )
  }
}
