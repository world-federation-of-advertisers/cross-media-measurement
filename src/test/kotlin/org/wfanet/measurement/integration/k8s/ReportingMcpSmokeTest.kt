/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Struct
import com.google.protobuf.Value
import com.google.protobuf.util.JsonFormat
import io.grpc.ManagedChannel
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Paths
import java.time.Duration
import java.util.logging.Logger
import org.junit.AfterClass
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.measurement.integration.k8s.testing.CorrectnessTestConfig
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse

/**
 * Test that a deployed Reporting MCP server serves MCP against the real Reporting API.
 *
 * Assumptions:
 * * The MCP server is deployed and reachable at [CorrectnessTestConfig.getMcpHost], which the
 *   deployment renders only when the environment also configures an OAuth issuer.
 * * The Reporting API trusts the OpenID provider in `open_id_providers_config.json`.
 * * The MeasurementConsumer has EventGroups.
 *
 * The whole test is skipped when the environment has no MCP host configured.
 */
@RunWith(JUnit4::class)
class ReportingMcpSmokeTest {
  @Test
  fun `healthz returns OK`() {
    val response = sendGet("/healthz")

    assertThat(response.statusCode()).isEqualTo(200)
    assertThat(response.body()).isEqualTo("OK")
  }

  @Test
  fun `server serves OAuth protected resource metadata`() {
    val response = sendGet(OAUTH_PROTECTED_RESOURCE_PATH)

    assertThat(response.statusCode()).isEqualTo(200)
    val metadata: Struct = parseJson(response.body())
    assertThat(metadata.getFieldsOrThrow("resource").stringValue).isNotEmpty()
    assertThat(metadata.getFieldsOrThrow("authorization_servers").listValue.valuesList).isNotEmpty()
  }

  @Test
  fun `mcp returns unauthorized when bearer token is missing`() {
    val response = postMcp(INITIALIZE_REQUEST, bearerToken = null)

    assertThat(response.statusCode()).isEqualTo(401)
    assertThat(response.headers().firstValue(WWW_AUTHENTICATE_HEADER).orElse(""))
      .contains("resource_metadata=")
  }

  @Test
  fun `initialize returns server info`() {
    val response = postMcp(INITIALIZE_REQUEST, accessToken)

    assertThat(response.statusCode()).isEqualTo(200)
    val serverInfo: Struct = jsonRpcResult(response).getFieldsOrThrow("serverInfo").structValue
    assertThat(serverInfo.getFieldsOrThrow("name").stringValue).isEqualTo(SERVER_NAME)
  }

  @Test
  fun `list_event_groups returns EventGroups as proto JSON`() {
    val response = postMcp(listEventGroupsRequest(TEST_CONFIG.measurementConsumer), accessToken)

    assertThat(response.statusCode()).isEqualTo(200)
    val result: Struct = jsonRpcResult(response)
    assertThat(result.getFieldsOrDefault("isError", Value.getDefaultInstance()).boolValue).isFalse()
    val listEventGroupsResponse =
      ListEventGroupsResponse.newBuilder()
        .also { JsonFormat.parser().merge(toolResultText(result), it) }
        .build()
    assertThat(listEventGroupsResponse.eventGroupsList).isNotEmpty()
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.enclosingClass.name)

    private const val MCP_PATH = "/mcp"
    private const val OAUTH_PROTECTED_RESOURCE_PATH = "/.well-known/oauth-protected-resource"
    private const val WWW_AUTHENTICATE_HEADER = "WWW-Authenticate"
    private const val PROTOCOL_VERSION = "2025-03-26"
    private const val SERVER_NAME = "ReportingMcpServer"
    private const val EVENT_GROUPS_LIST_PERMISSION = "reporting.eventGroups.list"
    private const val SSE_DATA_PREFIX = "data:"
    private val TIMEOUT = Duration.ofSeconds(60)

    private val INITIALIZE_REQUEST =
      """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{""" +
        """"protocolVersion":"$PROTOCOL_VERSION","capabilities":{},""" +
        """"clientInfo":{"name":"ReportingMcpSmokeTest","version":"1.0"}}}"""

    private val CONFIG_PATH =
      Paths.get("src", "test", "kotlin", "org", "wfanet", "measurement", "integration", "k8s")
    private const val TEST_CONFIG_NAME = "correctness_test_config.textproto"

    private val TEST_CONFIG: CorrectnessTestConfig by lazy {
      val configFile =
        AbstractCorrectnessTest.getRuntimePath(CONFIG_PATH.resolve(TEST_CONFIG_NAME)).toFile()
      parseTextProto(configFile, CorrectnessTestConfig.getDefaultInstance())
    }

    private val baseUrl: String by lazy { "https://${TEST_CONFIG.mcpHost}" }

    private val httpClient: HttpClient by lazy {
      HttpClient.newBuilder().connectTimeout(TIMEOUT).build()
    }

    private val channels = mutableListOf<ManagedChannel>()

    private val accessToken: String by lazy {
      val accessChannel =
        buildMutualTlsChannel(
            TEST_CONFIG.accessPublicApiTarget,
            AbstractCorrectnessTest.ACCESS_SIGNING_CERTS,
            TEST_CONFIG.accessPublicApiCertHost.ifEmpty { null },
          )
          .also { channels.add(it) }
      val getAccessToken =
        AbstractCorrectnessTest.reportingAccessTokenProvider(
          TEST_CONFIG.measurementConsumer,
          accessChannel,
          TEST_CONFIG.reportingTokenAudience,
          setOf(EVENT_GROUPS_LIST_PERMISSION),
        )
      getAccessToken()
    }

    @BeforeClass
    @JvmStatic
    fun assumeMcpHostConfigured() {
      if (TEST_CONFIG.mcpHost.isEmpty()) {
        logger.warning("No MCP host configured for this environment. Skipping.")
      }
      assumeTrue(TEST_CONFIG.mcpHost.isNotEmpty())
    }

    @AfterClass
    @JvmStatic
    fun shutDownChannels() {
      for (channel in channels) {
        channel.shutdown()
      }
    }

    private fun listEventGroupsRequest(measurementConsumer: String): String =
      """{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{""" +
        """"name":"list_event_groups","arguments":{"parent":"$measurementConsumer"}}}"""

    private fun sendGet(path: String): HttpResponse<String> =
      httpClient.send(
        HttpRequest.newBuilder(URI("$baseUrl$path")).timeout(TIMEOUT).GET().build(),
        HttpResponse.BodyHandlers.ofString(),
      )

    private fun postMcp(body: String, bearerToken: String?): HttpResponse<String> {
      val requestBuilder =
        HttpRequest.newBuilder(URI("$baseUrl$MCP_PATH"))
          .timeout(TIMEOUT)
          .header("Content-Type", "application/json")
          .header("Accept", "application/json, text/event-stream")
      if (bearerToken != null) {
        requestBuilder.header("Authorization", "Bearer $bearerToken")
      }
      return httpClient.send(
        requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body)).build(),
        HttpResponse.BodyHandlers.ofString(),
      )
    }

    /** Returns the `result` of a JSON-RPC response, which may be framed as a single SSE event. */
    private fun jsonRpcResult(response: HttpResponse<String>): Struct {
      val body: String = response.body()
      val json: String =
        if (body.lineSequence().any { it.startsWith(SSE_DATA_PREFIX) }) {
          body
            .lineSequence()
            .filter { it.startsWith(SSE_DATA_PREFIX) }
            .joinToString("") { it.substringAfter(SSE_DATA_PREFIX).trim() }
        } else {
          body
        }

      val envelope: Struct = parseJson(json)
      check(!envelope.containsFields("error")) {
        "JSON-RPC error: ${envelope.getFieldsOrThrow("error")}"
      }
      return envelope.getFieldsOrThrow("result").structValue
    }

    /** Returns the text content of a `tools/call` result. */
    private fun toolResultText(result: Struct): String =
      result
        .getFieldsOrThrow("content")
        .listValue
        .getValues(0)
        .structValue
        .getFieldsOrThrow("text")
        .stringValue

    private fun parseJson(json: String): Struct {
      val builder = Struct.newBuilder()
      JsonFormat.parser().merge(json, builder)
      return builder.build()
    }
  }
}
