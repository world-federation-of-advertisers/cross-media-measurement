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

import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.server.ServerOptions
import io.modelcontextprotocol.kotlin.sdk.server.mcpStatelessStreamableHttp
import io.modelcontextprotocol.kotlin.sdk.types.Implementation
import io.modelcontextprotocol.kotlin.sdk.types.ServerCapabilities
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.mcp.prompts.registerReportingPrompts
import org.wfanet.measurement.reporting.mcp.tools.registerBasicReportTools
import org.wfanet.measurement.reporting.mcp.tools.registerEventGroupTools
import org.wfanet.measurement.reporting.mcp.tools.registerIqfTools
import org.wfanet.measurement.reporting.mcp.tools.registerReportingSetTools

private const val SERVER_NAME = "ReportingMcpServer"
private const val SERVER_VERSION = "0.1.0"
private const val BEARER_PREFIX = "Bearer "

/**
 * Configures this [Application] as the Reporting MCP server.
 *
 * Stateless Streamable HTTP: the SDK helper creates a fresh [Server] per POST (no session state),
 * so any replica can serve any request and pod restarts lose nothing. The helper installs
 * ContentNegotiation(McpJson) and SSE itself.
 *
 * DNS rebinding protection is enabled only when [allowedHosts] is non-empty (e.g. the in-cluster
 * service hostnames); otherwise the Host header is not checked.
 *
 * The bearer token is read per request and resolved lazily inside each tool call: a missing token
 * throws [IllegalArgumentException], which the tool error handler turns into a clean tool error
 * rather than a 500.
 */
fun Application.installReportingMcp(
  apiClient: ReportingPublicApiClient,
  allowedHosts: List<String> = emptyList(),
) {
  install(CORS) {
    anyHost()
    allowNonSimpleContentTypes = true
    allowMethod(HttpMethod.Options)
    allowMethod(HttpMethod.Post)
    allowHeader(HttpHeaders.ContentType)
    allowHeader(HttpHeaders.Authorization)
    allowHeader("Mcp-Protocol-Version")
    exposeHeader("Mcp-Protocol-Version")
  }

  mcpStatelessStreamableHttp(
    enableDnsRebindingProtection = allowedHosts.isNotEmpty(),
    allowedHosts = allowedHosts.ifEmpty { null },
  ) {
    val authorizationHeader = call.request.headers[HttpHeaders.Authorization]
    ReportingMcpServer.createServer(apiClient) { bearerToken(authorizationHeader) }
  }

  routing { get("/healthz") { call.respondText("OK", status = HttpStatusCode.OK) } }
}

/**
 * Extracts the bearer token from an `Authorization` header value, forwarded to the Reporting API.
 *
 * @throws IllegalArgumentException if the header is missing or has no non-blank bearer token
 */
private fun bearerToken(authorizationHeader: String?): String {
  val token =
    authorizationHeader
      ?.takeIf { it.startsWith(BEARER_PREFIX, ignoreCase = true) }
      ?.substring(BEARER_PREFIX.length)
      ?.trim()
  return requireNotNull(token?.takeUnless(String::isEmpty)) {
    "Missing bearer token in Authorization header"
  }
}

/**
 * Factory for the Reporting MCP [Server].
 *
 * Builds the server from already-constructed dependencies and has no knowledge of flags. The
 * flags-based entry point is [ReportingMcpServerDaemon].
 */
object ReportingMcpServer {
  /** Builds the MCP [Server] with all Reporting tools registered. */
  fun createServer(apiClient: ReportingPublicApiClient, getBearerToken: () -> String): Server {
    val server =
      Server(
        serverInfo = Implementation(name = SERVER_NAME, version = SERVER_VERSION),
        options =
          ServerOptions(
            capabilities =
              ServerCapabilities(
                // Stateless mode advertises only pull-based capabilities (tools, prompts); there
                // is no server-to-client push (logging/notifications).
                tools = ServerCapabilities.Tools(listChanged = false),
                prompts = ServerCapabilities.Prompts(listChanged = false),
              )
          ),
      )

    server.registerBasicReportTools(apiClient, getBearerToken)
    server.registerEventGroupTools(apiClient, getBearerToken)
    server.registerReportingSetTools(apiClient, getBearerToken)
    server.registerIqfTools(apiClient, getBearerToken)
    server.registerReportingPrompts()

    return server
  }
}
