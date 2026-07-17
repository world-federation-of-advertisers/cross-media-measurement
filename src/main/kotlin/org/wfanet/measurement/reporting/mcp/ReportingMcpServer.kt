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

import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.server.ServerOptions
import io.modelcontextprotocol.kotlin.sdk.server.mcpStatelessStreamableHttp
import io.modelcontextprotocol.kotlin.sdk.types.Implementation
import io.modelcontextprotocol.kotlin.sdk.types.ServerCapabilities
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.mcp.prompts.registerReportingPrompts
import org.wfanet.measurement.reporting.mcp.tools.registerBasicReportTools
import org.wfanet.measurement.reporting.mcp.tools.registerEventGroupTools
import org.wfanet.measurement.reporting.mcp.tools.registerIqfTools
import org.wfanet.measurement.reporting.mcp.tools.registerReportingSetTools

private const val SERVER_NAME = "ReportingMcpServer"
private const val SERVER_VERSION = "0.1.0"
private const val BEARER_PREFIX = "Bearer "
private const val MCP_PATH = "/mcp"
private const val OAUTH_PROTECTED_RESOURCE_PATH = "/.well-known/oauth-protected-resource"

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
 * When [oauthProtectedResource] and [oauthAuthorizationServers] are set, the server also serves
 * OAuth 2.0 Protected Resource Metadata (RFC 9728) at `/.well-known/oauth-protected-resource` so
 * MCP clients can discover the authorization server; otherwise that endpoint is absent and behavior
 * is unchanged. [oauthScopesSupported] and [oauthResourceDocumentation], when non-empty, are
 * advertised in that metadata (RFC 9728 `scopes_supported` and `resource_documentation`).
 *
 * When OAuth is configured, an MCP request without a bearer token is answered with `401` and a
 * `WWW-Authenticate` header pointing at the metadata endpoint, which is what tells an MCP client to
 * start the OAuth login flow (RFC 9728, MCP authorization spec). Health and discovery endpoints
 * stay open. Validating the token at this server (beyond presence) is a follow-up.
 *
 * The bearer token is read per request and resolved lazily inside each tool call: a missing token
 * throws [IllegalArgumentException], which the tool error handler turns into a clean tool error
 * rather than a 500.
 */
fun Application.installReportingMcp(
  apiClient: ReportingPublicApiClient,
  allowedHosts: List<String> = emptyList(),
  oauthProtectedResource: String? = null,
  oauthAuthorizationServers: List<String> = emptyList(),
  oauthScopesSupported: List<String> = emptyList(),
  oauthResourceDocumentation: String? = null,
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

  // MCP authorization (RFC 9728 + MCP authorization spec): when OAuth is configured, an MCP request
  // without a bearer token is answered with 401 and a WWW-Authenticate header pointing at the
  // Protected Resource Metadata, which is what tells the client to start the OAuth login flow.
  // Health and discovery endpoints stay open, and without OAuth configured behavior is unchanged (a
  // present token is still validated downstream). Validating the token here is a follow-up.
  if (oauthProtectedResource != null && oauthAuthorizationServers.isNotEmpty()) {
    val resourceMetadataUrl = oauthProtectedResource.trimEnd('/') + OAUTH_PROTECTED_RESOURCE_PATH
    intercept(ApplicationCallPipeline.Plugins) {
      if (
        call.request.httpMethod == HttpMethod.Post &&
          call.request.path() == MCP_PATH &&
          bearerTokenOrNull(call.request.headers[HttpHeaders.Authorization]) == null
      ) {
        call.response.headers.append(
          HttpHeaders.WWWAuthenticate,
          "Bearer resource_metadata=\"$resourceMetadataUrl\"",
        )
        call.respondText("Unauthorized", status = HttpStatusCode.Unauthorized)
        finish()
      }
    }
  }

  mcpStatelessStreamableHttp(
    enableDnsRebindingProtection = allowedHosts.isNotEmpty(),
    allowedHosts = allowedHosts.ifEmpty { null },
  ) {
    val authorizationHeader = call.request.headers[HttpHeaders.Authorization]
    ReportingMcpServer.createServer(apiClient) { bearerToken(authorizationHeader) }
  }

  routing {
    get("/healthz") { call.respondText("OK", status = HttpStatusCode.OK) }

    // When configured, advertise this server as an OAuth 2.0 protected resource (RFC 9728) so MCP
    // clients can discover the authorization server and obtain a token. Gated on configuration:
    // with no authorization server set the endpoint is absent and behavior is unchanged. The
    // interactive login flow itself is tracked in #4042.
    if (oauthProtectedResource != null && oauthAuthorizationServers.isNotEmpty()) {
      get(OAUTH_PROTECTED_RESOURCE_PATH) {
        call.respondText(
          oauthProtectedResourceMetadata(
            oauthProtectedResource,
            oauthAuthorizationServers,
            oauthScopesSupported,
            oauthResourceDocumentation,
          ),
          ContentType.Application.Json,
        )
      }
    }
  }
}

/**
 * Builds the OAuth 2.0 Protected Resource Metadata document (RFC 9728), advertising the
 * authorization server(s) a client should use to obtain a token for this resource.
 */
private fun oauthProtectedResourceMetadata(
  resource: String,
  authorizationServers: List<String>,
  scopesSupported: List<String>,
  resourceDocumentation: String?,
): String =
  buildJsonObject {
      put("resource", resource)
      putJsonArray("authorization_servers") { authorizationServers.forEach { add(it) } }
      putJsonArray("bearer_methods_supported") { add("header") }
      if (scopesSupported.isNotEmpty()) {
        putJsonArray("scopes_supported") { scopesSupported.forEach { add(it) } }
      }
      if (resourceDocumentation != null) {
        put("resource_documentation", resourceDocumentation)
      }
    }
    .toString()

/**
 * Extracts a non-blank bearer token from an `Authorization` header value, or null if the header is
 * absent or carries no non-blank bearer token.
 */
private fun bearerTokenOrNull(authorizationHeader: String?): String? =
  authorizationHeader
    ?.takeIf { it.startsWith(BEARER_PREFIX, ignoreCase = true) }
    ?.substring(BEARER_PREFIX.length)
    ?.trim()
    ?.takeUnless(String::isEmpty)

/**
 * Extracts the bearer token from an `Authorization` header value, forwarded to the Reporting API.
 *
 * @throws IllegalArgumentException if the header is missing or has no non-blank bearer token
 */
private fun bearerToken(authorizationHeader: String?): String =
  requireNotNull(bearerTokenOrNull(authorizationHeader)) {
    "Missing bearer token in Authorization header"
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
