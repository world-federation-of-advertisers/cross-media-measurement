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

import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ClientInterceptors
import io.grpc.MethodDescriptor
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.server.ServerOptions
import io.modelcontextprotocol.kotlin.sdk.server.mcpStatelessStreamableHttp
import io.modelcontextprotocol.kotlin.sdk.types.Implementation
import io.modelcontextprotocol.kotlin.sdk.types.ServerCapabilities
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.mcp.tools.registerBasicReportTools
import org.wfanet.measurement.reporting.mcp.tools.registerEventGroupTools
import org.wfanet.measurement.reporting.mcp.tools.registerIqfTools
import org.wfanet.measurement.reporting.mcp.tools.registerReportingSetTools
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import picocli.CommandLine

private const val SERVER_NAME = "HaloReportingMcpServer"
private const val SERVER_VERSION = "0.1.0"
private const val BEARER_PREFIX = "Bearer "

object ReportingMcpServerFromFlags {
  @CommandLine.Command(
    name = SERVER_NAME,
    description = ["MCP server for the Halo Reporting v2alpha public API."],
    mixinStandardHelpOptions = true,
    showDefaultValues = true,
  )
  fun run(
    @CommandLine.Mixin tlsFlags: TlsFlags,
    @CommandLine.Mixin mcpServerFlags: McpServerFlags,
  ) {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )

    val reportingChannel: Channel =
      buildMutualTlsChannel(
          target = mcpServerFlags.reportingPublicApiTarget,
          clientCerts = clientCerts,
          hostName = mcpServerFlags.reportingPublicApiCertHost,
        )
        .withVerboseLogging(mcpServerFlags.debugVerboseGrpcClientLogging)

    val deadlineInterceptor =
      object : ClientInterceptor {
        override fun <ReqT, RespT> interceptCall(
          method: MethodDescriptor<ReqT, RespT>,
          callOptions: CallOptions,
          next: Channel,
        ): ClientCall<ReqT, RespT> =
          next.newCall(
            method,
            callOptions.withDeadlineAfter(
              mcpServerFlags.reportingPublicApiDeadline.toMillis(),
              TimeUnit.MILLISECONDS,
            ),
          )
      }
    val deadlineChannel = ClientInterceptors.intercept(reportingChannel, deadlineInterceptor)
    val apiClient =
      ReportingPublicApiClient(
        basicReports = BasicReportsCoroutineStub(deadlineChannel),
        eventGroups = EventGroupsCoroutineStub(deadlineChannel),
        reportingSets = ReportingSetsCoroutineStub(deadlineChannel),
        impressionQualificationFilters =
          ImpressionQualificationFiltersCoroutineStub(deadlineChannel),
      )

    embeddedServer(CIO, host = mcpServerFlags.host, port = mcpServerFlags.port) {
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

        // Stateless Streamable HTTP: the SDK helper creates a fresh Server per POST
        // (no session state), so any replica can serve any request and pod restarts
        // lose nothing. The helper installs ContentNegotiation(McpJson) and SSE itself.
        //
        // DNS rebinding protection is enabled only when --allowed-host is set (e.g. the
        // in-cluster service hostnames); otherwise the Host header is not checked.
        //
        // The bearer token is read per request and resolved lazily inside each tool call
        // (a missing token throws IllegalArgumentException, which the tool error handler
        // turns into a clean tool error rather than a 500).
        mcpStatelessStreamableHttp(
          enableDnsRebindingProtection = mcpServerFlags.allowedHosts.isNotEmpty(),
          allowedHosts = mcpServerFlags.allowedHosts.ifEmpty { null },
        ) {
          val authorizationHeader = call.request.headers[HttpHeaders.Authorization]
          createMcpServer(apiClient) { bearerToken(authorizationHeader) }
        }

        routing { get("/healthz") { call.respondText("OK", status = HttpStatusCode.OK) } }
      }
      .start(wait = true)
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

  fun createMcpServer(apiClient: ReportingPublicApiClient, getBearerToken: () -> String): Server {
    val server =
      Server(
        serverInfo = Implementation(name = SERVER_NAME, version = SERVER_VERSION),
        options =
          ServerOptions(
            capabilities =
              ServerCapabilities(
                // Stateless mode: no server-to-client push (logging/notifications), so
                // only the tools capability is advertised.
                tools = ServerCapabilities.Tools(listChanged = false)
                // TODO(#3834): Add prompts capability in follow-up PR.
              )
          ),
      )

    server.registerBasicReportTools(apiClient, getBearerToken)
    server.registerEventGroupTools(apiClient, getBearerToken)
    server.registerReportingSetTools(apiClient, getBearerToken)
    server.registerIqfTools(apiClient, getBearerToken)

    return server
  }
}

class McpServerFlags {
  @CommandLine.Option(
    names = ["--host"],
    description = ["Network interface to bind to. Use 0.0.0.0 for container deployments."],
    defaultValue = "127.0.0.1",
  )
  lateinit var host: String
    private set

  @set:CommandLine.Option(
    names = ["--port"],
    description =
      [
        "HTTP port for the MCP server. For external access, terminate TLS upstream " +
          "(a LoadBalancer or proxy)."
      ],
    defaultValue = "8080",
  )
  var port by Delegates.notNull<Int>()

  @CommandLine.Option(
    names = ["--reporting-public-api-deadline"],
    description = ["Deadline for downstream Reporting API gRPC calls, e.g. 30s or 1m."],
    defaultValue = "30s",
  )
  lateinit var reportingPublicApiDeadline: Duration
    private set

  @CommandLine.Option(
    names = ["--reporting-public-api-target"],
    description = ["gRPC target of the existing Reporting v2alpha public API server."],
    required = true,
  )
  lateinit var reportingPublicApiTarget: String
    private set

  @CommandLine.Option(
    names = ["--reporting-public-api-cert-host"],
    description = ["TLS DNS-ID override for the Reporting API certificate."],
  )
  var reportingPublicApiCertHost: String? = null
    private set

  @CommandLine.Option(
    names = ["--allowed-host"],
    description =
      [
        "Host permitted by DNS rebinding protection (may be repeated). When unset, " +
          "the protection is disabled and the Host header is not checked."
      ],
  )
  var allowedHosts: List<String> = emptyList()
    private set

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables verbose downstream gRPC logging."],
    defaultValue = "false",
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
}

fun main(args: Array<String>) = commandLineMain(ReportingMcpServerFromFlags::run, args)
