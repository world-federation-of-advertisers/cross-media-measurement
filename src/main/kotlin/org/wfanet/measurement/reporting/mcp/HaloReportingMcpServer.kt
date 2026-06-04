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

import io.grpc.Channel
import io.ktor.http.HttpStatusCode
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.server.ServerOptions
import io.modelcontextprotocol.kotlin.sdk.server.mcpStreamableHttp
import io.modelcontextprotocol.kotlin.sdk.types.Implementation
import io.modelcontextprotocol.kotlin.sdk.types.ServerCapabilities
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.reporting.mcp.auth.BearerTokenExtractor
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
          target = mcpServerFlags.reportingServerApiTarget,
          clientCerts = clientCerts,
          hostName = mcpServerFlags.reportingServerApiCertHost,
        )
        .withVerboseLogging(mcpServerFlags.debugVerboseGrpcClientLogging)

    val apiClient =
      ReportingPublicApiClient(
        basicReports = BasicReportsCoroutineStub(reportingChannel),
        eventGroups = EventGroupsCoroutineStub(reportingChannel),
        reportingSets = ReportingSetsCoroutineStub(reportingChannel),
        impressionQualificationFilters =
          ImpressionQualificationFiltersCoroutineStub(reportingChannel),
      )

    embeddedServer(CIO, host = mcpServerFlags.host, port = mcpServerFlags.port) {
        // TODO(#3834): Add CORS configuration for browser-based MCP clients.

        // DNS rebinding protection is disabled because this server is deployed behind a
        // TLS-terminating proxy (e.g. Envoy, K8s Ingress) that handles host validation.
        mcpStreamableHttp(enableDnsRebindingProtection = false) {
          val bearerToken =
            BearerTokenExtractor.extract(call.request)
              ?: error("Missing or invalid Authorization: Bearer header")

          // Server is created per MCP session. The bearer token from the session's
          // initial request is captured in the closure and used for all subsequent
          // tool calls within that session.
          createMcpServer(apiClient) { bearerToken }
        }

        routing {
          get("/healthz") { call.respondText("OK", status = HttpStatusCode.OK) }
        }
      }
      .start(wait = true)
  }
}

fun createMcpServer(
  apiClient: ReportingPublicApiClient,
  getBearerToken: () -> String,
): Server {
  val server =
    Server(
      serverInfo = Implementation(name = SERVER_NAME, version = SERVER_VERSION),
      options =
        ServerOptions(
          capabilities =
            ServerCapabilities(
              tools = ServerCapabilities.Tools(listChanged = false),
              // TODO(#3834): Add logging capability.
              // TODO(#3834): Add prompts capability in follow-up PR.
            ),
        ),
    )

  server.registerBasicReportTools(apiClient, getBearerToken)
  server.registerEventGroupTools(apiClient, getBearerToken)
  server.registerReportingSetTools(apiClient, getBearerToken)
  server.registerIqfTools(apiClient, getBearerToken)

  return server
}

class McpServerFlags {
  @CommandLine.Option(
    names = ["--host"],
    description = ["Network interface to bind to. Use 0.0.0.0 for container deployments."],
    defaultValue = "127.0.0.1",
  )
  lateinit var host: String
    private set

  @CommandLine.Option(
    names = ["--port"],
    description =
      [
        "HTTP port for the MCP server. Deploy behind a TLS-terminating proxy " +
          "(e.g. Envoy, K8s Ingress) for production use."
      ],
    defaultValue = "8080",
  )
  var port: Int = 0
    private set

  @CommandLine.Option(
    names = ["--reporting-server-api-target"],
    description = ["gRPC target of the existing Reporting v2alpha public API server."],
    required = true,
  )
  lateinit var reportingServerApiTarget: String
    private set

  @CommandLine.Option(
    names = ["--reporting-server-api-cert-host"],
    description = ["TLS DNS-ID override for the Reporting API certificate."],
  )
  var reportingServerApiCertHost: String? = null
    private set

  @CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables verbose downstream gRPC logging."],
    defaultValue = "false",
  )
  var debugVerboseGrpcClientLogging: Boolean = false
    private set
}

fun main(args: Array<String>) = commandLineMain(ReportingMcpServerFromFlags::run, args)
