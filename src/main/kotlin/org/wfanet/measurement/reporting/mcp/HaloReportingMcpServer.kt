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

package org.wfanet.measurement.reporting.mcp

import io.grpc.Channel
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.reporting.mcp.auth.BearerTokenExtractor
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.mcp.prompts.registerWorkflowPrompts
import org.wfanet.measurement.reporting.mcp.tools.registerBasicReportTools
import org.wfanet.measurement.reporting.mcp.tools.registerEventGroupTools
import org.wfanet.measurement.reporting.mcp.tools.registerIqfTools
import org.wfanet.measurement.reporting.mcp.tools.registerReportingSetTools
import picocli.CommandLine

private object HaloReportingMcpServer {
  @CommandLine.Command(
    name = "HaloReportingMcpServer",
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

    val apiClient = ReportingPublicApiClient(reportingChannel)

    embeddedServer(CIO, port = mcpServerFlags.port) {
        routing {
          post("/mcp") {
            val bearerToken = BearerTokenExtractor.extract(call.request)
            if (bearerToken == null) {
              call.respondText(
                McpServer.errorResponse(null, -32000, "Missing Authorization: Bearer header"),
                ContentType.Application.Json,
                HttpStatusCode.Unauthorized,
              )
              return@post
            }

            val mcpServer = createMcpServer(apiClient) { bearerToken }
            val requestBody = call.receiveText()
            val response = runBlocking { mcpServer.handleRequest(requestBody) }

            if (response.isNotEmpty()) {
              call.respondText(response, ContentType.Application.Json)
            } else {
              call.respondText("", status = HttpStatusCode.NoContent)
            }
          }

          get("/healthz") {
            call.respondText("OK", status = HttpStatusCode.OK)
          }
        }
      }
      .start(wait = true)
  }
}

internal fun createMcpServer(
  apiClient: ReportingPublicApiClient,
  getBearerToken: () -> String,
): McpServer {
  val server = McpServer()
  server.registerBasicReportTools(apiClient, getBearerToken)
  server.registerEventGroupTools(apiClient, getBearerToken)
  server.registerReportingSetTools(apiClient, getBearerToken)
  server.registerIqfTools(apiClient, getBearerToken)
  server.registerWorkflowPrompts()
  return server
}

class McpServerFlags {
  @CommandLine.Option(
    names = ["--port"],
    description = ["HTTP port for the MCP server."],
    defaultValue = "8443",
  )
  var port: Int = 8443
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
  var reportingServerApiCertHost: String = ""
    private set

  @CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables verbose downstream gRPC logging."],
    defaultValue = "false",
  )
  var debugVerboseGrpcClientLogging: Boolean = false
    private set
}

fun main(args: Array<String>) = commandLineMain(HaloReportingMcpServer::run, args)
