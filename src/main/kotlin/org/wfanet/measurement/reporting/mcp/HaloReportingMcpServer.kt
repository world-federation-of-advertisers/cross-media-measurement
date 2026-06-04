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
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.install
import io.ktor.server.auth.Authentication
import io.ktor.server.auth.UserIdPrincipal
import io.ktor.server.auth.authenticate
import io.ktor.server.auth.bearer
import io.ktor.server.auth.principal
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.request.header
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.sse.SSE
import io.ktor.server.sse.sse
import io.ktor.util.collections.ConcurrentMap
import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.server.ServerOptions
import io.modelcontextprotocol.kotlin.sdk.server.StreamableHttpServerTransport
import io.modelcontextprotocol.kotlin.sdk.types.Implementation
import io.modelcontextprotocol.kotlin.sdk.types.McpJson
import io.modelcontextprotocol.kotlin.sdk.types.ServerCapabilities
import java.util.concurrent.TimeUnit
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
private const val MCP_SESSION_ID_HEADER = "Mcp-Session-Id"

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
              mcpServerFlags.reportingServerApiDeadlineSeconds,
              TimeUnit.SECONDS,
            ),
          )
      }
    val deadlineChannel =
      ClientInterceptors.intercept(reportingChannel, deadlineInterceptor)
    val apiClient =
      ReportingPublicApiClient(
        basicReports = BasicReportsCoroutineStub(deadlineChannel),
        eventGroups = EventGroupsCoroutineStub(deadlineChannel),
        reportingSets = ReportingSetsCoroutineStub(deadlineChannel),
        impressionQualificationFilters =
          ImpressionQualificationFiltersCoroutineStub(deadlineChannel),
      )

    val transports = ConcurrentMap<String, StreamableHttpServerTransport>()

    embeddedServer(CIO, host = mcpServerFlags.host, port = mcpServerFlags.port) {
        install(CORS) {
          anyHost()
          allowNonSimpleContentTypes = true
          allowMethod(HttpMethod.Options)
          allowMethod(HttpMethod.Post)
          allowMethod(HttpMethod.Delete)
          allowHeader(HttpHeaders.ContentType)
          allowHeader(HttpHeaders.Authorization)
          allowHeader(MCP_SESSION_ID_HEADER)
          allowHeader("Mcp-Protocol-Version")
          exposeHeader(MCP_SESSION_ID_HEADER)
          exposeHeader("Mcp-Protocol-Version")
        }

        install(ContentNegotiation) { json(McpJson) }
        install(SSE)

        // Bearer auth extracts the token for passthrough to the Reporting API.
        // The downstream API validates the token — we accept all non-empty tokens here.
        install(Authentication) {
          bearer("mcp-bearer") {
            authenticate { credential ->
              UserIdPrincipal(credential.token)
            }
          }
        }

        routing {
          authenticate("mcp-bearer") {
            route("/mcp") {
              sse {
                val transport = findTransport(call, transports) ?: return@sse
                transport.handleRequest(this, call)
              }

              post {
                val transport =
                  getOrCreateTransport(call, transports, apiClient) ?: return@post
                transport.handleRequest(null, call)
              }

              delete {
                val transport = findTransport(call, transports) ?: return@delete
                transport.handleRequest(null, call)
              }
            }
          }

          get("/healthz") { call.respondText("OK", status = HttpStatusCode.OK) }
        }
      }
      .start(wait = true)
  }

  private suspend fun findTransport(
    call: ApplicationCall,
    transports: ConcurrentMap<String, StreamableHttpServerTransport>,
  ): StreamableHttpServerTransport? {
    val sessionId = call.request.header(MCP_SESSION_ID_HEADER)
    if (sessionId == null) {
      call.respond(HttpStatusCode.BadRequest, "Missing $MCP_SESSION_ID_HEADER header")
      return null
    }
    val transport = transports[sessionId]
    if (transport == null) {
      call.respond(HttpStatusCode.NotFound, "Session not found")
    }
    return transport
  }

  private suspend fun getOrCreateTransport(
    call: ApplicationCall,
    transports: ConcurrentMap<String, StreamableHttpServerTransport>,
    apiClient: ReportingPublicApiClient,
  ): StreamableHttpServerTransport? {
    val sessionId = call.request.header(MCP_SESSION_ID_HEADER)
    if (sessionId != null) {
      val transport = transports[sessionId]
      if (transport == null) {
        call.respond(HttpStatusCode.NotFound, "Session not found")
      }
      return transport
    }

    val bearerToken = call.principal<UserIdPrincipal>()?.name ?: return null
    val configuration =
      StreamableHttpServerTransport.Configuration(enableJsonResponse = true)
    val transport = StreamableHttpServerTransport(configuration)

    transport.setOnSessionInitialized { initializedSessionId ->
      transports[initializedSessionId] = transport
    }
    transport.setOnSessionClosed { closedSessionId ->
      transports.remove(closedSessionId)
    }

    val server = createMcpServer(apiClient) { bearerToken }
    // Intentionally redundant with setOnSessionClosed — belt-and-suspenders cleanup
    // per SDK sample. ConcurrentMap.remove is idempotent.
    server.onClose { transport.sessionId?.let { transports.remove(it) } }
    server.createSession(transport)

    return transport
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
              logging = ServerCapabilities.Logging,
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
  var port: Int = 8080
    private set

  @CommandLine.Option(
    names = ["--reporting-server-api-deadline"],
    description = ["Deadline in seconds for downstream Reporting API gRPC calls."],
    defaultValue = "30",
  )
  var reportingServerApiDeadlineSeconds: Long = 30
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
