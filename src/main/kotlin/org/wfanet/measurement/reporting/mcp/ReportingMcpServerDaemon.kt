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
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import picocli.CommandLine

/** Builds the runtime dependencies from command-line flags and starts the MCP server. */
@CommandLine.Command(
  name = "ReportingMcpServerDaemon",
  description = ["MCP server for the Reporting v2alpha public API."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class ReportingMcpServerDaemon : Runnable {
  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags
  @CommandLine.Mixin private lateinit var mcpServerFlags: McpServerFlags

  override fun run() {
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
        installReportingMcp(
          apiClient,
          allowedHosts = mcpServerFlags.allowedHosts,
          oauthProtectedResource = mcpServerFlags.oauthProtectedResource,
          oauthAuthorizationServers = mcpServerFlags.oauthAuthorizationServers,
          oauthScopesSupported = mcpServerFlags.oauthScopesSupported,
          oauthResourceDocumentation = mcpServerFlags.oauthResourceDocumentation,
        )
      }
      .start(wait = true)
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

  @CommandLine.Option(
    names = ["--oauth-protected-resource"],
    description =
      [
        "OAuth 2.0 resource identifier for this server (RFC 9728), e.g. its public URL. When set " +
          "together with --oauth-authorization-server, the server serves Protected Resource " +
          "Metadata at /.well-known/oauth-protected-resource so MCP clients can discover how to " +
          "obtain a token. Unset (default) disables the endpoint and leaves behavior unchanged."
      ],
  )
  var oauthProtectedResource: String? = null
    private set

  @CommandLine.Option(
    names = ["--oauth-authorization-server"],
    description =
      [
        "Issuer URL of an OAuth 2.0 authorization server that issues tokens for this resource " +
          "(RFC 9728 authorization_servers; may be repeated)."
      ],
  )
  var oauthAuthorizationServers: List<String> = emptyList()
    private set

  @CommandLine.Option(
    names = ["--oauth-scope"],
    description =
      [
        "OAuth 2.0 scope a client should request to access this resource (RFC 9728 " +
          "scopes_supported; may be repeated). When set, it is advertised in the Protected " +
          "Resource Metadata so clients know which scopes to request. Unset (default) omits the " +
          "field."
      ],
  )
  var oauthScopesSupported: List<String> = emptyList()
    private set

  @CommandLine.Option(
    names = ["--oauth-resource-documentation"],
    description =
      [
        "URL of human-readable documentation for this resource (RFC 9728 resource_documentation). " +
          "When set, it is advertised in the Protected Resource Metadata. Unset (default) omits " +
          "the field."
      ],
  )
  var oauthResourceDocumentation: String? = null
    private set

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables verbose downstream gRPC logging."],
    defaultValue = "false",
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
}

fun main(args: Array<String>) = commandLineMain(ReportingMcpServerDaemon(), args)
