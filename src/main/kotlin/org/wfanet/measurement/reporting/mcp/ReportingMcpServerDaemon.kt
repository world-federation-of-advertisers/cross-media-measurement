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
import io.ktor.server.engine.sslConnector
import java.security.KeyStore
import java.security.cert.Certificate
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.reporting.mcp.grpc.ReportingPublicApiClient
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import picocli.CommandLine

private const val TLS_KEY_ALIAS = "reporting-mcp-server"

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

    // Serve TLS in-server (LoadBalancer + in-server TLS termination, mirroring
    // reporting-grpc-gateway). SigningKeyHandle intentionally hides the private
    // key, so re-read the cert + key from the PEM files to build a KeyStore for
    // Ktor's sslConnector.
    val serverCertificate = readCertificate(tlsFlags.certFile)
    val serverPrivateKey =
      readPrivateKey(tlsFlags.privateKeyFile, serverCertificate.publicKey.algorithm)
    val keyStorePassword = CharArray(0)
    val keyStore =
      KeyStore.getInstance("PKCS12").apply {
        load(null, null)
        setKeyEntry(
          TLS_KEY_ALIAS,
          serverPrivateKey,
          keyStorePassword,
          arrayOf<Certificate>(serverCertificate),
        )
      }

    embeddedServer(
        CIO,
        configure = {
          sslConnector(
            keyStore = keyStore,
            keyAlias = TLS_KEY_ALIAS,
            keyStorePassword = { keyStorePassword },
            privateKeyPassword = { keyStorePassword },
          ) {
            host = mcpServerFlags.host
            port = mcpServerFlags.port
          }
        },
      ) {
        installReportingMcp(apiClient, mcpServerFlags.allowedHosts)
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
    description = ["HTTPS port for the MCP server. TLS is terminated in-server."],
    defaultValue = "8443",
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

fun main(args: Array<String>) = commandLineMain(ReportingMcpServerDaemon(), args)
