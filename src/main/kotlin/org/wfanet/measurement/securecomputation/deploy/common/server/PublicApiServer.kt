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

package org.wfanet.measurement.securecomputation.deploy.common.server

import io.grpc.BindableService
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.asCoroutineDispatcher
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.Services
import picocli.CommandLine

private const val SERVER_NAME = "SecureComputationApiServer"

@CommandLine.Command(name = SERVER_NAME)
class PublicApiServer : Runnable {
  @CommandLine.Mixin private lateinit var serverFlags: CommonServer.Flags
  @CommandLine.Mixin private lateinit var serviceFlags: ServiceFlags

  @CommandLine.Option(
    names = ["--secure-computation-internal-api-target"],
    description = ["gRPC target of the Secure Computation internal API server"],
    required = true,
  )
  private lateinit var internalApiTarget: String

  @CommandLine.Option(
    names = ["--secure-computation-internal-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Secure Computation internal API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --secure-computation-internal-api-target.",
      ],
    required = false,
  )
  private var internalApiCertHost: String? = null

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false",
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for gRPC channels to shutdown."],
  )
  private lateinit var channelShutdownTimeout: Duration

  override fun run() {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = serverFlags.tlsFlags.certFile,
        privateKeyFile = serverFlags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = serverFlags.tlsFlags.certCollectionFile,
      )
    val internalApiChannel =
      buildMutualTlsChannel(internalApiTarget, clientCerts, internalApiCertHost)
        .withShutdownTimeout(channelShutdownTimeout)
        .withVerboseLogging(debugVerboseGrpcClientLogging)

    val services: List<BindableService> =
      Services.build(internalApiChannel, serviceFlags.executor.asCoroutineDispatcher()).toList()

    val server: CommonServer = CommonServer.fromFlags(serverFlags, SERVER_NAME, services)
    server.start().blockUntilShutdown()
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(PublicApiServer(), args)
  }
}
