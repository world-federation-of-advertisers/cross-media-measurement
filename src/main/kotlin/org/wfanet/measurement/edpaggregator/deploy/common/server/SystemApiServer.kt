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

package org.wfanet.measurement.edpaggregator.deploy.common.server

import io.grpc.ServerServiceDefinition
import java.io.File
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.RateLimitConfig
import org.wfanet.measurement.edpaggregator.service.v1alpha.Services
import picocli.CommandLine

private const val SERVER_NAME = "EdpAggregatorApiServer"

@CommandLine.Command(name = SERVER_NAME)
class SystemApiServer : Runnable {
  @CommandLine.Mixin private lateinit var serverFlags: CommonServer.Flags
  @CommandLine.Mixin private lateinit var serviceFlags: ServiceFlags

  @CommandLine.Option(
    names = ["--edp-aggregator-internal-api-target"],
    description = ["gRPC target of the EDP Aggregator internal API server"],
    required = true,
  )
  private lateinit var internalApiTarget: String

  @CommandLine.Option(
    names = ["--edp-aggregator-internal-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the EDP Aggregator internal API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --edp-aggregator-internal-api-target.",
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

  @CommandLine.Option(
    names = ["--rate-limit-config-file"],
    description =
      [
        "File path to a RateLimitConfig protobuf message in text format.",
        "If unset, expensive List methods are bounded by a conservative default and all other " +
          "methods are unlimited.",
      ],
    required = false,
  )
  private var rateLimitConfigFile: File? = null

  private val rateLimitConfig: RateLimitConfig by lazy {
    val configFile = rateLimitConfigFile
    if (configFile == null) {
      EdpaApiRateLimiting.defaultConfig(EdpaApiRateLimiting.PUBLIC_API_EXPENSIVE_METHODS)
    } else {
      parseTextProto(configFile, RateLimitConfig.getDefaultInstance())
    }
  }

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
    val serviceDispatcher: CoroutineDispatcher = serviceFlags.executor.asCoroutineDispatcher()

    val rateLimitingInterceptor = EdpaApiRateLimiting.interceptor(rateLimitConfig)
    val services: List<ServerServiceDefinition> =
      Services.build(internalApiChannel, serviceDispatcher).toList().map {
        it.withInterceptor(rateLimitingInterceptor)
      }

    val server: CommonServer = CommonServer.fromFlags(serverFlags, SERVER_NAME, services)
    server.start().blockUntilShutdown()
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(SystemApiServer(), args)
  }
}
