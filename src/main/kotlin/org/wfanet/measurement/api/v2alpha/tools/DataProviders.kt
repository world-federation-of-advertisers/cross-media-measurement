/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.api.v2alpha.tools

import io.grpc.ManagedChannel
import java.io.File
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import picocli.CommandLine

private val CHANNEL_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30)

/**
 * Command-line utility for [DataProviders] messages. This utility should never be used to create a
 * data provider.
 *
 * Use the `help` subcommand for usage help.
 */
@CommandLine.Command(
  description = ["Utility for Certificate messages."],
  subcommands = [CommandLine.HelpCommand::class, Get::class]
)
class DataProviders private constructor() : Runnable {
  override fun run() {
    // No-op. See subcommands.
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(DataProviders(), args)
  }
}

@CommandLine.Command(name = "get", showDefaultValues = true)
private class Get : Runnable {

  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags

  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  private lateinit var target: String

  @CommandLine.Option(
    names = ["--certificate-der-file"],
    description = ["File containing format-specific key data"],
    required = true
  )
  private lateinit var certificateDerFile: File

  @CommandLine.Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target.",
      ],
    required = false,
  )
  private var certHost: String? = null

  @CommandLine.Option(
    names = ["--data-provider-resource"],
    description = ["API resource name of the data provider resource"],
    required = true
  )
  lateinit var dataProviderResource: String

  @CommandLine.Option(names = ["--out", "-o"], description = ["Output file"], required = true)
  private lateinit var outFile: File

  val kingdomChannel: ManagedChannel by lazy {
    buildMutualTlsChannel(target, tlsFlags.signingCerts, certHost)
      .withShutdownTimeout(CHANNEL_SHUTDOWN_TIMEOUT)
  }

  private val dataProvidersClient: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(kingdomChannel)
  }

  override fun run() {
    val dataProvider = runBlocking {
      dataProvidersClient.getDataProvider(getDataProviderRequest { name = dataProviderResource })
    }

    println("Data Provider Resource: ${dataProvider.name}")

    outFile.outputStream().use { signature.writeTo(it) }
  }
}
