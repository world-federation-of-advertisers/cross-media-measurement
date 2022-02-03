// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.service.internal.testing.integration

import io.grpc.ManagedChannel
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.testing.SigningCertsTesting
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoDate
import picocli.CommandLine

@CommandLine.Command(
  name = "RunPanelMatchResourceSetupJob",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private val EXCHANGE_DATE = LocalDate.now()
private const val API_VERSION = "v2alpha"
private const val SCHEDULE = "@daily"

private fun run(@CommandLine.Mixin flags: PanelMatchResourceSetupFlags) {
  val clientCerts =
    SigningCertsTesting.fromPemFiles(
      certificateFile = flags.tlsFlags.certFile,
      privateKeyFile = flags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
    )
  val kingdomInternalApiChannel: ManagedChannel =
    buildMutualTlsChannel(
      flags.kingdomInternalApiTarget,
      clientCerts,
      flags.kingdomInternalApiCertHost
    )

  val exchangeWorkflow: ExchangeWorkflow by lazy {
    flags
      .exchangeWorkflow
      .inputStream()
      .use { input -> parseTextProto(input.bufferedReader(), exchangeWorkflow {}) }
      .copy { firstExchangeDate = EXCHANGE_DATE.toProtoDate() }
  }

  runBlocking {
    // Runs the resource setup job.
    PanelMatchResourceSetup(kingdomInternalApiChannel)
      .createResourcesForWorkflow(
        exchangeSchedule = SCHEDULE,
        apiVersion = API_VERSION,
        exchangeWorkflow = exchangeWorkflow,
        exchangeDate = EXCHANGE_DATE.toProtoDate(),
        runId = flags.runId
      )
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
