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

package org.wfanet.measurement.loadtest.panelmatchresourcesetup

import io.grpc.ManagedChannel
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import picocli.CommandLine

private val EXCHANGE_DATE = LocalDate.now()
private const val SCHEDULE = "@daily"

@CommandLine.Command(
  name = "RunPanelMatchResourceSetupJob",
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(@CommandLine.Mixin flags: PanelMatchResourceSetupFlags) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = flags.tlsFlags.certFile,
      privateKeyFile = flags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
    )
  val kingdomInternalApiChannel: ManagedChannel =
    buildMutualTlsChannel(
      flags.kingdomInternalApiTarget,
      clientCerts,
      flags.kingdomInternalApiCertHost,
    )

  val dataProviderContent =
    EntityContent(
      displayName = flags.edpDisplayName,
      signingKey = loadSigningKey(flags.edpCertDerFile, flags.edpKeyDerFile),
      encryptionPublicKey = loadPublicKey(flags.edpEncryptionPublicKeyset).toEncryptionPublicKey(),
    )

  val exchangeWorkflow: ExchangeWorkflow by lazy {
    flags.exchangeWorkflow
      .inputStream()
      .use { input -> parseTextProto(input.bufferedReader(), exchangeWorkflow {}) }
      .copy { firstExchangeDate = EXCHANGE_DATE.toProtoDate() }
  }

  runBlocking {
    // Runs the resource setup job.
    PanelMatchResourceSetup(kingdomInternalApiChannel)
      .process(
        dataProviderContent = dataProviderContent,
        exchangeSchedule = SCHEDULE,
        exchangeWorkflow = exchangeWorkflow,
        exchangeDate = EXCHANGE_DATE.toProtoDate(),
      )
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
