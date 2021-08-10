// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.resourcesetup

import io.grpc.ManagedChannel
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.consent.crypto.keystore.testing.InMemoryKeyStore
import picocli.CommandLine

@CommandLine.Command(
  name = "RunResourceSetupJob",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: ResourceSetupFlags) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = flags.tlsFlags.certFile,
      privateKeyFile = flags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
    )
  val v2alphaPublicApiChannel: ManagedChannel =
    buildMutualTlsChannel(
      flags.KingdomPublicApiFlags.target,
      clientCerts,
      flags.KingdomPublicApiFlags.certHost
    )
  val dataProvidersStub = DataProvidersCoroutineStub(v2alphaPublicApiChannel)
  val measurementConsumersStub = MeasurementConsumersCoroutineStub(v2alphaPublicApiChannel)

  val inMemoryKeyStore = InMemoryKeyStore()

  // Makes sure the three maps contain the same set of EDPs.
  require(
    flags.edpCsCertDerFiles.keys == flags.edpCsKeyDerFiles.keys &&
      flags.edpCsCertDerFiles.keys == flags.edpEncryptionPublicKeyDerFiles.keys
  )
  val dataProviderContents =
    flags.edpCsCertDerFiles.map {
      EntityContent(
        displayName = it.key,
        consentSignalPrivateKeyDer = flags.edpCsKeyDerFiles[it.key]!!.readBytes().toByteString(),
        consentSignalCertificateDer = it.value.readBytes().toByteString(),
        encryptionPublicKeyDer =
          flags.edpEncryptionPublicKeyDerFiles[it.key]!!.readBytes().toByteString()
      )
    }
  val measurementConsumerContent =
    EntityContent(
      displayName = "mc_001",
      consentSignalPrivateKeyDer = flags.mcCsKeyDerFiles.readBytes().toByteString(),
      consentSignalCertificateDer = flags.mcCsCertDerFile.readBytes().toByteString(),
      encryptionPublicKeyDer = flags.mcEncryptionPublicKeyDerFile.readBytes().toByteString(),
    )

  runBlocking {
    // Populates data to the inMemoryKeyStore.
    dataProviderContents.forEach {
      inMemoryKeyStore.storePrivateKeyDer(it.displayName, it.consentSignalPrivateKeyDer)
    }
    inMemoryKeyStore.storePrivateKeyDer(
      measurementConsumerContent.displayName,
      measurementConsumerContent.consentSignalPrivateKeyDer
    )

    // Runs the resource setup job.
    ResourceSetupImpl(inMemoryKeyStore, dataProvidersStub, measurementConsumersStub, flags.runId)
      .process(dataProviderContents, measurementConsumerContent)
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
