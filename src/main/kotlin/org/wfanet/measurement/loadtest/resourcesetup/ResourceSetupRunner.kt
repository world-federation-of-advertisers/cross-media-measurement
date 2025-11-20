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

import io.grpc.Channel
import java.security.cert.X509Certificate
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub as InternalAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub as InternalCertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub as InternalModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineStub as InternalModelProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineStub as InternalModelSuitesCoroutineStub
import picocli.CommandLine

@CommandLine.Command(
  name = "RunResourceSetupJob",
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(@CommandLine.Mixin flags: ResourceSetupFlags) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = flags.tlsFlags.certFile,
      privateKeyFile = flags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
    )
  val v2alphaPublicApiChannel: Channel =
    buildMutualTlsChannel(
      flags.kingdomPublicApiFlags.target,
      clientCerts,
      flags.kingdomPublicApiFlags.certHost,
    )
  val kingdomInternalApiChannel: Channel =
    buildMutualTlsChannel(
        flags.kingdomInternalApiFlags.target,
        clientCerts,
        flags.kingdomInternalApiFlags.certHost,
      )
      .withDefaultDeadline(flags.kingdomInternalApiFlags.defaultDeadlineDuration)
  val internalDataProvidersStub = InternalDataProvidersCoroutineStub(kingdomInternalApiChannel)
  val internalAccountsStub = InternalAccountsCoroutineStub(kingdomInternalApiChannel)
  val measurementConsumersStub = MeasurementConsumersCoroutineStub(v2alphaPublicApiChannel)
  val internalCertificatesStub = InternalCertificatesCoroutineStub(kingdomInternalApiChannel)
  val internalModelProvidersStub = InternalModelProvidersCoroutineStub(kingdomInternalApiChannel)
  val internalModelSuitesStub = InternalModelSuitesCoroutineStub(kingdomInternalApiChannel)
  val internalModelLinesStub = InternalModelLinesCoroutineStub(kingdomInternalApiChannel)
  val accountsStub = AccountsCoroutineStub(v2alphaPublicApiChannel)
  val apiKeysStub = ApiKeysCoroutineStub(v2alphaPublicApiChannel)

  val dataProviderContents =
    flags.dataProviderParams.map {
      EntityContent(
        displayName = it.displayName,
        signingKey = loadSigningKey(it.consentSignalingCertFile, it.consentSignalingKeyFile),
        encryptionPublicKey = loadPublicKey(it.encryptionPublicKeysetFile).toEncryptionPublicKey(),
      )
    }
  val measurementConsumerContent =
    EntityContent(
      displayName = "mc_001",
      signingKey = loadSigningKey(flags.mcCsCertDerFile, flags.mcCsKeyDerFile),
      encryptionPublicKey = loadPublicKey(flags.mcEncryptionPublicKeyset).toEncryptionPublicKey(),
    )
  val duchyCerts =
    flags.duchyCsCertDerFiles.map {
      DuchyCert(duchyId = it.key, consentSignalCertificateDer = it.value.readByteString())
    }
  val modelProviderRootCert: X509Certificate = readCertificate(flags.modelProviderRootCertFile)
  val modelProviderAkid =
    checkNotNull(modelProviderRootCert.subjectKeyIdentifier) {
      "ModelProvider root cert missing SKID"
    }

  runBlocking {
    // Runs the resource setup job.
    ResourceSetup(
        internalAccountsClient = internalAccountsStub,
        internalDataProvidersClient = internalDataProvidersStub,
        internalCertificatesClient = internalCertificatesStub,
        accountsClient = accountsStub,
        apiKeysClient = apiKeysStub,
        measurementConsumersClient = measurementConsumersStub,
        runId = flags.runId,
        requiredDuchies = flags.requiredDuchies,
        bazelConfigName = flags.bazelConfigName,
        outputDir = flags.outputDir,
        internalModelProvidersClient = internalModelProvidersStub,
        internalModelSuitesClient = internalModelSuitesStub,
        internalModelLinesClient = internalModelLinesStub,
      )
      .process(dataProviderContents, measurementConsumerContent, duchyCerts, modelProviderAkid)
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
