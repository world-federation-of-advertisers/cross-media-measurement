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

import com.google.protobuf.ByteString
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey.Type.EC_P256
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.api.v2alpha.createDataProviderRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.consent.client.dataprovider.signEncryptionPublicKey as signEdpEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey as signMcEncryptionPublicKey
import org.wfanet.measurement.consent.crypto.keystore.KeyStore
import org.wfanet.measurement.consent.crypto.keystore.PrivateKeyHandle

/** A Job preparing resources required for the correctness test. */
class ResourceSetup(
  private val keyStore: KeyStore,
  private val dataProvidersClient: DataProvidersCoroutineStub,
  private val certificatesClient: CertificatesCoroutineStub,
  private val measurementConsumersClient: MeasurementConsumersCoroutineStub,
  private val runId: String
) {

  /** Process to create resources. */
  suspend fun process(
    dataProviderContents: List<EntityContent>,
    measurementConsumerContent: EntityContent,
    duchyCerts: List<DuchyCert>
  ) {
    logger.info("Starting with RunID: $runId ...")

    // Step 1: Create the EDPs via the public API.
    dataProviderContents.forEach {
      val dataProvider = createDataProvider(it)
      logger.info(
        "Successfully created data provider: ${dataProvider.name} " +
          "with certificate ${dataProvider.certificate}"
      )
    }

    // Step 2: Create the MC via the public API.
    val measurementConsumer = createMeasurementConsumer(measurementConsumerContent)
    logger.info(
      "Successfully created measurement consumer: ${measurementConsumer.name} " +
        "with certificate ${measurementConsumer.certificate} ..."
    )

    // Step 3: Create certificate for each duchy.
    duchyCerts.forEach {
      val certificate = createDuchyCertificate(it)
      logger.info("Successfully created certificate ${certificate.name}")
    }
  }

  suspend fun createDataProvider(dataProviderContent: EntityContent): DataProvider {
    val encryptionPublicKey = encryptionPublicKey {
      type = EC_P256
      publicKeyInfo = dataProviderContent.encryptionPublicKeyDer
    }

    val privateKeyHandle = PrivateKeyHandle(dataProviderContent.displayName, keyStore)
    val request = createDataProviderRequest {
      dataProvider =
        dataProvider {
          certificateDer = dataProviderContent.consentSignalCertificateDer
          publicKey =
            signEdpEncryptionPublicKey(
              encryptionPublicKey,
              privateKeyHandle,
              readCertificate(dataProviderContent.consentSignalCertificateDer)
            )
          displayName = dataProviderContent.displayName
        }
    }
    return dataProvidersClient.createDataProvider(request)
  }

  suspend fun createMeasurementConsumer(
    measurementConsumerContent: EntityContent
  ): MeasurementConsumer {
    val encryptionPublicKey = encryptionPublicKey {
      // TODO: Get the type using the consent-signaling-client lib.
      type = EC_P256
      publicKeyInfo = measurementConsumerContent.encryptionPublicKeyDer
    }
    val privateKeyHandle = PrivateKeyHandle(measurementConsumerContent.displayName, keyStore)
    val request = createMeasurementConsumerRequest {
      measurementConsumer =
        measurementConsumer {
          certificateDer = measurementConsumerContent.consentSignalCertificateDer
          publicKey =
            signMcEncryptionPublicKey(
              encryptionPublicKey,
              privateKeyHandle,
              readCertificate(measurementConsumerContent.consentSignalCertificateDer)
            )
          displayName = measurementConsumerContent.displayName
        }
    }
    return measurementConsumersClient.createMeasurementConsumer(request)
  }

  private suspend fun createDuchyCertificate(duchyCert: DuchyCert): Certificate {
    val request = createCertificateRequest {
      parent = DuchyKey(duchyCert.duchyId).toName()
      certificate = certificate { x509Der = duchyCert.consentSignalCertificateDer }
    }
    return certificatesClient.createCertificate(request)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

/** Relevant data required to create entity like EDP or MC. */
data class EntityContent(
  /** The display name of the entity. */
  val displayName: String,
  /** The private key mapping the consent signaling certificate in DER format. */
  val consentSignalPrivateKeyDer: ByteString,
  /** The consent signaling certificate in DER format. */
  val consentSignalCertificateDer: ByteString,
  /** The ASN.1 SubjectPublicKeyInfo in DER format */
  val encryptionPublicKeyDer: ByteString
)

data class DuchyCert(
  /** The external duchy Id. */
  val duchyId: String,
  /** The consent signaling certificate in DER format. */
  val consentSignalCertificateDer: ByteString
)
