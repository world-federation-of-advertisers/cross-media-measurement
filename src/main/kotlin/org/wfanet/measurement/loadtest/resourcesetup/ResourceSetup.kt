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
import com.google.protobuf.kotlin.toByteString
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.api.v2alpha.createDataProviderRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.kingdom.service.api.v2alpha.withIdToken

/** A Job preparing resources required for the correctness test. */
class ResourceSetup(
  private val dataProvidersClient: DataProvidersCoroutineStub,
  private val certificatesClient: CertificatesCoroutineStub,
  private val measurementConsumersClient: MeasurementConsumersCoroutineStub,
  private val runId: String
) {

  /** Process to create resources. */
  suspend fun process(
    dataProviderContents: List<EntityContent>,
    measurementConsumerContent: EntityContent,
    duchyCerts: List<DuchyCert>,
    measurementConsumerCreationToken: String,
    idToken: String,
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
    val measurementConsumer =
      createMeasurementConsumer(
        measurementConsumerContent,
        measurementConsumerCreationToken,
        idToken
      )
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
    val encryptionPublicKey = dataProviderContent.encryptionPublicKey
    val request = createDataProviderRequest {
      dataProvider =
        dataProvider {
          certificateDer = dataProviderContent.signingKey.certificate.encoded.toByteString()
          publicKey = signEncryptionPublicKey(encryptionPublicKey, dataProviderContent.signingKey)
          displayName = dataProviderContent.displayName
        }
    }
    return dataProvidersClient.createDataProvider(request)
  }

  suspend fun createMeasurementConsumer(
    measurementConsumerContent: EntityContent,
    measurementConsumerCreationToken: String,
    idToken: String,
  ): MeasurementConsumer {
    val encryptionPublicKey = measurementConsumerContent.encryptionPublicKey
    val request = createMeasurementConsumerRequest {
      measurementConsumer =
        measurementConsumer {
          certificateDer = measurementConsumerContent.signingKey.certificate.encoded.toByteString()
          publicKey =
            signEncryptionPublicKey(encryptionPublicKey, measurementConsumerContent.signingKey)
          displayName = measurementConsumerContent.displayName
        }
      this.measurementConsumerCreationToken = measurementConsumerCreationToken
    }
    return measurementConsumersClient.withIdToken(idToken).createMeasurementConsumer(request)
  }

  suspend fun createDuchyCertificate(duchyCert: DuchyCert): Certificate {
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
  /** The consent signaling encryption key. */
  val encryptionPublicKey: EncryptionPublicKey,
  /** The consent signaling signing key. */
  val signingKey: SigningKeyHandle
)

data class DuchyCert(
  /** The external duchy Id. */
  val duchyId: String,
  /** The consent signaling certificate in DER format. */
  val consentSignalCertificateDer: ByteString
)
