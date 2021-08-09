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

import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.CreateDataProviderRequest
import org.wfanet.measurement.api.v2alpha.CreateMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey.Type.EC_P256
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.consent.client.dataprovider.signEncryptionPublicKey as signEdpEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey as signMcEncryptionPublicKey
import org.wfanet.measurement.consent.crypto.keystore.KeyStore
import org.wfanet.measurement.consent.crypto.keystore.PrivateKeyHandle

class ResourceSetupImpl(
  private val keyStore: KeyStore,
  private val dataProvidersClient: DataProvidersCoroutineStub,
  private val measurementConsumersClient: MeasurementConsumersCoroutineStub,
  private val runId: String
) : ResourceSetup {

  suspend fun process(
    dataProviderContents: List<EntityContent>,
    measurementConsumerContent: EntityContent
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
  }

  override suspend fun createDataProvider(dataProviderContent: EntityContent): DataProvider {
    val encryptionPublicKey =
      EncryptionPublicKey.newBuilder()
        .apply {
          type = EC_P256
          publicKeyInfo = dataProviderContent.encryptionPublicKeyDer
        }
        .build()
    val privateKeyHandle = PrivateKeyHandle(dataProviderContent.displayName, keyStore)
    val request =
      CreateDataProviderRequest.newBuilder()
        .apply {
          dataProviderBuilder.apply {
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
        .build()
    return dataProvidersClient.createDataProvider(request)
  }

  override suspend fun createMeasurementConsumer(
    measurementConsumerContent: EntityContent
  ): MeasurementConsumer {
    val encryptionPublicKey =
      EncryptionPublicKey.newBuilder()
        .apply {
          // TODO: Get the type using the consent-signaling-client lib.
          type = EC_P256
          publicKeyInfo = measurementConsumerContent.encryptionPublicKeyDer
        }
        .build()
    val privateKeyHandle = PrivateKeyHandle(measurementConsumerContent.displayName, keyStore)
    val request =
      CreateMeasurementConsumerRequest.newBuilder()
        .apply {
          measurementConsumerBuilder.apply {
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
        .build()
    return measurementConsumersClient.createMeasurementConsumer(request)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
