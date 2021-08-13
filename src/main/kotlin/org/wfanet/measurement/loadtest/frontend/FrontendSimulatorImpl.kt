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

package org.wfanet.measurement.loadtest.frontend

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import java.time.Duration
import java.util.logging.Logger
import kotlin.random.Random
import kotlinx.coroutines.delay
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderList
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.GetMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.HybridCipherSuite
import org.wfanet.measurement.api.v2alpha.HybridCipherSuite.DataEncapsulationMechanism
import org.wfanet.measurement.api.v2alpha.HybridCipherSuite.KeyEncapsulationMechanism
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.Measurement.Result
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.crypto.hybridencryption.HybridCryptor
import org.wfanet.measurement.consent.crypto.hybridencryption.testing.ReversingHybridCryptor
import org.wfanet.measurement.consent.crypto.keystore.KeyStore
import org.wfanet.measurement.consent.crypto.keystore.PrivateKeyHandle

private const val DATA_PROVIDER_WILDCARD = "dataProviders/-"
private val CIPHER_SUITE =
  HybridCipherSuite.newBuilder()
    .apply {
      kem = KeyEncapsulationMechanism.ECDH_P256_HKDF_HMAC_SHA256
      dem = DataEncapsulationMechanism.AES_128_GCM
    }
    .build()

data class MeasurementConsumerData(
  // The MC's public API resource name
  val name: String,
  // The id of the MC's consent signaling private key in keyStore
  val consentSignalingPrivateKeyId: String,
  // The id of the MC's encryption private key in keyStore
  val encryptionPrivateKeyId: String
)

class FrontendSimulatorImpl(
  private val measurementConsumerData: MeasurementConsumerData,
  private val outputDpParams: DifferentialPrivacyParams,
  private val keyStore: KeyStore,
  private val dataProvidersClient: DataProvidersCoroutineStub,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val measurementsClient: MeasurementsCoroutineStub,
  private val measurementConsumersClient: MeasurementConsumersCoroutineStub,
  private val runId: String
) : FrontendSimulator {

  suspend fun process() {
    // Create a new measurement on behalf of the measurement consumer.
    val measurementConsumer = getMeasurementConsumer(measurementConsumerData.name)
    val createdMeasurement = createMeasurement(measurementConsumer)

    // Wait until the computation is DONE.
    logger.info("Waiting 2 min...")
    delay(Duration.ofMinutes(2).toMillis())

    // Get the CMMS computed result and compare it with the expected result.
    val mpcResult = getResult(createdMeasurement.name)
    val expectedResult = getExpectedResult()
    assertThat(mpcResult).isEqualTo(expectedResult)
  }

  override suspend fun createMeasurement(measurementConsumer: MeasurementConsumer): Measurement {
    val eventGroups = listEventGroups(measurementConsumer.name)
    val serializedDataProviderList =
      DataProviderList.newBuilder()
        .addAllDataProvider(eventGroups.map { extractDataProviderName(it.name) })
        .build()
        .toByteString()
    val dataProviderListSalt = Random.Default.nextBytes(32).toByteString()
    val dataProviderListHash =
      ByteString.copyFromUtf8("TODO: call createDataProviderListHash(list, salt)")
    val dataProviderEntries =
      eventGroups.map { createDataProviderEntry(it, measurementConsumer, dataProviderListHash) }

    val request =
      CreateMeasurementRequest.newBuilder()
        .apply {
          measurementBuilder.also {
            it.measurementConsumerCertificate = measurementConsumer.certificate
            it.measurementSpec =
              signMeasurementSpec(
                newMeasurementSpec(measurementConsumer.publicKey.data),
                PrivateKeyHandle(measurementConsumerData.consentSignalingPrivateKeyId, keyStore),
                readCertificate(measurementConsumer.certificateDer)
              )
            it.serializedDataProviderList = serializedDataProviderList
            it.dataProviderListSalt = dataProviderListSalt
            it.addAllDataProviders(dataProviderEntries)
            it.measurementReferenceId = runId
          }
        }
        .build()
    return measurementsClient.createMeasurement(request)
  }

  override suspend fun getResult(measurementName: String): Result {
    TODO("verify and decrypt result")
  }

  override suspend fun getExpectedResult(): Result {
    TODO("compute expected result using unencrypted sketches..")
  }

  private suspend fun getMeasurementConsumer(name: String): MeasurementConsumer {
    val request = GetMeasurementConsumerRequest.newBuilder().also { it.name = name }.build()
    return measurementConsumersClient.getMeasurementConsumer(request)
  }

  private fun newMeasurementSpec(serializedMeasurementPublicKey: ByteString): MeasurementSpec {
    return MeasurementSpec.newBuilder()
      .apply {
        measurementPublicKey = serializedMeasurementPublicKey
        cipherSuite = CIPHER_SUITE
        reachAndFrequencyBuilder.apply {
          reachPrivacyParams = outputDpParams
          frequencyPrivacyParams = outputDpParams
        }
      }
      .build()
  }

  private suspend fun listEventGroups(measurementConsumer: String): List<EventGroup> {
    val request =
      ListEventGroupsRequest.newBuilder()
        .apply {
          parent = DATA_PROVIDER_WILDCARD
          filterBuilder.addMeasurementConsumers(measurementConsumer)
        }
        .build()
    return eventGroupsClient.listEventGroups(request).eventGroupsList
  }

  private fun extractDataProviderName(eventGroupName: String): String {
    val eventGroupKey = EventGroupKey.fromName(eventGroupName) ?: error("Invalid eventGroup name.")
    return DataProviderKey(eventGroupKey.dataProviderId).toName()
  }

  private suspend fun getDataProvider(name: String): DataProvider {
    val request = GetDataProviderRequest.newBuilder().also { it.name = name }.build()
    return dataProvidersClient.getDataProvider(request)
  }

  private suspend fun createDataProviderEntry(
    eventGroup: EventGroup,
    measurementConsumer: MeasurementConsumer,
    dataProviderListHash: ByteString
  ): DataProviderEntry {
    val dataProvider = getDataProvider(extractDataProviderName(eventGroup.name))
    val requisitionSpec =
      RequisitionSpec.newBuilder()
        .also {
          it.addEventGroupsBuilder().apply {
            key = eventGroup.name
            // TODO: populate other fields when the EventGroup design is done.
          }
          it.measurementPublicKey = measurementConsumer.publicKey.data
          it.dataProviderListHash = dataProviderListHash
        }
        .build()
    val signedRequisitionSpec =
      signRequisitionSpec(
        requisitionSpec,
        PrivateKeyHandle(measurementConsumerData.consentSignalingPrivateKeyId, keyStore),
        readCertificate(measurementConsumer.certificateDer)
      )
    return dataProvider.toDataProviderEntry(signedRequisitionSpec)
  }

  private suspend fun DataProvider.toDataProviderEntry(
    signedRequisitionSpec: SignedData
  ): DataProviderEntry {
    val value =
      DataProviderEntry.Value.newBuilder()
        .also {
          it.dataProviderCertificate = this.certificate
          it.dataProviderPublicKey = this.publicKey
          it.encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signedRequisitionSpec,
              EncryptionPublicKey.parseFrom(this.publicKey.data),
              CIPHER_SUITE,
              ::fakeGetHybridCryptorForCipherSuite // TODO: use the real HybridCryptor.
            )
        }
        .build()
    return DataProviderEntry.newBuilder()
      .also {
        it.key = this.name
        it.value = value
      }
      .build()
  }

  // TODO: delete this fake when the EciesCryptor is done.
  private fun fakeGetHybridCryptorForCipherSuite(cipherSuite: HybridCipherSuite): HybridCryptor {
    return ReversingHybridCryptor()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
