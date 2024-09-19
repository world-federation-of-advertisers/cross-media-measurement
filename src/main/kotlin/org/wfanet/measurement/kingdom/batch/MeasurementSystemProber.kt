/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.batch

import com.google.protobuf.Any
import com.google.type.interval
import io.grpc.StatusException
import java.io.File
import java.security.SecureRandom
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequestKt
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec

class MeasurementSystemProber(
  private val measurementConsumerName: String,
  private val dataProviderNames: List<String>,
  private val apiAuthenticationKey: String,
  private val privateKeyDerFile: File,
  private val measurementLookbackDuration: Duration,
  private val durationBetweenMeasurement: Duration,
  private val measurementConsumersStub:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub,
  private val measurementsStub:
    org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub,
  private val dataProvidersStub: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val eventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val clock: Clock = Clock.systemUTC(),
) {
  suspend fun run() {
    if (shouldCreateNewMeasurement()) {
      createMeasurement()
      // TODO(@roaminggypsy): Report measurement result with OpenTelemetry
    }
  }

  private suspend fun createMeasurement() {
    val dataProviderNameToEventGroup: Map<String, EventGroup> =
      buildDataProviderNameToEventGroup(dataProviderNames, dataProvidersStub, apiAuthenticationKey)

    val measurementConsumer: MeasurementConsumer =
      try {
        measurementConsumersStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getMeasurementConsumer(getMeasurementConsumerRequest { name = measurementConsumerName })
      } catch (e: StatusException) {
        throw Exception("Unable to get measurement consumer $measurementConsumerName", e)
      }
    val measurementConsumerCertificate = readCertificate(measurementConsumer.certificateDer)
    val measurementConsumerPrivateKey =
      readPrivateKey(
        privateKeyDerFile.readByteString(),
        measurementConsumerCertificate.publicKey.algorithm,
      )
    val measurementConsumerSigningKey =
      SigningKeyHandle(measurementConsumerCertificate, measurementConsumerPrivateKey)
    val packedMeasurementEncryptionPublicKey = measurementConsumer.publicKey.message

    val measurement = measurement {
      this.measurementConsumerCertificate = measurementConsumer.certificate
      dataProviders +=
        dataProviderNames.map {
          getDataProviderEntry(
            it,
            dataProviderNameToEventGroup[it]!!,
            measurementConsumerSigningKey,
            packedMeasurementEncryptionPublicKey,
          )
        }
      val unsignedMeasurementSpec = measurementSpec {
        measurementPublicKey = packedMeasurementEncryptionPublicKey
        nonceHashes += this@measurement.dataProviders.map { it.value.nonceHash }
        vidSamplingInterval = vidSamplingInterval {
          start = 0f
          width = 1f
        }
        reachAndFrequency =
          MeasurementSpecKt.reachAndFrequency {
            reachPrivacyParams = differentialPrivacyParams {
              epsilon = 0.005
              delta = 1e-15
            }
            frequencyPrivacyParams = differentialPrivacyParams {
              epsilon = 0.005
              delta = 1e-15
            }
            maximumFrequency = 1
          }
      }

      this.measurementSpec =
        signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
    }

    val response =
      try {
        measurementsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .createMeasurement(
            createMeasurementRequest {
              this.parent = measurementConsumerName
              this.measurement = measurement
            }
          )
      } catch (e: StatusException) {
        throw Exception("Unable to create a prober measurement", e)
      }
    logger.info(
      "A new prober measurement for measurement consumer $measurementConsumerName is created: $response"
    )
  }

  private suspend fun buildDataProviderNameToEventGroup(
    dataProviderNames: List<String>,
    dataProvidersStub: DataProvidersGrpcKt.DataProvidersCoroutineStub,
    apiAuthenticationKey: String,
  ): MutableMap<String, EventGroup> {
    val dataProviderNameToEventGroup = mutableMapOf<String, EventGroup>()
    for (dataProviderName in dataProviderNames) {
      val getDataProviderRequest = getDataProviderRequest { name = dataProviderName }
      val dataProvider: DataProvider =
        try {
          dataProvidersStub
            .withAuthenticationKey(apiAuthenticationKey)
            .getDataProvider(getDataProviderRequest)
        } catch (e: StatusException) {
          throw Exception("Unable to get DataProvider with name $dataProviderName", e)
        }

      // TODO(@roaminggypsy): Implement QA event group logic using simulatorEventGroupName
      val listEventGroupsRequest = listEventGroupsRequest {
        parent = dataProviderName
        filter =
          ListEventGroupsRequestKt.filter {
            measurementConsumers += measurementConsumerName
            dataProviders += dataProviderName
          }
      }

      val eventGroups: List<EventGroup> =
        try {
          eventGroupsStub
            .withAuthenticationKey(apiAuthenticationKey)
            .listEventGroups(listEventGroupsRequest)
            .eventGroupsList
            .toList()
        } catch (e: StatusException) {
          throw Exception(
            "Unable to get event groups associated with measurement consumer $measurementConsumerName and data provider $dataProviderName",
            e,
          )
        }

      if (eventGroups.size != 1) {
        throw IllegalStateException(
          "here should be exactly 1:1 mapping between a data provider and an event group, but data provider $dataProvider is related to ${eventGroups.size} event groups"
        )
      }

      dataProviderNameToEventGroup[dataProviderName] = eventGroups[0]
    }
    return dataProviderNameToEventGroup
  }

  /**
   * Check previous measurements and decide whether a new measurement needs to be created
   *
   * case 1: no previous measurements -> Yes. case 2a: previous measurements exist & no finished
   * measurements -> No. case 2a: previous measurements exist & the most recent created one is
   * finished long time ago -> Yes. case 2b: previous measurements exist & the most recent created
   * one is finished recently -> No.
   */
  private suspend fun shouldCreateNewMeasurement(): Boolean {
    val previousMeasurements: List<Measurement> =
      measurementsStub.listMeasurements(listMeasurementsRequest {}).measurementsList

    if (previousMeasurements.isEmpty()) {
      return true
    }

    val finishedMeasurements: List<Measurement> =
      measurementsStub
        .listMeasurements(
          listMeasurementsRequest {
            filter = ListMeasurementsRequestKt.filter { states += COMPLETED_MEASUREMENT_STATES }
          }
        )
        .measurementsList

    if (finishedMeasurements.isEmpty()) {
      return false
    }

    val oldFinishedMeasurements: List<Measurement> =
      measurementsStub
        .listMeasurements(
          listMeasurementsRequest {
            filter =
              ListMeasurementsRequestKt.filter {
                states += COMPLETED_MEASUREMENT_STATES
                updatedBefore = clock.instant().minus(durationBetweenMeasurement).toProtoTime()
              }
          }
        )
        .measurementsList

    return finishedMeasurements.size == oldFinishedMeasurements.size
  }

  private suspend fun getDataProviderEntry(
    dataProviderName: String,
    eventGroup: EventGroup,
    measurementConsumerSigningKey: SigningKeyHandle,
    packedMeasurementEncryptionPublicKey: Any,
  ): Measurement.DataProviderEntry {
    return dataProviderEntry {
      val requisitionSpec = requisitionSpec {
        events =
          RequisitionSpecKt.events {
            this.eventGroups += eventGroupEntry {
              eventGroupEntry {
                key = eventGroup.name
                value =
                  EventGroupEntryKt.value {
                    collectionInterval = interval {
                      startTime = clock.instant().minus(measurementLookbackDuration).toProtoTime()
                      endTime = clock.instant().plus(Duration.ofDays(1)).toProtoTime()
                    }
                  }
              }
            }
          }
        measurementPublicKey = packedMeasurementEncryptionPublicKey
        nonce = secureRandom.nextLong()
      }

      key = dataProviderName
      val dataProvider =
        try {
          dataProvidersStub
            .withAuthenticationKey(apiAuthenticationKey)
            .getDataProvider(getDataProviderRequest { name = dataProviderName })
        } catch (e: StatusException) {
          throw Exception(
            "Unable to get event groups associated with measurement consumer $measurementConsumerName and data provider $dataProviderName",
            e,
          )
        }

      value =
        MeasurementKt.DataProviderEntryKt.value {
          dataProviderCertificate = dataProvider.certificate
          dataProviderPublicKey = dataProvider.publicKey.message
          encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signRequisitionSpec(requisitionSpec, measurementConsumerSigningKey),
              dataProvider.publicKey.unpack(),
            )
          nonceHash = Hashing.hashSha256(requisitionSpec.nonce)
        }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val secureRandom = SecureRandom.getInstance("SHA1PRNG")

    private val COMPLETED_MEASUREMENT_STATES =
      listOf(Measurement.State.SUCCEEDED, Measurement.State.FAILED, Measurement.State.CANCELLED)
  }
}
