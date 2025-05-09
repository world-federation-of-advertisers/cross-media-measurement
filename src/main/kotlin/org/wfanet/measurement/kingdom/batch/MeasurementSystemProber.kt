/*
 * Copyright 2024 The Cross-Media Measurement Authors
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
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleGauge
import java.io.File
import java.security.SecureRandom
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.lastOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ListMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toInstant
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
  private val measurementUpdateLookbackDuration: Duration,
  private val measurementConsumersStub:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub,
  private val measurementsStub:
    org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub,
  private val dataProvidersStub: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val eventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  private val clock: Clock = Clock.systemUTC(),
  private val secureRandom: SecureRandom = SecureRandom(),
) {
  private val lastTerminalMeasurementTimeGauge: DoubleGauge =
    Instrumentation.meter
      .gaugeBuilder("${PROBER_NAMESPACE}.last_terminal_measurement.timestamp")
      .setUnit("s")
      .setDescription(
        "Unix epoch timestamp (in seconds) of the update time of the most recently issued and completed prober Measurement"
      )
      .build()
  private val lastTerminalRequisitionTimeGauge: DoubleGauge =
    Instrumentation.meter
      .gaugeBuilder("${PROBER_NAMESPACE}.last_terminal_requisition.timestamp")
      .setUnit("s")
      .setDescription(
        "Unix epoch timestamp (in seconds) of the update time of requisition associated with a particular EDP for the most recently issued Measurement"
      )
      .build()

  suspend fun run() {
    val lastUpdatedMeasurement = getLastUpdatedMeasurement()
    if (lastUpdatedMeasurement != null) {
      updateLastTerminalRequisitionGauge(lastUpdatedMeasurement)
      if (lastUpdatedMeasurement.state in COMPLETED_MEASUREMENT_STATES) {
        val attributes =
          Attributes.of(
            MEASUREMENT_SUCCESS_ATTRIBUTE_KEY,
            lastUpdatedMeasurement.state == Measurement.State.SUCCEEDED,
          )
        lastTerminalMeasurementTimeGauge.set(
          lastUpdatedMeasurement.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND,
          attributes,
        )
      }
    }
    if (shouldCreateNewMeasurement(lastUpdatedMeasurement)) {
      createMeasurement()
    }
  }

  private suspend fun createMeasurement() {
    val dataProviderNameToEventGroup: Map<String, EventGroup> = buildDataProviderNameToEventGroup()

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
            maximumFrequency = 10
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

  private suspend fun buildDataProviderNameToEventGroup(): Map<String, EventGroup> {
    val dataProviderNameToEventGroup = mutableMapOf<String, EventGroup>()
    for (dataProviderName in dataProviderNames) {
      val eventGroup: EventGroup =
        eventGroupsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .listResources(1) { pageToken: String, remaining ->
            val request = listEventGroupsRequest {
              parent = measurementConsumerName
              filter = ListEventGroupsRequestKt.filter { dataProviderIn += dataProviderName }
              this.pageToken = pageToken
              pageSize = remaining
            }
            val response =
              try {
                listEventGroups(request)
              } catch (e: StatusException) {
                throw Exception(
                  "Unable to get event groups associated with measurement consumer $measurementConsumerName and data provider $dataProviderName",
                  e,
                )
              }
            ResourceList(response.eventGroupsList, response.nextPageToken)
          }
          .map { it.single() }
          .single()

      dataProviderNameToEventGroup[dataProviderName] = eventGroup
    }
    return dataProviderNameToEventGroup
  }

  private fun shouldCreateNewMeasurement(lastUpdatedMeasurement: Measurement?): Boolean {
    if (lastUpdatedMeasurement == null) {
      return true
    }

    if (lastUpdatedMeasurement.state !in COMPLETED_MEASUREMENT_STATES) {
      return false
    }

    val updateInstant = lastUpdatedMeasurement.updateTime.toInstant()
    val nextMeasurementEarliestInstant = updateInstant.plus(durationBetweenMeasurement)
    return clock.instant() >= nextMeasurementEarliestInstant
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun getLastUpdatedMeasurement(): Measurement? {
    val measurements: Flow<ResourceList<Measurement, String>> =
      measurementsStub.withAuthenticationKey(apiAuthenticationKey).listResources(Int.MAX_VALUE) {
        pageToken: String,
        remaining ->
        val response: ListMeasurementsResponse =
          try {
            listMeasurements(
              listMeasurementsRequest {
                parent = measurementConsumerName
                this.pageToken = pageToken
                this.pageSize = remaining
                filter = filter {
                  updatedAfter =
                    clock.instant().minus(measurementUpdateLookbackDuration).toProtoTime()
                }
              }
            )
          } catch (e: StatusException) {
            throw Exception(
              "Unable to list measurements for measurement consumer $measurementConsumerName",
              e,
            )
          }
        ResourceList(response.measurementsList, response.nextPageToken)
      }

    return measurements.flattenConcat().lastOrNull()
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private fun getRequisitionsForMeasurement(measurementName: String): Flow<Requisition> {
    return requisitionsStub
      .withAuthenticationKey(apiAuthenticationKey)
      .listResources { pageToken: String ->
        val response: ListRequisitionsResponse =
          try {
            listRequisitions(
              listRequisitionsRequest {
                parent = measurementName
                this.pageToken = pageToken
              }
            )
          } catch (e: StatusException) {
            throw Exception("Unable to list requisitions for measurement $measurementName", e)
          }
        ResourceList(response.requisitionsList, response.nextPageToken)
      }
      .flattenConcat()
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

  private suspend fun updateLastTerminalRequisitionGauge(lastUpdatedMeasurement: Measurement) {
    val requisitions = getRequisitionsForMeasurement(lastUpdatedMeasurement.name)
    requisitions.collect { requisition ->
      val requisitionKey =
        requireNotNull(CanonicalRequisitionKey.fromName(requisition.name)) {
          "Requisition name ${requisition.name} is invalid"
        }
      val dataProviderName: String = requisitionKey.dataProviderId
      val attributes =
        Attributes.of(
          DATA_PROVIDER_ATTRIBUTE_KEY,
          dataProviderName,
          REQUISITION_SUCCESS_ATTRIBUTE_KEY,
          requisition.state == Requisition.State.FULFILLED,
        )
      lastTerminalRequisitionTimeGauge.set(
        requisition.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND,
        attributes,
      )
    }
  }

  companion object {
    private const val MILLISECONDS_PER_SECOND = 1000.0

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val COMPLETED_MEASUREMENT_STATES =
      listOf(Measurement.State.SUCCEEDED, Measurement.State.FAILED, Measurement.State.CANCELLED)

    private const val PROBER_NAMESPACE = "${Instrumentation.ROOT_NAMESPACE}.prober"
    private val DATA_PROVIDER_ATTRIBUTE_KEY =
      AttributeKey.stringKey("${Instrumentation.ROOT_NAMESPACE}.data_provider")
    private val REQUISITION_SUCCESS_ATTRIBUTE_KEY =
      AttributeKey.booleanKey("${Instrumentation.ROOT_NAMESPACE}.requisition.success")
    private val MEASUREMENT_SUCCESS_ATTRIBUTE_KEY =
      AttributeKey.booleanKey("${Instrumentation.ROOT_NAMESPACE}.measurement.success")
  }
}
