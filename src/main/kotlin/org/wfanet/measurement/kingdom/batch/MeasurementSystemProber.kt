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
import io.grpc.Status
import io.grpc.StatusException
import java.io.File
import java.security.SecureRandom
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.*
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.*
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest

private val COMPLETED_MEASUREMENT_STATES =
  listOf(
    org.wfanet.measurement.internal.kingdom.Measurement.State.SUCCEEDED,
    org.wfanet.measurement.internal.kingdom.Measurement.State.FAILED,
    org.wfanet.measurement.internal.kingdom.Measurement.State.CANCELLED,
  )

private val secureRandom = SecureRandom.getInstance("SHA1PRNG")

class MeasurementSystemProber(
  private val measurementConsumerName: String,
  private val dataProviderNames: List<String>,
  private val apiAuthenticationKey: String,
  private val privateKeyDerFile: File,
  private val measurementLookbackDuration: Duration,
  private val durationBetweenMeasurement: Duration,
  private val measurementConsumersService:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub,
  private val publicMeasurementsService:
    org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub,
  private val internalMeasurementsService: MeasurementsGrpcKt.MeasurementsCoroutineStub,
  private val dataProvidersService: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val eventGroupsService: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val clock: Clock = Clock.systemUTC(),
) {
  fun run() {
    runBlocking {
      if (!shouldCreateNewMeasurement()) {
        return@runBlocking
      }
    }

    val dataProviderNameToEventGroup = mutableMapOf<String, EventGroup>()
    for (dataProviderName in dataProviderNames) {
      val getDataProviderRequest = getDataProviderRequest { name = dataProviderName }
      val dataProvider = runBlocking {
        try {
          dataProvidersService
            .withAuthenticationKey(apiAuthenticationKey)
            .getDataProvider(getDataProviderRequest)
        } catch (e: StatusException) {
          throw when (e.status.code) {
              Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
              Status.Code.CANCELLED -> Status.CANCELLED
              Status.Code.NOT_FOUND -> Status.NOT_FOUND
              else -> Status.UNKNOWN
            }
            .withCause(e)
            .withDescription("Unable to get DataProvider with name $dataProviderName.")
            .asRuntimeException()
        }
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

      val eventGroups =
        runBlocking {
            eventGroupsService
              .withAuthenticationKey(apiAuthenticationKey)
              .listEventGroups(listEventGroupsRequest)
          }
          .eventGroupsList
          .toList()

      if (eventGroups.size != 1) {
        logger.warning(
          "There should be exactly 1:1 mapping between a data provider and an event group, but data provider $dataProvider is related to ${eventGroups.size} event groups"
        )
        return
      }

      dataProviderNameToEventGroup[dataProviderName] = eventGroups[0]
    }

    val measurementConsumer = runBlocking {
      measurementConsumersService
        .withAuthenticationKey(apiAuthenticationKey)
        .getMeasurementConsumer(getMeasurementConsumerRequest { name = measurementConsumerName })
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

    val response = runBlocking {
      publicMeasurementsService
        .withAuthenticationKey(apiAuthenticationKey)
        .createMeasurement(
          createMeasurementRequest {
            this.parent = parent
            this.measurement = measurement
          }
        )
    }
    logger.info("A new prober measurement is created: ${response.name}")

    // TODO(@roaminggypsy): Report measurement result with OpenTelemetry
  }

  /**
   * Check previous measurements and decide whether a new measurement needs to be created
   *
   * case 1: no previous measurements -> YES case 2: previous measurements exist a) at least 1
   * finished a long time ago -> Yes b) finished ones are finished too soon -> No c) no finished
   * measurements -> no
   */
  private suspend fun shouldCreateNewMeasurement(): Boolean {
    val previousMeasurements: List<Measurement> =
      internalMeasurementsService.streamMeasurements(streamMeasurementsRequest {}).toList()

    if (previousMeasurements.isEmpty()) {
      return true
    }

    val oldFinishedMeasurements: List<Measurement> =
      internalMeasurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter =
              StreamMeasurementsRequestKt.filter {
                states += COMPLETED_MEASUREMENT_STATES
                updatedBefore = clock.instant().minus(durationBetweenMeasurement).toProtoTime()
              }
          }
        )
        .toList()

    return oldFinishedMeasurements.isNotEmpty()
  }

  private fun getDataProviderEntry(
    dataProviderName: String,
    eventGroup: EventGroup,
    measurementConsumerSigningKey: SigningKeyHandle,
    packedMeasurementEncryptionPublicKey: Any,
  ): org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry {
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
      val dataProvider = runBlocking {
        dataProvidersService
          .withAuthenticationKey(apiAuthenticationKey)
          .getDataProvider(getDataProviderRequest { name = dataProviderName })
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
  }
}
