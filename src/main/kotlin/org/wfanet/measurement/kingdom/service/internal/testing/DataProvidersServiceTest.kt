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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import com.google.rpc.errorInfo
import com.google.type.copy
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt
import org.wfanet.measurement.internal.kingdom.batchGetDataProvidersRequest
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.certificateDetails
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.dataProviderCapabilities
import org.wfanet.measurement.internal.kingdom.dataProviderDetails
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest
import org.wfanet.measurement.internal.kingdom.modelLineKey
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.internal.kingdom.replaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.internal.kingdom.replaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

@RunWith(JUnit4::class)
abstract class DataProvidersServiceTest<T : DataProvidersCoroutineImplBase> {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHIES)

  private val recordingIdGenerator = RecordingIdGenerator()
  protected val idGenerator: IdGenerator
    get() = recordingIdGenerator

  private val clock = Clock.systemUTC()
  private val population = Population(clock, idGenerator)

  protected lateinit var services: Services<T>
    private set

  protected val dataProvidersService: T
    get() = services.dataProvidersService

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    services = newServices(idGenerator)
  }

  @Test
  fun `getDataProvider fails for missing DataProvider`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.getDataProvider(
          getDataProviderRequest { externalDataProviderId = 404L }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider")
  }

  @Test
  fun `createDataProvider fails for missing fields`() = runBlocking {
    val request =
      CREATE_DATA_PROVIDER_REQUEST.copy { details = details.copy { clearPublicKeySignature() } }
    val exception =
      assertFailsWith<StatusRuntimeException> { dataProvidersService.createDataProvider(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("Details field of DataProvider is missing fields.")
  }

  // TODO:Add test for availability outside of active range

  @Test
  fun `createDataProvider returns created DataProvider`() = runBlocking {
    val request = CREATE_DATA_PROVIDER_REQUEST

    val response: DataProvider = dataProvidersService.createDataProvider(request)

    assertThat(recordingIdGenerator.externalIds).hasSize(2)
    val remainingExternalIds = recordingIdGenerator.externalIds.toMutableSet()
    assertThat(response).ignoringFieldDescriptors(EXTERNAL_ID_FIELD_DESCRIPTORS).isEqualTo(request)
    val externalDataProviderId = ExternalId(response.externalDataProviderId)
    assertThat(externalDataProviderId).isIn(remainingExternalIds)
    remainingExternalIds.remove(externalDataProviderId)
    assertThat(ExternalId(response.certificate.externalDataProviderId))
      .isEqualTo(externalDataProviderId)
    assertThat(ExternalId(response.certificate.externalCertificateId)).isIn(remainingExternalIds)
  }

  @Test
  fun `createDataProvider returns created DataProvider with availability intervals`() =
    runBlocking {
      val modelLines: List<ModelLine> =
        (1..3).map {
          population.createModelLine(
            services.modelProvidersService,
            services.modelSuitesService,
            services.modelLinesService,
          )
        }
      val request =
        CREATE_DATA_PROVIDER_REQUEST.copy {
          modelLines.forEachIndexed { index, modelLine ->
            dataAvailabilityIntervals +=
              DataProviderKt.dataAvailabilityMapEntry {
                key = modelLineKey {
                  externalModelProviderId = modelLine.externalModelProviderId
                  externalModelSuiteId = modelLine.externalModelSuiteId
                  externalModelLineId = modelLine.externalModelLineId
                }
                value = interval {
                  startTime =
                    Timestamps.add(modelLine.activeStartTime, Durations.fromDays(index.toLong()))
                  endTime = Timestamps.add(modelLine.activeStartTime, Durations.fromDays(90L))
                }
              }
          }
        }

      val response: DataProvider = dataProvidersService.createDataProvider(request)

      assertThat(response.dataAvailabilityIntervalsList)
        .isEqualTo(request.dataAvailabilityIntervalsList)
      assertThat(response)
        .ignoringRepeatedFieldOrderOfFields(DataProvider.DATA_AVAILABILITY_INTERVALS_FIELD_NUMBER)
        .isEqualTo(
          dataProvidersService.getDataProvider(
            getDataProviderRequest { externalDataProviderId = response.externalDataProviderId }
          )
        )
    }

  @Test
  fun `createDataProvider succeeds when requiredExternalDuchyIds is empty`() = runBlocking {
    val request = CREATE_DATA_PROVIDER_REQUEST.copy { requiredExternalDuchyIds.clear() }

    val response: DataProvider = dataProvidersService.createDataProvider(request)

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
      .ignoringFieldDescriptors(EXTERNAL_ID_FIELD_DESCRIPTORS)
      .isEqualTo(request)
  }

  @Test
  fun `createDataProvider throws FAILED_PRECONDITION when ModelLine not active`() = runBlocking {
    val modelProvider = population.createModelProvider(services.modelProvidersService)
    val modelSuite = population.createModelSuite(services.modelSuitesService, modelProvider)
    val modelLine = population.createModelLine(services.modelLinesService, modelSuite)
    val request =
      CREATE_DATA_PROVIDER_REQUEST.copy {
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
            }
            value = interval {
              startTime = Timestamps.subtract(modelLine.activeStartTime, Durations.fromDays(1L))
              endTime = Timestamps.add(modelLine.activeStartTime, Durations.fromDays(30L))
            }
          }
      }

    val exception =
      assertFailsWith<StatusRuntimeException> { dataProvidersService.createDataProvider(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.MODEL_LINE_NOT_ACTIVE.name
          metadata["external_model_provider_id"] = modelLine.externalModelProviderId.toString()
          metadata["external_model_suite_id"] = modelLine.externalModelSuiteId.toString()
          metadata["external_model_line_id"] = modelLine.externalModelLineId.toString()
          metadata["active_start_time"] = modelLine.activeStartTime.toInstant().toString()
          metadata["active_end_time"] = Timestamps.MAX_VALUE.toInstant().toString()
        }
      )
  }

  @Test
  fun `getDataProvider succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(CREATE_DATA_PROVIDER_REQUEST)

    val response =
      dataProvidersService.getDataProvider(
        GetDataProviderRequest.newBuilder()
          .setExternalDataProviderId(dataProvider.externalDataProviderId)
          .build()
      )

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
      .isEqualTo(dataProvider)
  }

  @Test
  fun `batchGetDataProviders returns DataProviders in request order`() {
    val dataProviders = runBlocking {
      listOf(
        dataProvidersService.createDataProvider(CREATE_DATA_PROVIDER_REQUEST),
        dataProvidersService.createDataProvider(
          CREATE_DATA_PROVIDER_REQUEST.copy {
            certificate =
              certificate.copy {
                subjectKeyIdentifier = subjectKeyIdentifier.concat(ByteString.copyFromUtf8("2"))
              }
          }
        ),
        dataProvidersService.createDataProvider(
          CREATE_DATA_PROVIDER_REQUEST.copy {
            certificate =
              certificate.copy {
                subjectKeyIdentifier = subjectKeyIdentifier.concat(ByteString.copyFromUtf8("3"))
              }
          }
        ),
      )
    }
    val shuffledDataProviders = dataProviders.shuffled()
    val request = batchGetDataProvidersRequest {
      externalDataProviderIds += shuffledDataProviders.map { it.externalDataProviderId }
    }

    val response = runBlocking { dataProvidersService.batchGetDataProviders(request) }

    assertThat(response.dataProvidersList)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
      .containsExactlyElementsIn(shuffledDataProviders)
      .inOrder()
  }

  @Test
  fun `replaceDataProviderRequiredDuchies succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(CREATE_DATA_PROVIDER_REQUEST)
    val desiredDuchyList = listOf(Population.AGGREGATOR_DUCHY.externalDuchyId)

    val updatedDataProvider =
      dataProvidersService.replaceDataProviderRequiredDuchies(
        replaceDataProviderRequiredDuchiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          requiredExternalDuchyIds += desiredDuchyList
        }
      )

    // Ensure DataProvider with updated duchy list is returned from function.
    assertThat(updatedDataProvider.requiredExternalDuchyIdsList).isEqualTo(desiredDuchyList)
    // Ensure changes were persisted.
    assertThat(
        dataProvidersService.getDataProvider(
          getDataProviderRequest { externalDataProviderId = dataProvider.externalDataProviderId }
        )
      )
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
      .isEqualTo(updatedDataProvider)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws NOT_FOUND when edp not found`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.replaceDataProviderRequiredDuchies(
          replaceDataProviderRequiredDuchiesRequest {
            externalDataProviderId = 123
            requiredExternalDuchyIds += listOf(Population.AGGREGATOR_DUCHY.externalDuchyId)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws INVALID_ARGUMENT when no edp id`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.replaceDataProviderRequiredDuchies(
          replaceDataProviderRequiredDuchiesRequest {
            requiredExternalDuchyIds += listOf(Population.AGGREGATOR_DUCHY.externalDuchyId)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceDataAvailabilityIntervals updates DataProvider`() = runBlocking {
    val modelLines: List<ModelLine> =
      (1..3).map {
        population.createModelLine(
          services.modelProvidersService,
          services.modelSuitesService,
          services.modelLinesService,
        )
      }
    val dataProvider =
      dataProvidersService.createDataProvider(
        CREATE_DATA_PROVIDER_REQUEST.copy {
          modelLines.take(2).forEachIndexed { index, modelLine ->
            dataAvailabilityIntervals +=
              DataProviderKt.dataAvailabilityMapEntry {
                key = modelLineKey {
                  externalModelProviderId = modelLine.externalModelProviderId
                  externalModelSuiteId = modelLine.externalModelSuiteId
                  externalModelLineId = modelLine.externalModelLineId
                }
                value = interval {
                  startTime =
                    Timestamps.add(modelLine.activeStartTime, Durations.fromDays(index.toLong()))
                  endTime = Timestamps.add(modelLine.activeStartTime, Durations.fromDays(90L))
                }
              }
          }
        }
      )
    val request = replaceDataAvailabilityIntervalsRequest {
      externalDataProviderId = dataProvider.externalDataProviderId
      // Keep/update one entry.
      dataAvailabilityIntervals +=
        dataProvider.dataAvailabilityIntervalsList.first().copy {
          value = value.copy { endTime = Timestamps.add(endTime, Durations.fromDays(1L)) }
        }
      // Add a new entry.
      dataAvailabilityIntervals +=
        DataProviderKt.dataAvailabilityMapEntry {
          val modelLine = modelLines.last()
          key = modelLineKey {
            externalModelProviderId = modelLine.externalModelProviderId
            externalModelSuiteId = modelLine.externalModelSuiteId
            externalModelLineId = modelLine.externalModelLineId
          }
          value = interval {
            startTime = Timestamps.add(modelLine.activeStartTime, Durations.fromDays(10L))
            endTime = Timestamps.add(modelLine.activeStartTime, Durations.fromDays(100L))
          }
        }
    }

    val response: DataProvider = dataProvidersService.replaceDataAvailabilityIntervals(request)

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFields(DataProvider.DATA_AVAILABILITY_INTERVALS_FIELD_NUMBER)
      .isEqualTo(
        dataProvider.copy {
          dataAvailabilityIntervals.clear()
          dataAvailabilityIntervals += request.dataAvailabilityIntervalsList
        }
      )
    assertThat(response)
      .ignoringRepeatedFieldOrderOfFields(DataProvider.DATA_AVAILABILITY_INTERVALS_FIELD_NUMBER)
      .isEqualTo(
        dataProvidersService.getDataProvider(
          getDataProviderRequest { externalDataProviderId = dataProvider.externalDataProviderId }
        )
      )
  }

  @Test
  fun `replaceDataAvailabilityIntervals throws FAILED_PRECONDITION when ModelLine not found`() =
    runBlocking {
      val now: Instant = clock.instant()
      val modelLine: ModelLine =
        population.createModelLine(
          services.modelProvidersService,
          services.modelSuitesService,
          services.modelLinesService,
        )
      val dataProvider: DataProvider =
        dataProvidersService.createDataProvider(CREATE_DATA_PROVIDER_REQUEST)
      val request = replaceDataAvailabilityIntervalsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = 404L
            }
            value = interval {
              startTime = now.minus(90L, ChronoUnit.DAYS).toProtoTime()
              endTime = now.minus(3L, ChronoUnit.DAYS).toProtoTime()
            }
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          dataProvidersService.replaceDataAvailabilityIntervals(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.MODEL_LINE_NOT_FOUND.name
            metadata["external_model_provider_id"] = modelLine.externalModelProviderId.toString()
            metadata["external_model_suite_id"] = modelLine.externalModelSuiteId.toString()
            metadata["external_model_line_id"] = "404"
          }
        )
    }

  @Test
  fun `replaceDataAvailabilityIntervals throws FAILED_PRECONDITION when ModelLine not active`() =
    runBlocking {
      val now = clock.instant()
      val activeInterval = interval {
        startTime = now.plus(1L, ChronoUnit.DAYS).toProtoTime()
        endTime = now.plus(91L, ChronoUnit.DAYS).toProtoTime()
      }
      val modelProvider = population.createModelProvider(services.modelProvidersService)
      val modelSuite = population.createModelSuite(services.modelSuitesService, modelProvider)
      val modelLine =
        population.createModelLine(
          services.modelLinesService,
          modelSuite,
          activeInterval = activeInterval,
        )
      val dataProvider = dataProvidersService.createDataProvider(CREATE_DATA_PROVIDER_REQUEST)
      val request = replaceDataAvailabilityIntervalsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
            }
            value = interval {
              startTime = Timestamps.add(activeInterval.startTime, Durations.fromDays(1))
              endTime = Timestamps.add(activeInterval.endTime, Durations.fromDays(1))
            }
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          dataProvidersService.replaceDataAvailabilityIntervals(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.MODEL_LINE_NOT_ACTIVE.name
            metadata["external_model_provider_id"] = modelLine.externalModelProviderId.toString()
            metadata["external_model_suite_id"] = modelLine.externalModelSuiteId.toString()
            metadata["external_model_line_id"] = modelLine.externalModelLineId.toString()
            metadata["active_start_time"] = modelLine.activeStartTime.toInstant().toString()
            metadata["active_end_time"] = modelLine.activeEndTime.toInstant().toString()
          }
        )
    }

  @Test
  fun `replaceDataAvailabilityIntervals throws INVALID_ARGUMENT when end time not set`() =
    runBlocking {
      val now: Instant = clock.instant()
      val modelLine: ModelLine =
        population.createModelLine(
          services.modelProvidersService,
          services.modelSuitesService,
          services.modelLinesService,
        )
      val dataProvider: DataProvider =
        dataProvidersService.createDataProvider(CREATE_DATA_PROVIDER_REQUEST)
      val request = replaceDataAvailabilityIntervalsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
            }
            value = interval { startTime = now.minus(90L, ChronoUnit.DAYS).toProtoTime() }
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          dataProvidersService.replaceDataAvailabilityIntervals(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.REQUIRED_FIELD_NOT_SET.name
            metadata["field_name"] = "data_availability_intervals[0].value.end_time"
          }
        )
    }

  @Test
  fun `replaceDataAvailabilityInterval modifies DataProvider`() = runBlocking {
    val dataAvailabilityInterval = interval {
      startTime = timestamp { seconds = 200 }
      endTime = timestamp { seconds = 300 }
    }
    val dataProvider =
      dataProvidersService.createDataProvider(
        dataProvider {
          certificate {
            notValidBefore = timestamp { seconds = 12345 }
            notValidAfter = timestamp { seconds = 23456 }
            details = certificateDetails { x509Der = CERTIFICATE_DER }
          }
          details = dataProviderDetails {
            apiVersion = "v2alpha"
            publicKey = PUBLIC_KEY
            publicKeySignature = PUBLIC_KEY_SIGNATURE
            publicKeySignatureAlgorithmOid = PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
            this.dataAvailabilityInterval = dataAvailabilityInterval
          }
          requiredExternalDuchyIds += DUCHIES.map { it.externalDuchyId }
        }
      )

    val updatedDataProvider =
      dataProvidersService.replaceDataAvailabilityInterval(
        replaceDataAvailabilityIntervalRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          this.dataAvailabilityInterval = dataAvailabilityInterval
        }
      )

    assertThat(updatedDataProvider.details.dataAvailabilityInterval)
      .isEqualTo(dataAvailabilityInterval)
    // Ensure changes were persisted.
    assertThat(
        dataProvidersService.getDataProvider(
          getDataProviderRequest { externalDataProviderId = dataProvider.externalDataProviderId }
        )
      )
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
      .isEqualTo(updatedDataProvider)
  }

  @Test
  fun `replaceDataAvailabilityInterval throws NOT_FOUND when edp not found`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.replaceDataAvailabilityInterval(
          replaceDataAvailabilityIntervalRequest {
            externalDataProviderId = 123
            dataAvailabilityInterval = interval {
              startTime = timestamp { seconds = 200 }
              endTime = timestamp { seconds = 300 }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `replaceDataAvailabilityInterval throws NOT_FOUND when no edp id`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.replaceDataAvailabilityInterval(
          replaceDataAvailabilityIntervalRequest {
            dataAvailabilityInterval = interval {
              startTime = timestamp { seconds = 200 }
              endTime = timestamp { seconds = 300 }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceDataProviderCapabilites updates DataProvider`() = runBlocking {
    val dataProvider: DataProvider =
      dataProvidersService.createDataProvider(CREATE_DATA_PROVIDER_REQUEST)
    val capabilities = dataProviderCapabilities {
      honestMajorityShareShuffleSupported = true
      trusTeeSupported = true
    }

    val response: DataProvider =
      dataProvidersService.replaceDataProviderCapabilities(
        replaceDataProviderCapabilitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          this.capabilities = capabilities
        }
      )

    assertThat(response.details.capabilities).isEqualTo(capabilities)
    // Ensure changes were persisted.
    assertThat(
        dataProvidersService.getDataProvider(
          getDataProviderRequest { externalDataProviderId = dataProvider.externalDataProviderId }
        )
      )
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
      .isEqualTo(response)
  }

  /** Random [IdGenerator] which records generated IDs. */
  private class RecordingIdGenerator : IdGenerator {
    private val delegate = RandomIdGenerator()
    private val mutableInternalIds: MutableList<InternalId> = mutableListOf()
    private val mutableExternalIds: MutableList<ExternalId> = mutableListOf()

    val internalIds: List<InternalId>
      get() = mutableInternalIds

    val externalIds: List<ExternalId>
      get() = mutableExternalIds

    override fun generateExternalId(): ExternalId {
      return delegate.generateExternalId().also { mutableExternalIds.add(it) }
    }

    override fun generateInternalId(): InternalId {
      return delegate.generateInternalId().also { mutableInternalIds.add(it) }
    }
  }

  protected data class Services<T>(
    val dataProvidersService: T,
    val modelProvidersService: ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase,
    val modelSuitesService: ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase,
    val modelLinesService: ModelLinesGrpcKt.ModelLinesCoroutineImplBase,
  )

  companion object {
    private val EXTERNAL_ID_FIELD_DESCRIPTORS =
      listOf(
        DataProvider.getDescriptor()
          .findFieldByNumber(DataProvider.EXTERNAL_DATA_PROVIDER_ID_FIELD_NUMBER),
        Certificate.getDescriptor()
          .findFieldByNumber(Certificate.EXTERNAL_DATA_PROVIDER_ID_FIELD_NUMBER),
        Certificate.getDescriptor()
          .findFieldByNumber(Certificate.EXTERNAL_CERTIFICATE_ID_FIELD_NUMBER),
      )
    private val UNORDERED_FIELD_DESCRIPTORS =
      listOf(
        DataProvider.getDescriptor()
          .findFieldByNumber(DataProvider.REQUIRED_EXTERNAL_DUCHY_IDS_FIELD_NUMBER)
      )

    private const val PUBLIC_KEY_SIGNATURE_ALGORITHM_OID = "2.9999"
    private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
    private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
    private val CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")
    private val CERTIFICATE_SKID = ByteString.copyFromUtf8("Certificate SKID")
    private val CREATE_DATA_PROVIDER_REQUEST = dataProvider {
      certificate = certificate {
        notValidBefore = timestamp { seconds = 12345 }
        notValidAfter = timestamp { seconds = 23456 }
        subjectKeyIdentifier = CERTIFICATE_SKID
        details = certificateDetails { x509Der = CERTIFICATE_DER }
      }
      details = dataProviderDetails {
        apiVersion = "v2alpha"
        publicKey = PUBLIC_KEY
        publicKeySignature = PUBLIC_KEY_SIGNATURE
        publicKeySignatureAlgorithmOid = PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
      }
      requiredExternalDuchyIds += DUCHIES.map { it.externalDuchyId }
    }
  }
}
