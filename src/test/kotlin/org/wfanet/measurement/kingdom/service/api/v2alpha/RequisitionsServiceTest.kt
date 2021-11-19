// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.Requisition.State
import org.wfanet.measurement.api.v2alpha.RequisitionKey
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ComputationParticipantKt.liquidLegionsV2Details
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.ProtocolConfig as InternalProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.Requisition.Refusal as InternalRefusal
import org.wfanet.measurement.internal.kingdom.Requisition.State as InternalState
import org.wfanet.measurement.internal.kingdom.RequisitionKt as InternalRequisitionKt
import org.wfanet.measurement.internal.kingdom.RequisitionKt.details
import org.wfanet.measurement.internal.kingdom.RequisitionKt.duchyValue
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.protocolConfig as internalProtocolConfig
import org.wfanet.measurement.internal.kingdom.refuseRequisitionRequest as internalRefuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.requisition as internalRequisition
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider

private val UPDATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val UPDATE_TIME_B: Timestamp = Instant.ofEpochSecond(456).toProtoTime()

private const val DEFAULT_LIMIT = 50

private const val WILDCARD_NAME = "dataProviders/-"

private const val DUCHIES_MAP_KEY = "1"
private const val REQUISITION_NAME = "dataProviders/AAAAAAAAAHs/requisitions/AAAAAAAAAHs"
private const val MEASUREMENT_NAME = "measurementConsumers/AAAAAAAAAHs/measurements/AAAAAAAAAHs"

private val DATA_PROVIDER_NAME = makeDataProvider(12345L)

private val VISIBLE_MEASUREMENT_STATES: Set<InternalMeasurement.State> =
  setOf(
    InternalMeasurement.State.PENDING_REQUISITION_FULFILLMENT,
    InternalMeasurement.State.PENDING_PARTICIPANT_CONFIRMATION,
    InternalMeasurement.State.PENDING_COMPUTATION,
    InternalMeasurement.State.SUCCEEDED,
    InternalMeasurement.State.FAILED,
    InternalMeasurement.State.CANCELLED
  )

private val INTERNAL_REQUISITION: InternalRequisition = internalRequisition {
  externalMeasurementConsumerId = 1L
  externalMeasurementId = 2L
  externalRequisitionId = 3L
  externalComputationId = 4L
  externalDataProviderId = 5L
  updateTime = UPDATE_TIME
  state = InternalState.FULFILLED
  externalFulfillingDuchyId = "9"
  duchies[DUCHIES_MAP_KEY] =
    duchyValue {
      externalDuchyCertificateId = 6L
      liquidLegionsV2 =
        liquidLegionsV2Details {
          elGamalPublicKey = UPDATE_TIME.toByteString()
          elGamalPublicKeySignature = UPDATE_TIME.toByteString()
        }
    }
  dataProviderCertificate =
    internalCertificate {
      externalDataProviderId = this@internalRequisition.externalDataProviderId
      externalCertificateId = 7L
    }
  parentMeasurement =
    parentMeasurement {
      apiVersion = Version.V2_ALPHA.string
      externalMeasurementConsumerCertificateId = 8L
      protocolConfig =
        internalProtocolConfig {
          externalProtocolConfigId = "llv2"
          liquidLegionsV2 = InternalProtocolConfig.LiquidLegionsV2.getDefaultInstance()
        }
    }
}

private val REQUISITION: Requisition = requisition {
  name =
    RequisitionKey(
        externalIdToApiId(INTERNAL_REQUISITION.externalDataProviderId),
        externalIdToApiId(INTERNAL_REQUISITION.externalRequisitionId)
      )
      .toName()

  measurement =
    MeasurementKey(
        externalIdToApiId(INTERNAL_REQUISITION.externalMeasurementConsumerId),
        externalIdToApiId(INTERNAL_REQUISITION.externalMeasurementId)
      )
      .toName()
  measurementConsumerCertificate =
    MeasurementConsumerCertificateKey(
        externalIdToApiId(INTERNAL_REQUISITION.externalMeasurementConsumerId),
        externalIdToApiId(
          INTERNAL_REQUISITION.parentMeasurement.externalMeasurementConsumerCertificateId
        )
      )
      .toName()
  measurementSpec =
    signedData {
      data = INTERNAL_REQUISITION.parentMeasurement.measurementSpec
      signature = INTERNAL_REQUISITION.parentMeasurement.measurementSpecSignature
    }
  protocolConfig =
    protocolConfig {
      name = "protocolConfigs/llv2"
      liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
    }
  dataProviderCertificate =
    DataProviderCertificateKey(
        externalIdToApiId(INTERNAL_REQUISITION.externalDataProviderId),
        externalIdToApiId(INTERNAL_REQUISITION.dataProviderCertificate.externalCertificateId)
      )
      .toName()
  dataProviderPublicKey =
    signedData {
      data = INTERNAL_REQUISITION.details.dataProviderPublicKey
      signature = INTERNAL_REQUISITION.details.dataProviderPublicKeySignature
    }

  val entry = INTERNAL_REQUISITION.duchiesMap[DUCHIES_MAP_KEY]!!

  duchies +=
    duchyEntry {
      key = DUCHIES_MAP_KEY
      value =
        value {
          duchyCertificate = externalIdToApiId(entry.externalDuchyCertificateId)
          liquidLegionsV2 =
            liquidLegionsV2 {
              elGamalPublicKey =
                signedData {
                  data = entry.liquidLegionsV2.elGamalPublicKey
                  signature = entry.liquidLegionsV2.elGamalPublicKeySignature
                }
            }
        }
    }

  state = State.FULFILLED
}

@RunWith(JUnit4::class)
class RequisitionsServiceTest {
  private val internalRequisitionMock: RequisitionsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalRequisitionMock) }

  private lateinit var service: RequisitionsService

  @Before
  fun initService() {
    service = RequisitionsService(RequisitionsCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `listRequisitions with parent uses filter with parent`() = runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION))

    val request = listRequisitionsRequest { parent = DATA_PROVIDER_NAME }

    val result = service.listRequisitions(request)

    val expected = listRequisitionsResponse {
      requisitions += REQUISITION
      requisitions += REQUISITION
      nextPageToken = UPDATE_TIME.toByteArray().base64UrlEncode()
    }

    val streamRequisitionRequest =
      captureFirst<StreamRequisitionsRequest> {
        verify(internalRequisitionMock).streamRequisitions(capture())
      }

    assertThat(streamRequisitionRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamRequisitionsRequest {
          limit = DEFAULT_LIMIT
          filter =
            StreamRequisitionsRequestKt.filter {
              externalDataProviderId =
                apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)
              measurementStates += VISIBLE_MEASUREMENT_STATES
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions with page token and filter uses filter with timestamp from page token`() =
      runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION.copy { updateTime = UPDATE_TIME_B }))

    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
      pageToken = UPDATE_TIME.toByteArray().base64UrlEncode()
      filter = filter { states += State.UNFULFILLED }
    }

    val result = service.listRequisitions(request)

    val expected = listRequisitionsResponse {
      requisitions += REQUISITION
      nextPageToken = UPDATE_TIME_B.toByteArray().base64UrlEncode()
    }

    val streamRequisitionRequest =
      captureFirst<StreamRequisitionsRequest> {
        verify(internalRequisitionMock).streamRequisitions(capture())
      }

    assertThat(streamRequisitionRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamRequisitionsRequest {
          limit = 2
          filter =
            StreamRequisitionsRequestKt.filter {
              externalDataProviderId =
                apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)
              updatedAfter = UPDATE_TIME
              states += InternalState.UNFULFILLED
              measurementStates += VISIBLE_MEASUREMENT_STATES
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions with parent and filter containing measurement uses filter with both`() =
      runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION))

    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter { measurement = MEASUREMENT_NAME }
    }

    val result = service.listRequisitions(request)

    val expected = listRequisitionsResponse {
      requisitions += REQUISITION
      nextPageToken = UPDATE_TIME.toByteArray().base64UrlEncode()
    }

    val streamRequisitionRequest =
      captureFirst<StreamRequisitionsRequest> {
        verify(internalRequisitionMock).streamRequisitions(capture())
      }

    assertThat(streamRequisitionRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamRequisitionsRequest {
          limit = DEFAULT_LIMIT
          filter =
            StreamRequisitionsRequestKt.filter {
              val measurementKey = MeasurementKey.fromName(MEASUREMENT_NAME)!!
              externalMeasurementConsumerId =
                apiIdToExternalId(measurementKey.measurementConsumerId)
              externalMeasurementId = apiIdToExternalId(measurementKey.measurementId)
              externalDataProviderId =
                apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)
              measurementStates += VISIBLE_MEASUREMENT_STATES
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions throws INVALID_ARGUMENT when only wildcard parent`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.listRequisitions(listRequisitionsRequest { parent = WILDCARD_NAME }) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("Either parent data provider or measurement filter must be provided")
  }

  @Test
  fun `listRequisitions throws INVALID_ARGUMENT when parent is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.listRequisitions(listRequisitionsRequest { parent = "adsfasdf" }) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Parent is either unspecified or invalid")
  }

  @Test
  fun `listRequisitions throws INVALID_ARGUMENT when state in filter is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.listRequisitions(
            listRequisitionsRequest {
              parent = DATA_PROVIDER_NAME
              filter = filter { states += State.STATE_UNSPECIFIED }
            }
          )
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("State is invalid")
  }

  @Test
  fun `refuseRequisition throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.refuseRequisition(RefuseRequisitionRequest.getDefaultInstance()) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }

  @Test
  fun `refuseRequisition throws INVALID_ARGUMENT when refusal details are missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.refuseRequisition(refuseRequisitionRequest { name = REQUISITION_NAME })
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Refusal details must be present")
  }

  @Test
  fun `refuseRequisition with refusal returns the updated requisition`() = runBlocking {
    whenever(internalRequisitionMock.refuseRequisition(any()))
      .thenReturn(
        INTERNAL_REQUISITION.copy {
          state = InternalState.REFUSED
          details =
            details {
              refusal =
                InternalRequisitionKt.refusal {
                  justification = InternalRefusal.Justification.UNFULFILLABLE
                }
            }
        }
      )

    val request = refuseRequisitionRequest {
      name = REQUISITION_NAME
      refusal = refusal { justification = Refusal.Justification.UNFULFILLABLE }
    }

    val result = service.refuseRequisition(request)

    val expected =
      REQUISITION.copy {
        state = State.REFUSED
        refusal = refusal { justification = Refusal.Justification.UNFULFILLABLE }
      }

    verifyProtoArgument(internalRequisitionMock, RequisitionsCoroutineImplBase::refuseRequisition)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        internalRefuseRequisitionRequest {
          refusal =
            InternalRequisitionKt.refusal {
              justification = InternalRefusal.Justification.UNFULFILLABLE
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }
}
