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
import org.mockito.kotlin.capture
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.Requisition.State
import org.wfanet.measurement.api.v2alpha.RequisitionKey
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.RefuseRequisitionRequest as InternalRefuseRequest
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.Requisition.DuchyValue
import org.wfanet.measurement.internal.kingdom.Requisition.Refusal as InternalRefusal
import org.wfanet.measurement.internal.kingdom.Requisition.State as InternalState
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest

private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val CREATE_TIME_B: Timestamp = Instant.ofEpochSecond(456).toProtoTime()

private const val MIN_LIMIT = 1
private const val DEFAULT_LIMIT = 50

private const val DUCHIES_MAP_KEY = "1"
private const val REQUISITION_NAME = "dataProviders/AAAAAAAAAHs/requisitions/AAAAAAAAAHs"
private const val MEASUREMENT_NAME = "measurementConsumers/AAAAAAAAAHs/measurements/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"

private val INTERNAL_REQUISITION: InternalRequisition =
  InternalRequisition.newBuilder()
    .apply {
      externalMeasurementConsumerId = 1
      externalMeasurementId = 2
      externalRequisitionId = 3
      externalComputationId = 4
      externalDataProviderId = 5
      externalDataProviderCertificateId = 6
      createTime = CREATE_TIME
      state = InternalState.FULFILLED
      externalFulfillingDuchyId = "9"
      putDuchies(
        DUCHIES_MAP_KEY,
        DuchyValue.newBuilder()
          .apply {
            externalDuchyCertificateId = 1
            liquidLegionsV2Builder.apply {
              elGamalPublicKey = CREATE_TIME.toByteString()
              elGamalPublicKeySignature = CREATE_TIME.toByteString()
            }
          }
          .build()
      )
      parentMeasurementBuilder.apply { apiVersion = Version.V2_ALPHA.string }
    }
    .build()

private val REQUISITION: Requisition = buildRequisition {
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
  measurementSpec {
    data = INTERNAL_REQUISITION.parentMeasurement.measurementSpec
    signature = INTERNAL_REQUISITION.parentMeasurement.measurementSpecSignature
  }
  dataProviderCertificate =
    DataProviderCertificateKey(
        externalIdToApiId(INTERNAL_REQUISITION.externalDataProviderId),
        externalIdToApiId(INTERNAL_REQUISITION.externalDataProviderCertificateId)
      )
      .toName()
  dataProviderPublicKey {
    data = INTERNAL_REQUISITION.details.dataProviderPublicKey
    signature = INTERNAL_REQUISITION.details.dataProviderPublicKeySignature
  }

  val entry = INTERNAL_REQUISITION.duchiesMap[DUCHIES_MAP_KEY]
  addAllDuchies(
    List(1) {
      buildDuchyEntry {
        key = DUCHIES_MAP_KEY
        value {
          duchyCertificate = externalIdToApiId(entry!!.externalDuchyCertificateId)
          buildLiquidLegionsV2 {
            elGamalPublicKey {
              data = entry.liquidLegionsV2.elGamalPublicKey
              signature = entry.liquidLegionsV2.elGamalPublicKeySignature
            }
          }
        }
      }
    }
  )

  state = State.FULFILLED
}

@RunWith(JUnit4::class)
class RequisitionServiceTest {
  private val internalRequisitionMock: RequisitionsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalRequisitionMock) }

  private lateinit var service: RequisitionsService

  @Before
  fun initService() {
    service = RequisitionsService(RequisitionsCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `listRequisitions without page token and without filter returns unfiltered results`() =
      runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION))

    val request = buildListRequisitionsRequest { pageSize = 2 }

    val result = service.listRequisitions(request)

    val expected =
      ListRequisitionsResponse.newBuilder()
        .apply {
          addRequisitions(REQUISITION)
          addRequisitions(REQUISITION)
          nextPageToken = CREATE_TIME.toByteArray().base64UrlEncode()
        }
        .build()

    val streamRequisitionRequest =
      captureFirst<StreamRequisitionsRequest> {
        verify(internalRequisitionMock).streamRequisitions(capture())
      }

    assertThat(streamRequisitionRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        buildStreamRequisitionsRequest {
          limit = 2
          filter = StreamRequisitionsRequest.Filter.getDefaultInstance()
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions with page token and filter uses filter with timestamp from page token`() =
      runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION.rebuild { createTime = CREATE_TIME_B }))

    val request = buildListRequisitionsRequest {
      pageSize = 2
      pageToken = CREATE_TIME.toByteArray().base64UrlEncode()
      filterBuilder.apply { addStates(State.UNFULFILLED) }
    }

    val result = service.listRequisitions(request)

    val expected =
      ListRequisitionsResponse.newBuilder()
        .apply {
          addRequisitions(REQUISITION)
          nextPageToken = CREATE_TIME_B.toByteArray().base64UrlEncode()
        }
        .build()

    val streamRequisitionRequest =
      captureFirst<StreamRequisitionsRequest> {
        verify(internalRequisitionMock).streamRequisitions(capture())
      }

    assertThat(streamRequisitionRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        buildStreamRequisitionsRequest {
          limit = 2
          filterBuilder.apply {
            createdAfter = CREATE_TIME
            addStates(InternalState.UNFULFILLED)
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

    val request = buildListRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      filterBuilder.apply { measurement = MEASUREMENT_NAME }
    }

    val result = service.listRequisitions(request)

    val expected =
      ListRequisitionsResponse.newBuilder()
        .apply {
          addRequisitions(REQUISITION)
          nextPageToken = CREATE_TIME.toByteArray().base64UrlEncode()
        }
        .build()

    val streamRequisitionRequest =
      captureFirst<StreamRequisitionsRequest> {
        verify(internalRequisitionMock).streamRequisitions(capture())
      }

    assertThat(streamRequisitionRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        buildStreamRequisitionsRequest {
          limit = DEFAULT_LIMIT
          filterBuilder.apply {
            val measurementKey: MeasurementKey = MeasurementKey.fromName(MEASUREMENT_NAME)!!
            externalMeasurementConsumerId = apiIdToExternalId(measurementKey.measurementConsumerId)
            val dataProviderKey: DataProviderKey = DataProviderKey.fromName(DATA_PROVIDER_NAME)!!
            externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions throws INVALID_ARGUMENT when parent is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.listRequisitions(buildListRequisitionsRequest { parent = "adsfasdf" })
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name invalid")
  }

  @Test
  fun `refuseRequisition throws INVALID_ARGUMENT when name is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.refuseRequisition(RefuseRequisitionRequest.getDefaultInstance()) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }

  @Test
  fun `refuseRequisition throws INVALID_ARGUMENT when refusal details are missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.refuseRequisition(
            RefuseRequisitionRequest.newBuilder().apply { name = REQUISITION_NAME }.build()
          )
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Refusal details must be present")
  }

  @Test
  fun `refuseRequisition with refusal returns the updated requisition`() = runBlocking {
    whenever(internalRequisitionMock.refuseRequisition(any()))
      .thenReturn(
        INTERNAL_REQUISITION.rebuild {
          state = InternalState.REFUSED
          detailsBuilder.apply {
            refusalBuilder.apply { justification = InternalRefusal.Justification.UNFULFILLABLE }
          }
        }
      )

    val request =
      RefuseRequisitionRequest.newBuilder()
        .apply {
          name = REQUISITION_NAME
          refusalBuilder.apply { justification = Refusal.Justification.UNFULFILLABLE }
        }
        .build()

    val result = service.refuseRequisition(request)

    val expected =
      REQUISITION
        .toBuilder()
        .apply {
          state = State.REFUSED
          refusalBuilder.apply { justification = Refusal.Justification.UNFULFILLABLE }
        }
        .build()

    verifyProtoArgument(internalRequisitionMock, RequisitionsCoroutineImplBase::refuseRequisition)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        InternalRefuseRequest.newBuilder()
          .apply {
            refusalBuilder.apply { justification = InternalRefusal.Justification.UNFULFILLABLE }
          }
          .build()
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }
}

internal inline fun Requisition.DuchyEntry.Builder.value(
  fill: (@Builder Requisition.DuchyEntry.Value.Builder).() -> Unit
) = valueBuilder.apply(fill)

internal inline fun InternalRequisition.rebuild(
  fill: (@Builder InternalRequisition.Builder).() -> Unit
) = toBuilder().apply(fill).build()

internal inline fun buildListRequisitionsRequest(
  fill: (@Builder ListRequisitionsRequest.Builder).() -> Unit
) = ListRequisitionsRequest.newBuilder().apply(fill).build()
