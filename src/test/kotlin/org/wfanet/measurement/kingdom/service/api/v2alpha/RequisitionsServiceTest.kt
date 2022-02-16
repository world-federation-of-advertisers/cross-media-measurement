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
import org.wfanet.measurement.api.v2.alpha.ListRequisitionsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.listRequisitionsPageToken
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
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
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ComputationParticipantKt.liquidLegionsV2Details
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

private val UPDATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private const val DEFAULT_LIMIT = 50

private const val WILDCARD_NAME = "dataProviders/-"

private const val DUCHIES_MAP_KEY = "1"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME_2 = "measurementConsumers/BBBBBBBBBHs"
private const val MEASUREMENT_NAME = "$MEASUREMENT_CONSUMER_NAME/measurements/AAAAAAAAAHs"

private val DATA_PROVIDER_NAME = makeDataProvider(123L)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(124L)

private val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/AAAAAAAAAHs"

private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"

private val EXTERNAL_REQUISITION_ID =
  apiIdToExternalId(RequisitionKey.fromName(REQUISITION_NAME)!!.requisitionId)
private val EXTERNAL_DATA_PROVIDER_ID =
  apiIdToExternalId(RequisitionKey.fromName(REQUISITION_NAME)!!.dataProviderId)
private val EXTERNAL_MEASUREMENT_ID =
  apiIdToExternalId(MeasurementKey.fromName(MEASUREMENT_NAME)!!.measurementId)
private val EXTERNAL_MEASUREMENT_CONSUMER_ID =
  apiIdToExternalId(
    MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!.measurementConsumerId
  )

private val VISIBLE_REQUISITION_STATES: Set<InternalRequisition.State> =
  setOf(
    InternalRequisition.State.UNFULFILLED,
    InternalRequisition.State.FULFILLED,
    InternalRequisition.State.REFUSED
  )

@RunWith(JUnit4::class)
class RequisitionsServiceTest {
  private val internalRequisitionMock: RequisitionsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { refuseRequisition(any()) }
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
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalRequisitionMock) }

  private lateinit var service: RequisitionsService

  @Before
  fun initService() {
    service = RequisitionsService(RequisitionsCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `listRequisitions with parent uses filter with parent`() {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION))

    val request = listRequisitionsRequest { parent = DATA_PROVIDER_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listRequisitions(request) }
      }

    val expected = listRequisitionsResponse {
      requisitions += REQUISITION
      requisitions += REQUISITION
    }

    val streamRequisitionRequest =
      captureFirst<StreamRequisitionsRequest> {
        verify(internalRequisitionMock).streamRequisitions(capture())
      }

    assertThat(streamRequisitionRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamRequisitionsRequest {
          limit = DEFAULT_LIMIT + 1
          filter =
            StreamRequisitionsRequestKt.filter {
              externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              states += VISIBLE_REQUISITION_STATES
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions with page token returns next page`() = runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION, INTERNAL_REQUISITION))

    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
      val requisitionPageToken = listRequisitionsPageToken {
        pageSize = 2
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        states += State.UNFULFILLED
        lastRequisition =
          previousPageEnd {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          }
      }
      pageToken = requisitionPageToken.toByteArray().base64UrlEncode()
      filter = filter { states += State.UNFULFILLED }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listRequisitions(request) }
      }

    val expected = listRequisitionsResponse {
      requisitions += REQUISITION
      requisitions += REQUISITION
      val requisitionPageToken = listRequisitionsPageToken {
        pageSize = 2
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        states += State.UNFULFILLED
        lastRequisition =
          previousPageEnd {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          }
      }
      nextPageToken = requisitionPageToken.toByteArray().base64UrlEncode()
    }

    val streamRequisitionRequest =
      captureFirst<StreamRequisitionsRequest> {
        verify(internalRequisitionMock).streamRequisitions(capture())
      }

    assertThat(streamRequisitionRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamRequisitionsRequest {
          limit = 3
          filter =
            StreamRequisitionsRequestKt.filter {
              externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              states += InternalState.UNFULFILLED
              externalDataProviderIdAfter = EXTERNAL_DATA_PROVIDER_ID
              externalRequisitionIdAfter = EXTERNAL_REQUISITION_ID
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions with more results remaining returns response with next page token`() =
      runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION))

    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 1
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listRequisitions(request) }
      }

    val expected = listRequisitionsResponse {
      requisitions += REQUISITION
      val requisitionPageToken = listRequisitionsPageToken {
        pageSize = 1
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        lastRequisition =
          previousPageEnd {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          }
      }
      nextPageToken = requisitionPageToken.toByteArray().base64UrlEncode()
    }

    val streamRequisitionsRequest =
      captureFirst<StreamRequisitionsRequest> {
        verify(internalRequisitionMock).streamRequisitions(capture())
      }

    assertThat(streamRequisitionsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamRequisitionsRequest {
          limit = 2
          filter =
            StreamRequisitionsRequestKt.filter {
              externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              states += VISIBLE_REQUISITION_STATES
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions with parent and filter containing measurement uses filter with both`() {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION))

    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter { measurement = MEASUREMENT_NAME }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listRequisitions(request) }
      }

    val expected = listRequisitionsResponse { requisitions += REQUISITION }

    val streamRequisitionsRequest =
      captureFirst<StreamRequisitionsRequest> {
        verify(internalRequisitionMock).streamRequisitions(capture())
      }

    assertThat(streamRequisitionsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamRequisitionsRequest {
          limit = DEFAULT_LIMIT + 1
          filter =
            StreamRequisitionsRequestKt.filter {
              externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
              externalMeasurementId = EXTERNAL_MEASUREMENT_ID
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              states += VISIBLE_REQUISITION_STATES
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions throws INVALID ARGUMENT when mc caller doesn't match mc in page token `() =
      runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION))

    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      val requisitionPageToken = listRequisitionsPageToken {
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        externalMeasurementConsumerId = 456
        externalMeasurementId = EXTERNAL_MEASUREMENT_ID
        lastRequisition =
          previousPageEnd {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          }
      }
      pageToken = requisitionPageToken.toByteArray().base64UrlEncode()
      filter = filter { measurement = MEASUREMENT_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listRequisitions throws INVALID ARGUMENT when measurement filter doesn't match `() =
      runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION))

    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      val requisitionPageToken = listRequisitionsPageToken {
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
        externalMeasurementId = 456
        lastRequisition =
          previousPageEnd {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          }
      }
      pageToken = requisitionPageToken.toByteArray().base64UrlEncode()
      filter = filter { measurement = MEASUREMENT_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listRequisitions throws INVALID ARGUMENT when edp caller doesn't match edp in token `() =
      runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION))

    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      val requisitionPageToken = listRequisitionsPageToken {
        externalDataProviderId = 456
        lastRequisition =
          previousPageEnd {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          }
      }
      pageToken = requisitionPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listRequisitions throws INVALID ARGUMENT when states don't match states in page token `() =
      runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION))

    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      val requisitionPageToken = listRequisitionsPageToken {
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        states += State.UNFULFILLED
        lastRequisition =
          previousPageEnd {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          }
      }
      pageToken = requisitionPageToken.toByteArray().base64UrlEncode()
      filter = filter { states += State.REFUSED }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listRequisitions throws PERMISSION_DENIED when mc caller doesn't match`() {
    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter { measurement = MEASUREMENT_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listRequisitions throws PERMISSION_DENIED when edp caller doesn't match`() {
    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter { measurement = MEASUREMENT_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listRequisitions throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter { measurement = MEASUREMENT_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listRequisitions throws UNAUTHENTICATED when no principal is not found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.listRequisitions(listRequisitionsRequest { parent = WILDCARD_NAME }) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listRequisitions throws INVALID_ARGUMENT when only wildcard parent`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.listRequisitions(listRequisitionsRequest { parent = WILDCARD_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listRequisitions throws INVALID_ARGUMENT when parent is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listRequisitions(listRequisitionsRequest { parent = "adsfasdf" }) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisition throws UNAUTHENTICATED when no principal is not found`() {
    val request = refuseRequisitionRequest { name = REQUISITION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.refuseRequisition(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `refuseRequisition throws PERMISSION_DENIED when edp caller doesn't match`() {
    val request = refuseRequisitionRequest { name = REQUISITION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.refuseRequisition(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `refuseRequisition throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = refuseRequisitionRequest { name = REQUISITION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.refuseRequisition(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `refuseRequisition throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.refuseRequisition(refuseRequisitionRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisition throws INVALID_ARGUMENT when refusal details are missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.refuseRequisition(refuseRequisitionRequest { name = REQUISITION_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
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

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.refuseRequisition(request) }
      }

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

  companion object {
    private val INTERNAL_REQUISITION: InternalRequisition = internalRequisition {
      externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
      externalMeasurementId = EXTERNAL_MEASUREMENT_ID
      externalRequisitionId = EXTERNAL_REQUISITION_ID
      externalComputationId = 4L
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
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
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
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
  }
}
