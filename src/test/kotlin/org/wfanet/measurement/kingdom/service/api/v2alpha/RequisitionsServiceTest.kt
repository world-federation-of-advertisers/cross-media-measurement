/*
 * Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.kotlin.toByteStringUtf8
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
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.ListRequisitionsPageToken
import org.wfanet.measurement.api.v2alpha.ListRequisitionsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.Requisition.State
import org.wfanet.measurement.api.v2alpha.RequisitionKey
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsPageToken
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ComputationParticipantKt.liquidLegionsV2Details
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt.directRequisitionParams
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.ProtocolConfig as InternalProtocolConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt as InternalProtocolConfigKt
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
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest as internalFulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.protocolConfig as internalProtocolConfig
import org.wfanet.measurement.internal.kingdom.refuseRequisitionRequest as internalRefuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.requisition as internalRequisition
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest

private val UPDATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private const val DEFAULT_LIMIT = 50

private const val WILDCARD_NAME = "dataProviders/-"

private const val DUCHY_ID = "worker1"
private const val DUCHY_CERTIFICATE_NAME = "duchies/$DUCHY_ID/certificates/AAAAAAAAAAY"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME_2 = "measurementConsumers/BBBBBBBBBHs"
private const val MEASUREMENT_NAME = "$MEASUREMENT_CONSUMER_NAME/measurements/AAAAAAAAAHs"

private val DATA_PROVIDER_NAME = makeDataProvider(123L)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(124L)

private val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/AAAAAAAAAHs"
private const val INVALID_REQUISITION_NAME = "requisitions/AAAAAAAAAHs"

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

private val REQUISITION_ENCRYPTED_DATA = "foo".toByteStringUtf8()
private const val NONCE = -7452112597811743614 // Hex: 9894C7134537B482

private val VISIBLE_REQUISITION_STATES: Set<InternalRequisition.State> =
  setOf(
    InternalRequisition.State.UNFULFILLED,
    InternalRequisition.State.FULFILLED,
    InternalRequisition.State.REFUSED
  )

@RunWith(JUnit4::class)
class RequisitionsServiceTest {
  private val internalRequisitionMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { refuseRequisition(any()) }
      .thenReturn(
        INTERNAL_REQUISITION.copy {
          state = InternalState.REFUSED
          details = details {
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
  fun `listRequisitions requests internal Requisitions with Measurement parent`() {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION))

    val request = listRequisitionsRequest { parent = MEASUREMENT_NAME }

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
              externalMeasurementId = EXTERNAL_MEASUREMENT_ID
              states += VISIBLE_REQUISITION_STATES
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions requests internal Requisitions with direct protocol`() {
    val internalRequisition =
      INTERNAL_REQUISITION.copy {
        parentMeasurement =
          parentMeasurement.copy {
            protocolConfig = internalProtocolConfig {
              externalProtocolConfigId = "direct"
              direct = INTERNAL_DIRECT_RF_PROTOCOL_CONFIG
            }
          }
      }

    val requisition =
      REQUISITION.copy {
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols += ProtocolConfigKt.protocol { direct = DIRECT_RF_PROTOCOL_CONFIG }
          }
      }

    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(internalRequisition, internalRequisition))

    val request = listRequisitionsRequest { parent = MEASUREMENT_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listRequisitions(request) }
      }

    val expected = listRequisitionsResponse {
      requisitions += requisition
      requisitions += requisition
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
              externalMeasurementId = EXTERNAL_MEASUREMENT_ID
              states += VISIBLE_REQUISITION_STATES
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listRequisitions with page token returns next page`() {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION, INTERNAL_REQUISITION))
      .thenReturn(flowOf(INTERNAL_REQUISITION))
    val initialRequest = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
      filter = filter { states += State.UNFULFILLED }
    }
    val initialResponse =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listRequisitions(initialRequest) }
      }
    val request = initialRequest.copy { pageToken = initialResponse.nextPageToken }

    val response =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listRequisitions(request) }
      }

    assertThat(response).isEqualTo(listRequisitionsResponse { requisitions += REQUISITION })
  }

  @Test
  fun `listRequisitions requests internal Requisitions filtered by Measurement state`() =
    runBlocking {
      whenever(internalRequisitionMock.streamRequisitions(any()))
        .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION, INTERNAL_REQUISITION))
      val request = listRequisitionsRequest {
        parent = DATA_PROVIDER_NAME
        pageSize = 2
        filter = filter { measurementStates += Measurement.State.FAILED }
      }

      val response =
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.listRequisitions(request) }
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
                states += VISIBLE_REQUISITION_STATES
                externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
                measurementStates += InternalMeasurement.State.FAILED
              }
          }
        )
      assertThat(response)
        .ignoringFields(ListRequisitionsResponse.NEXT_PAGE_TOKEN_FIELD_NUMBER)
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          listRequisitionsResponse {
            requisitions += REQUISITION
            requisitions += REQUISITION
          }
        )
      assertThat(ListRequisitionsPageToken.parseFrom(response.nextPageToken.base64UrlDecode()))
        .isEqualTo(
          listRequisitionsPageToken {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            measurementStates += Measurement.State.FAILED
            lastRequisition = previousPageEnd {
              externalRequisitionId = EXTERNAL_REQUISITION_ID
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            }
          }
        )
    }

  @Test
  fun `listRequisitions with more results remaining returns response with next page token`() {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION))
    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 1
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listRequisitions(request) }
      }

    assertThat(result)
      .ignoringFields(ListRequisitionsResponse.NEXT_PAGE_TOKEN_FIELD_NUMBER)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(listRequisitionsResponse { requisitions += REQUISITION })
    assertThat(ListRequisitionsPageToken.parseFrom(result.nextPageToken.base64UrlDecode()))
      .isEqualTo(
        listRequisitionsPageToken {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          lastRequisition = previousPageEnd {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          }
        }
      )
  }

  @Test
  fun `listRequisitions throws INVALID ARGUMENT when page size is negative`() {
    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = -123
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.listRequisitions(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("page_size")
  }

  @Test
  fun `listRequisitions throws INVALID ARGUMENT when state filter mismatches on next page`() {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION, INTERNAL_REQUISITION))
      .thenReturn(flowOf(INTERNAL_REQUISITION))
    val initialRequest = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
      filter = filter { states += State.UNFULFILLED }
    }
    val initialResponse =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listRequisitions(initialRequest) }
      }
    val request =
      initialRequest.copy {
        pageToken = initialResponse.nextPageToken
        filter = filter.copy { states.clear() }
      }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("page")
  }

  @Test
  fun `listRequisitions throws INVALID ARGUMENT when Measurement state filter mismatches on next page`() {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION, INTERNAL_REQUISITION))
      .thenReturn(flowOf(INTERNAL_REQUISITION))
    val initialRequest = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
      filter = filter { measurementStates += Measurement.State.AWAITING_REQUISITION_FULFILLMENT }
    }
    val initialResponse =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listRequisitions(initialRequest) }
      }
    val request =
      initialRequest.copy {
        pageToken = initialResponse.nextPageToken
        filter = filter.copy { measurementStates += Measurement.State.FAILED }
      }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("page")
  }

  @Test
  fun `listRequisitions throws PERMISSION_DENIED for MC principal with DataProvider parent`() {
    val request = listRequisitionsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listRequisitions throws PERMISSION_DENIED for EDP principal with Measurement parent`() {
    val request = listRequisitionsRequest { parent = MEASUREMENT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.listRequisitions(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listRequisitions throws PERMISSION_DENIED when EDP principal doesn't match`() {
    val request = listRequisitionsRequest { parent = DATA_PROVIDER_NAME }

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
    val request = listRequisitionsRequest { parent = DATA_PROVIDER_NAME }

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
          details = details {
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

  @Test
  fun `refuseRequisition for requisition for direct measurement returns the updated requisition`() =
    runBlocking {
      whenever(internalRequisitionMock.refuseRequisition(any()))
        .thenReturn(
          INTERNAL_REQUISITION.copy {
            state = InternalState.REFUSED
            details = details {
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

  @Test
  fun `fulfillDirectRequisition fulfills the requisition when direct protocol config is not specified`() =
    runBlocking {
      whenever(internalRequisitionMock.fulfillRequisition(any()))
        .thenReturn(
          INTERNAL_REQUISITION.copy {
            state = InternalState.FULFILLED
            details = details { encryptedData = REQUISITION_ENCRYPTED_DATA }
          }
        )

      val request = fulfillDirectRequisitionRequest {
        name = REQUISITION_NAME
        encryptedData = REQUISITION_ENCRYPTED_DATA
        nonce = NONCE
      }

      val result =
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.fulfillDirectRequisition(request) }
        }

      val expected = fulfillDirectRequisitionResponse { state = State.FULFILLED }

      verifyProtoArgument(
          internalRequisitionMock,
          RequisitionsCoroutineImplBase::fulfillRequisition
        )
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          internalFulfillRequisitionRequest {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            nonce = NONCE
            directParams = directRequisitionParams {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              encryptedData = REQUISITION_ENCRYPTED_DATA
            }
          }
        )

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `fulfillDirectRequisition fulfills the requisition when direct protocol config is specified`() =
    runBlocking {
      whenever(internalRequisitionMock.fulfillRequisition(any()))
        .thenReturn(
          INTERNAL_REQUISITION.copy {
            state = InternalState.FULFILLED
            details = details { encryptedData = REQUISITION_ENCRYPTED_DATA }
            parentMeasurement =
              parentMeasurement.copy {
                protocolConfig = internalProtocolConfig {
                  externalProtocolConfigId = "direct"
                  direct = INTERNAL_DIRECT_RF_PROTOCOL_CONFIG
                }
              }
          }
        )

      val request = fulfillDirectRequisitionRequest {
        name = REQUISITION_NAME
        encryptedData = REQUISITION_ENCRYPTED_DATA
        nonce = NONCE
      }

      val result =
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.fulfillDirectRequisition(request) }
        }

      val expected = fulfillDirectRequisitionResponse { state = State.FULFILLED }

      verifyProtoArgument(
          internalRequisitionMock,
          RequisitionsCoroutineImplBase::fulfillRequisition
        )
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          internalFulfillRequisitionRequest {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            nonce = NONCE
            directParams = directRequisitionParams {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              encryptedData = REQUISITION_ENCRYPTED_DATA
            }
          }
        )

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `fulfillDirectRequisition throw INVALID_ARGUMENT when name is unspecified`() = runBlocking {
    val request = fulfillDirectRequisitionRequest {
      // No name
      encryptedData = REQUISITION_ENCRYPTED_DATA
      nonce = NONCE
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.fulfillDirectRequisition(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `fulfillDirectRequisition throw INVALID_ARGUMENT when encrypted_data is empty`() =
    runBlocking {
      val request = fulfillDirectRequisitionRequest {
        name = REQUISITION_NAME
        // No encrypted_data
        nonce = NONCE
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withDataProviderPrincipal(DATA_PROVIDER_NAME) {
            runBlocking { service.fulfillDirectRequisition(request) }
          }
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `fulfillDirectRequisition throw INVALID_ARGUMENT when name is invalid`() = runBlocking {
    val request = fulfillDirectRequisitionRequest {
      name = INVALID_REQUISITION_NAME
      encryptedData = REQUISITION_ENCRYPTED_DATA
      nonce = NONCE
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.fulfillDirectRequisition(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `fulfillDirectRequisition throw INVALID_ARGUMENT when nonce is missing`() = runBlocking {
    val request = fulfillDirectRequisitionRequest {
      name = REQUISITION_NAME
      encryptedData = REQUISITION_ENCRYPTED_DATA
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.fulfillDirectRequisition(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `fulfillDirectRequisition throw PERMISSION_DENIED when EDP doesn't match`() = runBlocking {
    val request = fulfillDirectRequisitionRequest {
      name = REQUISITION_NAME
      encryptedData = REQUISITION_ENCRYPTED_DATA
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.fulfillDirectRequisition(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  companion object {
    private const val MAXIMUM_FREQUENCY_DIRECT_DISTRIBUTION = 10
    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = UPDATE_TIME.toByteString()
      reachAndFrequency =
        MeasurementSpecKt.reachAndFrequency {
          reachPrivacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 1.0
          }
          frequencyPrivacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 1.0
          }
        }
      vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval { width = 1.0f }
      nonceHashes += ByteString.copyFromUtf8("foo")
    }

    private val DIRECT_RF_PROTOCOL_CONFIG =
      ProtocolConfigKt.direct {
        noiseMechanisms += ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
        noiseMechanisms += ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
        deterministicCountDistinct = ProtocolConfigKt.DirectKt.deterministicCountDistinct {}
        liquidLegionsCountDistinct = ProtocolConfigKt.DirectKt.liquidLegionsCountDistinct {}
        deterministicDistribution =
          ProtocolConfigKt.DirectKt.deterministicDistribution {
            maximumFrequency = MAXIMUM_FREQUENCY_DIRECT_DISTRIBUTION
          }
        liquidLegionsDistribution =
          ProtocolConfigKt.DirectKt.liquidLegionsDistribution {
            maximumFrequency = MAXIMUM_FREQUENCY_DIRECT_DISTRIBUTION
          }
      }

    private val INTERNAL_DIRECT_RF_PROTOCOL_CONFIG =
      InternalProtocolConfigKt.direct {
        noiseMechanisms += InternalProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
        noiseMechanisms += InternalProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
        deterministicCountDistinct = InternalProtocolConfigKt.DirectKt.deterministicCountDistinct {}
        liquidLegionsCountDistinct = InternalProtocolConfigKt.DirectKt.liquidLegionsCountDistinct {}
        deterministicDistribution =
          InternalProtocolConfigKt.DirectKt.deterministicDistribution {
            maximumFrequency = MAXIMUM_FREQUENCY_DIRECT_DISTRIBUTION
          }
        liquidLegionsDistribution =
          InternalProtocolConfigKt.DirectKt.liquidLegionsDistribution {
            maximumFrequency = MAXIMUM_FREQUENCY_DIRECT_DISTRIBUTION
          }
      }

    private val INTERNAL_REQUISITION: InternalRequisition = internalRequisition {
      externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
      externalMeasurementId = EXTERNAL_MEASUREMENT_ID
      externalRequisitionId = EXTERNAL_REQUISITION_ID
      externalComputationId = 4L
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      updateTime = UPDATE_TIME
      state = InternalState.FULFILLED
      externalFulfillingDuchyId = "9"
      duchies[DUCHY_ID] = duchyValue {
        externalDuchyCertificateId = 6L
        liquidLegionsV2 = liquidLegionsV2Details {
          elGamalPublicKey = UPDATE_TIME.toByteString()
          elGamalPublicKeySignature = UPDATE_TIME.toByteString()
        }
      }
      dataProviderCertificate = internalCertificate {
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        externalCertificateId = 7L
      }
      parentMeasurement = parentMeasurement {
        apiVersion = Version.V2_ALPHA.string
        externalMeasurementConsumerCertificateId = 8L
        measurementSpec = MEASUREMENT_SPEC.toByteString()
        protocolConfig = internalProtocolConfig {
          externalProtocolConfigId = "llv2"
          liquidLegionsV2 = InternalProtocolConfig.LiquidLegionsV2.getDefaultInstance()
        }
        state = InternalMeasurement.State.PENDING_REQUISITION_FULFILLMENT
        dataProvidersCount = 1
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
      measurementSpec = signedData {
        data = INTERNAL_REQUISITION.parentMeasurement.measurementSpec
        signature = INTERNAL_REQUISITION.parentMeasurement.measurementSpecSignature
      }
      protocolConfig = protocolConfig {
        measurementType = ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
        protocols +=
          ProtocolConfigKt.protocol { direct = DEFAULT_DIRECT_REACH_AND_FREQUENCY_PROTOCOL_CONFIG }
      }
      dataProviderCertificate =
        DataProviderCertificateKey(
            externalIdToApiId(INTERNAL_REQUISITION.externalDataProviderId),
            externalIdToApiId(INTERNAL_REQUISITION.dataProviderCertificate.externalCertificateId)
          )
          .toName()
      dataProviderPublicKey = signedData {
        data = INTERNAL_REQUISITION.details.dataProviderPublicKey
        signature = INTERNAL_REQUISITION.details.dataProviderPublicKeySignature
      }

      val internalDuchyValue: InternalRequisition.DuchyValue =
        INTERNAL_REQUISITION.duchiesMap.getValue(DUCHY_ID)
      duchies += duchyEntry {
        key = DUCHY_ID
        value = value {
          duchyCertificate = DUCHY_CERTIFICATE_NAME
          liquidLegionsV2 = liquidLegionsV2 {
            elGamalPublicKey = signedData {
              data = internalDuchyValue.liquidLegionsV2.elGamalPublicKey
              signature = internalDuchyValue.liquidLegionsV2.elGamalPublicKeySignature
            }
          }
        }
      }

      state = State.FULFILLED
      measurementState = Measurement.State.AWAITING_REQUISITION_FULFILLMENT
    }
  }
}
