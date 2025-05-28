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
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.any as protoAny
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.timestamp
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
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetRequisitionRequest
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
import org.wfanet.measurement.api.v2alpha.RequisitionKt
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.elGamalPublicKey
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.getRequisitionRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsPageToken
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt.directRequisitionParams
import org.wfanet.measurement.internal.kingdom.HonestMajorityShareShuffleParams
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.ProtocolConfig as InternalProtocolConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt as InternalProtocolConfigKt
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.Requisition.State as InternalState
import org.wfanet.measurement.internal.kingdom.RequisitionDetailsKt
import org.wfanet.measurement.internal.kingdom.RequisitionKt as InternalRequisitionKt
import org.wfanet.measurement.internal.kingdom.RequisitionRefusal as InternalRefusal
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest as internalFulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.honestMajorityShareShuffleParams
import org.wfanet.measurement.internal.kingdom.liquidLegionsV2Params
import org.wfanet.measurement.internal.kingdom.protocolConfig as internalProtocolConfig
import org.wfanet.measurement.internal.kingdom.refuseRequisitionRequest as internalRefuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.requisition as internalRequisition
import org.wfanet.measurement.internal.kingdom.requisitionDetails
import org.wfanet.measurement.internal.kingdom.requisitionRefusal as internalRequisitionRefusal
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionNotFoundByDataProviderException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionStateIllegalException

private val UPDATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val ETAG = ETags.computeETag(UPDATE_TIME.toInstant())

private const val DEFAULT_LIMIT = 10

private const val WILDCARD_NAME = "dataProviders/-"

private const val DUCHY_ID = "worker1"
private const val DUCHY_CERTIFICATE_NAME = "duchies/$DUCHY_ID/certificates/AAAAAAAAAAY"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME_2 = "measurementConsumers/BBBBBBBBBHs"
private const val MEASUREMENT_NAME = "$MEASUREMENT_CONSUMER_NAME/measurements/AAAAAAAAAHs"

private val DATA_PROVIDER_NAME = makeDataProvider(123L)
private val DATA_PROVIDER_CERTIFICATE_NAME = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAAY"
private val DATA_PROVIDER_NAME_2 = makeDataProvider(124L)

private val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/AAAAAAAAAHs"
private const val INVALID_REQUISITION_NAME = "requisitions/AAAAAAAAAHs"

private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"

private val EXTERNAL_REQUISITION_ID =
  apiIdToExternalId(CanonicalRequisitionKey.fromName(REQUISITION_NAME)!!.requisitionId)
private val EXTERNAL_DATA_PROVIDER_ID =
  apiIdToExternalId(CanonicalRequisitionKey.fromName(REQUISITION_NAME)!!.dataProviderId)
private val EXTERNAL_DATA_PROVIDER_CERTIFICATE_ID =
  apiIdToExternalId(
    DataProviderCertificateKey.fromName(DATA_PROVIDER_CERTIFICATE_NAME)!!.certificateId
  )
private val EXTERNAL_MEASUREMENT_ID =
  apiIdToExternalId(MeasurementKey.fromName(MEASUREMENT_NAME)!!.measurementId)
private val EXTERNAL_MEASUREMENT_CONSUMER_ID =
  apiIdToExternalId(
    MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!.measurementConsumerId
  )

private val ENCRYPTED_RESULT = encryptedMessage {
  ciphertext = "foo".toByteStringUtf8()
  typeUrl = ProtoReflection.getTypeUrl(SignedMessage.getDescriptor())
}
private const val NONCE = -7452112597811743614 // Hex: 9894C7134537B482

private val VISIBLE_REQUISITION_STATES: Set<InternalRequisition.State> =
  setOf(
    InternalRequisition.State.UNFULFILLED,
    InternalRequisition.State.FULFILLED,
    InternalRequisition.State.REFUSED,
    InternalRequisition.State.WITHDRAWN,
  )

@RunWith(JUnit4::class)
class RequisitionsServiceTest {
  private val internalRequisitionMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { refuseRequisition(any()) }
      .thenReturn(
        INTERNAL_REQUISITION.copy {
          state = InternalState.REFUSED
          details = requisitionDetails {
            refusal = internalRequisitionRefusal {
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

    val streamRequisitionRequest: StreamRequisitionsRequest = captureFirst {
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

    val streamRequisitionRequest: StreamRequisitionsRequest = captureFirst {
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
  fun `listRequisitions requests internal Requisitions whose duchy protocol is not set`() {
    val internalRequisition =
      INTERNAL_REQUISITION.copy {
        parentMeasurement =
          parentMeasurement.copy {
            protocolConfig = internalProtocolConfig {
              externalProtocolConfigId = "direct"
              direct = INTERNAL_DIRECT_RF_PROTOCOL_CONFIG
            }
          }
        duchies[DUCHY_ID] = InternalRequisitionKt.duchyValue { externalDuchyCertificateId = 6L }
      }

    val requisition =
      REQUISITION.copy {
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols += ProtocolConfigKt.protocol { direct = DIRECT_RF_PROTOCOL_CONFIG }
          }

        duchies.clear()
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

    val streamRequisitionRequest: StreamRequisitionsRequest = captureFirst {
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
    whenever(internalRequisitionMock.streamRequisitions(any())).thenAnswer {
      val request: StreamRequisitionsRequest = it.getArgument(0)
      if (request.filter.hasAfter()) {
        assertThat(request.filter.after.updateTime).isEqualTo(INTERNAL_REQUISITION.updateTime)
        assertThat(request.filter.after.externalDataProviderId)
          .isEqualTo(INTERNAL_REQUISITION.externalDataProviderId)
        assertThat(request.filter.after.externalRequisitionId)
          .isEqualTo(INTERNAL_REQUISITION.externalRequisitionId)
      }
      flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION)
    }

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

    assertThat(response)
      .isEqualTo(
        listRequisitionsResponse {
          requisitions += REQUISITION
          requisitions += REQUISITION
          nextPageToken = initialResponse.nextPageToken
        }
      )
  }

  @Test
  fun `listRequisitions with more results remaining returns response with next page token`() {
    val laterUpdateTime = timestamp { seconds = INTERNAL_REQUISITION.updateTime.seconds + 100 }
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(
        flowOf(INTERNAL_REQUISITION, INTERNAL_REQUISITION.copy { updateTime = laterUpdateTime })
      )
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
            updateTime = INTERNAL_REQUISITION.updateTime
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
  fun `listRequisitions shows HMSS requisitions with encryption key`() = runBlocking {
    whenever(internalRequisitionMock.streamRequisitions(any()))
      .thenReturn(flowOf(INTERNAL_HMSS_REQUISITION))
    val request = listRequisitionsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
    }

    val response =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listRequisitions(request) }
      }

    val streamRequisitionRequest: StreamRequisitionsRequest = captureFirst {
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
            }
        }
      )
    assertThat(response)
      .ignoringFields(ListRequisitionsResponse.NEXT_PAGE_TOKEN_FIELD_NUMBER)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(listRequisitionsResponse { requisitions += HMSS_REQUISITION })
  }

  @Test
  fun `getRequisition returns the Requisition`() = runBlocking {
    whenever(internalRequisitionMock.getRequisition(any())).thenReturn(INTERNAL_REQUISITION)

    val request = getRequisitionRequest { name = REQUISITION_NAME }

    val requisition =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.getRequisition(request) }
      }

    assertThat(requisition).isEqualTo(REQUISITION)
  }

  @Test
  fun `getRequisition throws UNAUTHENTICATED when no principal is not found`() {
    val request = getRequisitionRequest { name = REQUISITION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.getRequisition(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getRequisition throws PERMISSION_DENIED when edp caller doesn't match`() {
    val request = getRequisitionRequest { name = REQUISITION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.getRequisition(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getRequisition throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = getRequisitionRequest { name = REQUISITION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.getRequisition(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getRequisition throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.getRequisition(GetRequisitionRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisition throws UNAUTHENTICATED when no principal is not found`() {
    val request = refuseRequisitionRequest {
      name = REQUISITION_NAME
      refusal = refusal { justification = Refusal.Justification.UNFULFILLABLE }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.refuseRequisition(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `refuseRequisition throws PERMISSION_DENIED when edp caller doesn't match`() {
    val request = refuseRequisitionRequest {
      name = REQUISITION_NAME
      refusal = refusal { justification = Refusal.Justification.UNFULFILLABLE }
    }

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
    val request = refuseRequisitionRequest {
      name = REQUISITION_NAME
      refusal = refusal { justification = Refusal.Justification.UNFULFILLABLE }
    }

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
            details.copy {
              refusal = internalRequisitionRefusal {
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
          refusal = internalRequisitionRefusal {
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
            details =
              details.copy {
                refusal = internalRequisitionRefusal {
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
            refusal = internalRequisitionRefusal {
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
            details = details.copy { encryptedData = ENCRYPTED_RESULT.ciphertext }
          }
        )

      val request = fulfillDirectRequisitionRequest {
        name = REQUISITION_NAME
        encryptedResult = ENCRYPTED_RESULT
        nonce = NONCE
        certificate = DATA_PROVIDER_CERTIFICATE_NAME
      }

      val result =
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.fulfillDirectRequisition(request) }
        }

      val expected = fulfillDirectRequisitionResponse { state = State.FULFILLED }
      verifyProtoArgument(
          internalRequisitionMock,
          RequisitionsCoroutineImplBase::fulfillRequisition,
        )
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          internalFulfillRequisitionRequest {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            nonce = NONCE
            directParams = directRequisitionParams {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              encryptedData = ENCRYPTED_RESULT.ciphertext
              externalCertificateId = EXTERNAL_DATA_PROVIDER_CERTIFICATE_ID
              apiVersion = Version.V2_ALPHA.string
            }
          }
        )

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `fulfillDirectRequisition fulfills the requisition when certificate is not specified`() =
    runBlocking {
      whenever(internalRequisitionMock.fulfillRequisition(any()))
        .thenReturn(
          INTERNAL_REQUISITION.copy {
            state = InternalState.FULFILLED
            details = details.copy { encryptedData = ENCRYPTED_RESULT.ciphertext }
          }
        )

      val request = fulfillDirectRequisitionRequest {
        name = REQUISITION_NAME
        encryptedResult = ENCRYPTED_RESULT
        nonce = NONCE
      }

      val result =
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.fulfillDirectRequisition(request) }
        }

      val expected = fulfillDirectRequisitionResponse { state = State.FULFILLED }
      verifyProtoArgument(
          internalRequisitionMock,
          RequisitionsCoroutineImplBase::fulfillRequisition,
        )
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          internalFulfillRequisitionRequest {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            nonce = NONCE
            directParams = directRequisitionParams {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              encryptedData = ENCRYPTED_RESULT.ciphertext
              apiVersion = Version.V2_ALPHA.string
            }
          }
        )

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `fulfillDirectRequisition fulfills the requisition when direct protocol config is specified`() =
    runBlocking {
      val internalRequisition =
        INTERNAL_REQUISITION.copy {
          state = InternalState.FULFILLED
          details =
            details.copy {
              encryptedData = ENCRYPTED_RESULT.ciphertext
              fulfillmentContext =
                RequisitionDetailsKt.fulfillmentContext {
                  buildLabel = "nightly-20250414.1"
                  warnings += "The data smell funny"
                }
            }
          parentMeasurement =
            parentMeasurement.copy {
              protocolConfig = internalProtocolConfig {
                externalProtocolConfigId = "direct"
                direct = INTERNAL_DIRECT_RF_PROTOCOL_CONFIG
              }
            }
        }
      whenever(internalRequisitionMock.fulfillRequisition(any())).thenReturn(internalRequisition)

      val request = fulfillDirectRequisitionRequest {
        name = REQUISITION_NAME
        encryptedResult = ENCRYPTED_RESULT
        nonce = NONCE
        certificate = DATA_PROVIDER_CERTIFICATE_NAME
        fulfillmentContext =
          RequisitionKt.fulfillmentContext {
            buildLabel = internalRequisition.details.fulfillmentContext.buildLabel
            warnings += internalRequisition.details.fulfillmentContext.warningsList
          }
      }

      val response =
        withDataProviderPrincipal(DATA_PROVIDER_NAME) { service.fulfillDirectRequisition(request) }

      verifyProtoArgument(
          internalRequisitionMock,
          RequisitionsCoroutineImplBase::fulfillRequisition,
        )
        .isEqualTo(
          internalFulfillRequisitionRequest {
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            nonce = NONCE
            fulfillmentContext = internalRequisition.details.fulfillmentContext
            directParams = directRequisitionParams {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              encryptedData = ENCRYPTED_RESULT.ciphertext
              externalCertificateId = EXTERNAL_DATA_PROVIDER_CERTIFICATE_ID
              apiVersion = Version.V2_ALPHA.string
            }
          }
        )
      assertThat(response).isEqualTo(fulfillDirectRequisitionResponse { state = State.FULFILLED })
    }

  @Test
  fun `fulfillDirectRequisition throw INVALID_ARGUMENT when name is unspecified`() = runBlocking {
    val request = fulfillDirectRequisitionRequest {
      // No name
      encryptedResult = ENCRYPTED_RESULT
      nonce = NONCE
      certificate = DATA_PROVIDER_CERTIFICATE_NAME
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
        certificate = DATA_PROVIDER_CERTIFICATE_NAME
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
      encryptedResult = ENCRYPTED_RESULT
      nonce = NONCE
      certificate = DATA_PROVIDER_CERTIFICATE_NAME
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
      encryptedResult = ENCRYPTED_RESULT
      certificate = DATA_PROVIDER_CERTIFICATE_NAME
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
  fun `fulfillDirectRequisition throw INVALID_ARGUMENT when certificate resource is not valid`() =
    runBlocking {
      val request = fulfillDirectRequisitionRequest {
        name = REQUISITION_NAME
        encryptedResult = ENCRYPTED_RESULT
        certificate = "some-invalid-certificate-resource"
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
      encryptedResult = ENCRYPTED_RESULT
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.fulfillDirectRequisition(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisition throws NOT_FOUND with requisition name when requisition is missing`() {
    internalRequisitionMock.stub {
      onBlocking { refuseRequisition(any()) }
        .thenThrow(
          RequisitionNotFoundByDataProviderException(
              ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              ExternalId(EXTERNAL_REQUISITION_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.refuseRequisition(
              refuseRequisitionRequest {
                name = REQUISITION_NAME
                refusal = refusal { justification = Refusal.Justification.UNFULFILLABLE }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("requisition", REQUISITION_NAME)
  }

  @Test
  fun `refuseRequisition throws FAILED_PRECONDITION with requisition id and state when requisition state is illegal`() {
    internalRequisitionMock.stub {
      onBlocking { refuseRequisition(any()) }
        .thenThrow(
          RequisitionStateIllegalException(
              ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              InternalState.FULFILLED,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Requisition state illegal.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.refuseRequisition(
              refuseRequisitionRequest {
                name = REQUISITION_NAME
                refusal = refusal { justification = Refusal.Justification.UNFULFILLABLE }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("state", State.FULFILLED.toString())
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("requisitionId", externalIdToApiId(EXTERNAL_REQUISITION_ID))
  }

  @Test
  fun `refuseRequisition throws FAILED_PRECONDITION with measurement name and state when measurement state is illegal`() {
    internalRequisitionMock.stub {
      onBlocking { refuseRequisition(any()) }
        .thenThrow(
          MeasurementStateIllegalException(
              ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
              ExternalId(EXTERNAL_MEASUREMENT_ID),
              InternalMeasurement.State.FAILED,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Measurement state illegal.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.refuseRequisition(
              refuseRequisitionRequest {
                name = REQUISITION_NAME
                refusal = refusal { justification = Refusal.Justification.UNFULFILLABLE }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("state", Measurement.State.FAILED.toString())
    assertThat(exception.errorInfo?.metadataMap).containsEntry("measurement", MEASUREMENT_NAME)
  }

  @Test
  fun `fulfillDirectRequisition NOT_FOUND with requisition name when requisition is missing`() {
    internalRequisitionMock.stub {
      onBlocking { fulfillRequisition(any()) }
        .thenThrow(
          RequisitionNotFoundByDataProviderException(
              ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              ExternalId(EXTERNAL_REQUISITION_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.fulfillDirectRequisition(
              fulfillDirectRequisitionRequest {
                name = REQUISITION_NAME
                encryptedResult = ENCRYPTED_RESULT
                nonce = NONCE
                certificate = DATA_PROVIDER_CERTIFICATE_NAME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("requisition", REQUISITION_NAME)
  }

  @Test
  fun `fulfillDirectRequisition FAILED_PRECONDITION with requisition id and state when requisition state is illegal`() {
    internalRequisitionMock.stub {
      onBlocking { fulfillRequisition(any()) }
        .thenThrow(
          RequisitionStateIllegalException(
              ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              InternalState.FULFILLED,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Requisition state illegal.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.fulfillDirectRequisition(
              fulfillDirectRequisitionRequest {
                name = REQUISITION_NAME
                encryptedResult = ENCRYPTED_RESULT
                nonce = NONCE
                certificate = DATA_PROVIDER_CERTIFICATE_NAME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("state", State.FULFILLED.toString())
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("requisitionId", externalIdToApiId(EXTERNAL_REQUISITION_ID))
  }

  @Test
  fun `fulfillDirectRequisition FAILED_PRECONDITION with measurement name and state when measurement state is illegal`() {
    internalRequisitionMock.stub {
      onBlocking { fulfillRequisition(any()) }
        .thenThrow(
          MeasurementStateIllegalException(
              ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
              ExternalId(EXTERNAL_MEASUREMENT_ID),
              InternalMeasurement.State.FAILED,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Measurement state illegal.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.fulfillDirectRequisition(
              fulfillDirectRequisitionRequest {
                name = REQUISITION_NAME
                encryptedResult = ENCRYPTED_RESULT
                nonce = NONCE
                certificate = DATA_PROVIDER_CERTIFICATE_NAME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("state", Measurement.State.FAILED.toString())
    assertThat(exception.errorInfo?.metadataMap).containsEntry("measurement", MEASUREMENT_NAME)
  }

  @Test
  fun `fulfillDirectRequisition FAILED_PRECONDITION with duchy name when duchy not found`() {
    val duchyName = DuchyKey(DUCHY_ID).toName()
    internalRequisitionMock.stub {
      onBlocking { fulfillRequisition(any()) }
        .thenThrow(
          DuchyNotFoundException(DUCHY_ID)
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Duchy not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.fulfillDirectRequisition(
              fulfillDirectRequisitionRequest {
                name = REQUISITION_NAME
                encryptedResult = ENCRYPTED_RESULT
                nonce = NONCE
                certificate = DATA_PROVIDER_CERTIFICATE_NAME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("duchy", duchyName)
  }

  companion object {
    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = encryptionPublicKey { data = UPDATE_TIME.toByteString() }.pack()
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
    private val PACKED_MEASUREMENT_SPEC: ProtoAny = MEASUREMENT_SPEC.pack()

    private val ENCRYPTED_REQUISITION_SPEC = encryptedMessage {
      ciphertext = "RequisitionSpec ciphertext".toByteStringUtf8()
      typeUrl = ProtoReflection.getTypeUrl(SignedMessage.getDescriptor())
    }

    private val DATA_PROVIDER_PUBLIC_KEY = encryptionPublicKey {
      data = "key data".toByteStringUtf8()
    }
    private val PACKED_DATA_PROVIDER_PUBLIC_KEY: ProtoAny = DATA_PROVIDER_PUBLIC_KEY.pack()

    private val EL_GAMAL_PUBLIC_KEY = elGamalPublicKey { ellipticCurveId = 123 }
    private val SIGNED_EL_GAMAL_PUBLIC_KEY = signedMessage {
      setMessage(EL_GAMAL_PUBLIC_KEY.pack())
      signature = UPDATE_TIME.toByteString()
      signatureAlgorithmOid = "2.9999"
    }
    private val TINK_PUBLIC_KEY_1 = ByteString.copyFromUtf8("This is an Tink Public Key 1.")
    private val TINK_PUBLIC_KEY_SIGNATURE_1 =
      ByteString.copyFromUtf8("This is an Tink Public Key signature 1.")
    private const val TINK_PUBLIC_KEY_SIGNATURE_ALGORITHM_OID = "2.9999"

    private val TINK_PUBLIC_KEY_2 = ByteString.copyFromUtf8("This is an Tink Public Key 2.")
    private val TINK_PUBLIC_KEY_SIGNATURE_2 =
      ByteString.copyFromUtf8("This is an Tink Public Key signature 2.")

    private val DIRECT_RF_PROTOCOL_CONFIG =
      ProtocolConfigKt.direct {
        noiseMechanisms += ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
        noiseMechanisms += ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
        customDirectMethodology = ProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
        deterministicCountDistinct =
          ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
        liquidLegionsCountDistinct =
          ProtocolConfig.Direct.LiquidLegionsCountDistinct.getDefaultInstance()
        deterministicDistribution =
          ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
        liquidLegionsDistribution =
          ProtocolConfig.Direct.LiquidLegionsDistribution.getDefaultInstance()
      }

    private val INTERNAL_DIRECT_RF_PROTOCOL_CONFIG =
      InternalProtocolConfigKt.direct {
        noiseMechanisms += InternalProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
        noiseMechanisms += InternalProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
        customDirectMethodology =
          InternalProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
        deterministicCountDistinct =
          InternalProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
        liquidLegionsCountDistinct =
          InternalProtocolConfig.Direct.LiquidLegionsCountDistinct.getDefaultInstance()
        deterministicDistribution =
          InternalProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
        liquidLegionsDistribution =
          InternalProtocolConfig.Direct.LiquidLegionsDistribution.getDefaultInstance()
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
      duchies[DUCHY_ID] =
        InternalRequisitionKt.duchyValue {
          externalDuchyCertificateId = 6L
          liquidLegionsV2 = liquidLegionsV2Params {
            elGamalPublicKey = SIGNED_EL_GAMAL_PUBLIC_KEY.message.value
            elGamalPublicKeySignature = SIGNED_EL_GAMAL_PUBLIC_KEY.signature
            elGamalPublicKeySignatureAlgorithmOid = SIGNED_EL_GAMAL_PUBLIC_KEY.signatureAlgorithmOid
          }
        }
      dataProviderCertificate = internalCertificate {
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        externalCertificateId = 7L
      }
      parentMeasurement =
        InternalRequisitionKt.parentMeasurement {
          apiVersion = Version.V2_ALPHA.string
          externalMeasurementConsumerCertificateId = 8L
          measurementSpec = PACKED_MEASUREMENT_SPEC.value
          protocolConfig = internalProtocolConfig {
            externalProtocolConfigId = "llv2"
            liquidLegionsV2 = InternalProtocolConfig.LiquidLegionsV2.getDefaultInstance()
          }
          state = InternalMeasurement.State.PENDING_REQUISITION_FULFILLMENT
          dataProvidersCount = 1
        }
      details = requisitionDetails {
        dataProviderPublicKey = PACKED_DATA_PROVIDER_PUBLIC_KEY.value
        encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC.ciphertext
      }
      etag = ETAG
    }

    private val INTERNAL_HMSS_REQUISITION =
      INTERNAL_REQUISITION.copy {
        val source = this@copy

        state = InternalState.UNFULFILLED
        duchies.clear()
        duchies["aggregator"] =
          InternalRequisitionKt.duchyValue {
            externalDuchyCertificateId = 6L
            honestMajorityShareShuffle = HonestMajorityShareShuffleParams.getDefaultInstance()
          }
        duchies["worker1"] =
          InternalRequisitionKt.duchyValue {
            externalDuchyCertificateId = 6L
            honestMajorityShareShuffle = honestMajorityShareShuffleParams {
              tinkPublicKey = TINK_PUBLIC_KEY_1
              tinkPublicKeySignature = TINK_PUBLIC_KEY_SIGNATURE_1
              tinkPublicKeySignatureAlgorithmOid = TINK_PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
            }
          }
        duchies["worker2"] =
          InternalRequisitionKt.duchyValue {
            externalDuchyCertificateId = 6L
            honestMajorityShareShuffle = honestMajorityShareShuffleParams {
              tinkPublicKey = TINK_PUBLIC_KEY_2
              tinkPublicKeySignature = TINK_PUBLIC_KEY_SIGNATURE_2
              tinkPublicKeySignatureAlgorithmOid = TINK_PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
            }
          }
        externalFulfillingDuchyId = "worker1"

        parentMeasurement =
          source.parentMeasurement.copy {
            protocolConfig = internalProtocolConfig {
              externalProtocolConfigId = "hmss"
              honestMajorityShareShuffle =
                InternalProtocolConfigKt.honestMajorityShareShuffle {
                  reachRingModulus = 127
                  reachAndFrequencyRingModulus = 127
                  noiseMechanism = InternalProtocolConfig.NoiseMechanism.DISCRETE_GAUSSIAN
                }
            }
            dataProvidersCount = 2
          }
      }

    private val REQUISITION: Requisition = requisition {
      name =
        CanonicalRequisitionKey(
            externalIdToApiId(INTERNAL_REQUISITION.externalDataProviderId),
            externalIdToApiId(INTERNAL_REQUISITION.externalRequisitionId),
          )
          .toName()

      measurement =
        MeasurementKey(
            externalIdToApiId(INTERNAL_REQUISITION.externalMeasurementConsumerId),
            externalIdToApiId(INTERNAL_REQUISITION.externalMeasurementId),
          )
          .toName()
      measurementConsumerCertificate =
        MeasurementConsumerCertificateKey(
            externalIdToApiId(INTERNAL_REQUISITION.externalMeasurementConsumerId),
            externalIdToApiId(
              INTERNAL_REQUISITION.parentMeasurement.externalMeasurementConsumerCertificateId
            ),
          )
          .toName()
      measurementSpec = signedMessage {
        setMessage(PACKED_MEASUREMENT_SPEC)
        signature = INTERNAL_REQUISITION.parentMeasurement.measurementSpecSignature
        signatureAlgorithmOid =
          INTERNAL_REQUISITION.parentMeasurement.measurementSpecSignatureAlgorithmOid
      }
      protocolConfig = protocolConfig {
        measurementType = ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
        protocols +=
          ProtocolConfigKt.protocol { direct = DEFAULT_DIRECT_REACH_AND_FREQUENCY_PROTOCOL_CONFIG }
      }
      dataProviderCertificate =
        DataProviderCertificateKey(
            externalIdToApiId(INTERNAL_REQUISITION.externalDataProviderId),
            externalIdToApiId(INTERNAL_REQUISITION.dataProviderCertificate.externalCertificateId),
          )
          .toName()
      dataProviderPublicKey = PACKED_DATA_PROVIDER_PUBLIC_KEY
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
      // field.
      encryptedRequisitionSpecCiphertext = ENCRYPTED_REQUISITION_SPEC.ciphertext

      // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
      // field.
      signedDataProviderPublicKey = signedMessage { setMessage(PACKED_DATA_PROVIDER_PUBLIC_KEY) }

      duchies += duchyEntry {
        key = DUCHY_ID
        value = value {
          duchyCertificate = DUCHY_CERTIFICATE_NAME
          liquidLegionsV2 = liquidLegionsV2 { elGamalPublicKey = SIGNED_EL_GAMAL_PUBLIC_KEY }
        }
      }

      state = State.FULFILLED
      measurementState = Measurement.State.AWAITING_REQUISITION_FULFILLMENT
      updateTime = UPDATE_TIME
      etag = ETAG
    }

    private val HMSS_REQUISITION =
      REQUISITION.copy {
        val source = this@copy
        protocolConfig =
          source.protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                honestMajorityShareShuffle =
                  ProtocolConfigKt.honestMajorityShareShuffle {
                    ringModulus = 127
                    noiseMechanism = ProtocolConfig.NoiseMechanism.DISCRETE_GAUSSIAN
                  }
              }
          }
        duchies.clear()
        duchies += duchyEntry {
          key = "worker1"
          value = value {
            duchyCertificate = DUCHY_CERTIFICATE_NAME
            honestMajorityShareShuffle =
              Requisition.DuchyEntry.HonestMajorityShareShuffle.getDefaultInstance()
          }
        }
        duchies += duchyEntry {
          key = "worker2"
          value = value {
            duchyCertificate = "duchies/worker2/certificates/AAAAAAAAAAY"
            honestMajorityShareShuffle =
              RequisitionKt.DuchyEntryKt.honestMajorityShareShuffle {
                publicKey = signedMessage {
                  setMessage(
                    protoAny {
                      value = TINK_PUBLIC_KEY_2
                      typeUrl = ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
                    }
                  )
                  signature = TINK_PUBLIC_KEY_SIGNATURE_2
                  signatureAlgorithmOid = TINK_PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
                }
              }
          }
        }
        state = State.UNFULFILLED
      }
  }
}
