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

package org.wfanet.measurement.kingdom.service.api.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import java.time.Instant
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsResponse
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.MetricRequisition.Refusal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest

private const val CAMPAIGN_REFERENCE_ID = "some-provided-campaign-id"
private const val COMBINED_PUBLIC_KEY_ID = "some-combined-public-key-id"

private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val WINDOW_START_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()
private val WINDOW_END_TIME: Timestamp = Instant.ofEpochSecond(789).toProtoTime()

private val IRRELEVANT_DETAILS: RequisitionDetails = RequisitionDetails.getDefaultInstance()
private val REQUISITION: Requisition = Requisition.newBuilder().apply {
  externalDataProviderId = 1
  externalCampaignId = 2
  externalRequisitionId = 3
  combinedPublicKeyResourceId = COMBINED_PUBLIC_KEY_ID
  providedCampaignId = CAMPAIGN_REFERENCE_ID
  createTime = CREATE_TIME
  state = RequisitionState.FULFILLED
  windowStartTime = WINDOW_START_TIME
  windowEndTime = WINDOW_END_TIME
  requisitionDetails = IRRELEVANT_DETAILS
  requisitionDetailsJson = IRRELEVANT_DETAILS.toJson()
}.build()

private val REQUISITION_API_KEY: MetricRequisition.Key =
  MetricRequisition.Key.newBuilder().apply {
    dataProviderId = ExternalId(REQUISITION.externalDataProviderId).apiId.value
    campaignId = ExternalId(REQUISITION.externalCampaignId).apiId.value
    metricRequisitionId = ExternalId(REQUISITION.externalRequisitionId).apiId.value
  }.build()

@RunWith(JUnit4::class)
class RequisitionServiceTest {
  private val requisitionStorage: RequisitionsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(requisitionStorage) }

  private val channel = grpcTestServerRule.channel
  private val service = RequisitionService(RequisitionsCoroutineStub(channel))

  @Test
  fun `listMetricRequisitions without page token`() = runBlocking<Unit> {
    whenever(requisitionStorage.streamRequisitions(any()))
      .thenReturn(flowOf(REQUISITION, REQUISITION))

    val request = ListMetricRequisitionsRequest.newBuilder().apply {
      parentBuilder.apply {
        dataProviderId = ExternalId(REQUISITION.externalDataProviderId).apiId.value
        campaignId = ExternalId(REQUISITION.externalCampaignId).apiId.value
      }
      filterBuilder.addAllStates(
        listOf(
          MetricRequisition.State.UNFULFILLED,
          MetricRequisition.State.FULFILLED
        )
      )
      pageSize = 2
      pageToken = ""
    }.build()

    val result = service.listMetricRequisitions(request)

    val expected = ListMetricRequisitionsResponse.newBuilder().apply {
      addMetricRequisitionsBuilder().apply {
        key = REQUISITION_API_KEY
        state = MetricRequisition.State.FULFILLED
        campaignReferenceId = CAMPAIGN_REFERENCE_ID
        combinedPublicKeyBuilder.combinedPublicKeyId = COMBINED_PUBLIC_KEY_ID
      }
      addMetricRequisitionsBuilder().apply {
        key = REQUISITION_API_KEY
        state = MetricRequisition.State.FULFILLED
        campaignReferenceId = CAMPAIGN_REFERENCE_ID
        combinedPublicKeyBuilder.combinedPublicKeyId = COMBINED_PUBLIC_KEY_ID
      }
      nextPageToken = CREATE_TIME.toByteArray().base64UrlEncode()
    }.build()

    assertThat(result)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(expected)

    val requisitionStorageRequest = captureFirst<StreamRequisitionsRequest> {
      verify(requisitionStorage).streamRequisitions(capture())
    }
    assertThat(requisitionStorageRequest)
      .isEqualTo(
        StreamRequisitionsRequest.newBuilder().apply {
          limit = 2
          filterBuilder.apply {
            addAllStates(listOf(RequisitionState.UNFULFILLED, RequisitionState.FULFILLED))
            addExternalDataProviderIds(REQUISITION.externalDataProviderId)
            addExternalCampaignIds(REQUISITION.externalCampaignId)
          }
        }.build()
      )
  }

  @Test
  fun `listMetricRequisitions with page token`() = runBlocking<Unit> {
    whenever(requisitionStorage.streamRequisitions(any()))
      .thenReturn(emptyFlow())

    val request = ListMetricRequisitionsRequest.newBuilder().apply {
      parentBuilder.apply {
        dataProviderId = ExternalId(REQUISITION.externalDataProviderId).apiId.value
        campaignId = ExternalId(REQUISITION.externalCampaignId).apiId.value
      }
      filterBuilder.addStates(MetricRequisition.State.UNFULFILLED)
      pageSize = 1
      pageToken = CREATE_TIME.toByteArray().base64UrlEncode()
    }.build()

    val result = service.listMetricRequisitions(request)
    val expected = ListMetricRequisitionsResponse.getDefaultInstance()

    assertThat(result).isEqualTo(expected)

    val requisitionStorageRequest = captureFirst<StreamRequisitionsRequest> {
      verify(requisitionStorage).streamRequisitions(capture())
    }
    assertThat(requisitionStorageRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        StreamRequisitionsRequest.newBuilder().apply {
          limit = 1
          filterBuilder.apply {
            addStates(RequisitionState.UNFULFILLED)
            addExternalDataProviderIds(REQUISITION.externalDataProviderId)
            addExternalCampaignIds(REQUISITION.externalCampaignId)
            createdAfter = CREATE_TIME
          }
        }.build()
      )
  }

  @Test
  fun `listRequisitions returns refused MetricRequisition`() = runBlocking {
    val requisition = REQUISITION.toBuilder().apply {
      state = RequisitionState.PERMANENTLY_UNAVAILABLE
      requisitionDetailsBuilder.refusalBuilder.apply {
        justification = RequisitionDetails.Refusal.Justification.COLLECTION_INTERVAL_TOO_DISTANT
        message = "Too old"
      }
    }.build()
    whenever(requisitionStorage.streamRequisitions(any())).thenReturn(flowOf(requisition))

    val request = ListMetricRequisitionsRequest.newBuilder().apply {
      parentBuilder.apply {
        dataProviderId = ExternalId(REQUISITION.externalDataProviderId).apiId.value
        campaignId = ExternalId(REQUISITION.externalCampaignId).apiId.value
      }
      filterBuilder.addStates(MetricRequisition.State.PERMANENTLY_UNFILLABLE)
      pageSize = 10
    }.build()
    val response = service.listMetricRequisitions(request)

    assertThat(response.metricRequisitionsList).hasSize(1)
    val metricRequisition = response.metricRequisitionsList.first()
    assertThat(metricRequisition.state).isEqualTo(MetricRequisition.State.PERMANENTLY_UNFILLABLE)
    assertThat(metricRequisition.refusal).isEqualTo(
      Refusal.newBuilder().apply {
        justification = Refusal.Justification.COLLECTION_INTERVAL_TOO_DISTANT
        message = requisition.requisitionDetails.refusal.message
      }.build()
    )
  }
}
