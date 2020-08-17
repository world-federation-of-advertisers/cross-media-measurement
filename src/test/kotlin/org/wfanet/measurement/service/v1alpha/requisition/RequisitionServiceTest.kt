// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.service.v1alpha.requisition

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import java.time.Instant
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v1alpha.FulfillMetricRequisitionRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsResponse
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.service.internal.kingdom.testing.FakeRequisitionStorage
import org.wfanet.measurement.service.testing.GrpcTestServerRule
import org.wfanet.measurement.service.v1alpha.common.DuchyAuth

@RunWith(JUnit4::class)
class RequisitionServiceTest {

  companion object {
    var CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
    private var WINDOW_START_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()
    private var WINDOW_END_TIME: Timestamp = Instant.ofEpochSecond(789).toProtoTime()

    private var IRRELEVANT_DETAILS: RequisitionDetails = RequisitionDetails.getDefaultInstance()

    var REQUISITION: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      externalRequisitionId = 3
      createTime = CREATE_TIME
      state = RequisitionState.FULFILLED
      windowStartTime = WINDOW_START_TIME
      windowEndTime = WINDOW_END_TIME
      requisitionDetails = IRRELEVANT_DETAILS
      requisitionDetailsJson = IRRELEVANT_DETAILS.toJson()
    }.build()

    val REQUISITION_API_KEY: MetricRequisition.Key =
      MetricRequisition.Key.newBuilder().apply {
        dataProviderId = ExternalId(REQUISITION.externalDataProviderId).apiId.value
        campaignId = ExternalId(REQUISITION.externalCampaignId).apiId.value
        metricRequisitionId = ExternalId(REQUISITION.externalRequisitionId).apiId.value
      }.build()

    const val DUCHY_ID: String = "some-duchy-id"
    val DUCHY_AUTH_PROVIDER = { DuchyAuth(DUCHY_ID) }
  }

  private val requisitionStorage = FakeRequisitionStorage()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { channel ->
    listOf(
      requisitionStorage,
      RequisitionService(
        RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub(channel),
        DUCHY_AUTH_PROVIDER
      )
    )
  }

  private val stub: RequisitionCoroutineStub by lazy {
    RequisitionCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun fulfillMetricRequisition() = runBlocking<Unit> {
    requisitionStorage.mocker.mock(RequisitionStorageCoroutineImplBase::fulfillRequisition) {
      REQUISITION
    }

    val request = FulfillMetricRequisitionRequest.newBuilder().apply {
      keyBuilder.apply {
        dataProviderId = ExternalId(REQUISITION.externalDataProviderId).apiId.value
        campaignId = ExternalId(REQUISITION.externalCampaignId).apiId.value
        metricRequisitionId = ExternalId(REQUISITION.externalRequisitionId).apiId.value
      }
    }.build()

    val result = stub.fulfillMetricRequisition(request)

    val expected = MetricRequisition.newBuilder().apply {
      key = REQUISITION_API_KEY
      state = MetricRequisition.State.FULFILLED
    }.build()

    assertThat(result).isEqualTo(expected)

    assertThat(requisitionStorage.mocker.callsForMethod("fulfillRequisition"))
      .containsExactly(
        FulfillRequisitionRequest.newBuilder()
          .setExternalRequisitionId(REQUISITION.externalRequisitionId)
          .setDuchyId(DUCHY_ID)
          .build()
      )
  }

  @Test
  fun `listMetricRequisitions without page token`() = runBlocking<Unit> {
    requisitionStorage
      .mocker
      .mockStreaming(
        RequisitionStorageCoroutineImplBase::streamRequisitions
      ) {
        flowOf(REQUISITION, REQUISITION)
      }

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

    val result = stub.listMetricRequisitions(request)

    val expected = ListMetricRequisitionsResponse.newBuilder().apply {
      addMetricRequisitionsBuilder().apply {
        key = REQUISITION_API_KEY
        state = MetricRequisition.State.FULFILLED
      }
      addMetricRequisitionsBuilder().apply {
        key = REQUISITION_API_KEY
        state = MetricRequisition.State.FULFILLED
      }
      nextPageToken = CREATE_TIME.toByteArray().base64UrlEncode()
    }.build()

    assertThat(result)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(expected)

    assertThat(requisitionStorage.mocker.callsForMethod("streamRequisitions"))
      .ignoringRepeatedFieldOrder()
      .containsExactly(
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
    requisitionStorage
      .mocker
      .mockStreaming(RequisitionStorageCoroutineImplBase::streamRequisitions) { emptyFlow() }

    val request = ListMetricRequisitionsRequest.newBuilder().apply {
      parentBuilder.apply {
        dataProviderId = ExternalId(REQUISITION.externalDataProviderId).apiId.value
        campaignId = ExternalId(REQUISITION.externalCampaignId).apiId.value
      }
      filterBuilder.addStates(MetricRequisition.State.UNFULFILLED)
      pageSize = 1
      pageToken = CREATE_TIME.toByteArray().base64UrlEncode()
    }.build()

    val result = stub.listMetricRequisitions(request)
    val expected = ListMetricRequisitionsResponse.getDefaultInstance()

    assertThat(result).isEqualTo(expected)

    assertThat(requisitionStorage.mocker.callsForMethod("streamRequisitions"))
      .ignoringRepeatedFieldOrder()
      .containsExactly(
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
}
