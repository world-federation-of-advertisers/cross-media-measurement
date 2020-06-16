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
import org.wfanet.measurement.api.v1alpha.FulfillMetricsRequisitionRequest
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
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.service.internal.kingdom.testing.FakeRequisitionStorage
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class RequisitionServiceTest {

  companion object {
    var CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
    var WINDOW_START_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()
    var WINDOW_END_TIME: Timestamp = Instant.ofEpochSecond(789).toProtoTime()

    var IRRELEVANT_DETAILS: RequisitionDetails = RequisitionDetails.getDefaultInstance()

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
  }

  private val requisitionStorage = FakeRequisitionStorage()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { channel ->
    listOf(
      requisitionStorage,
      RequisitionService(
        RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub(channel)
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

    val request = FulfillMetricsRequisitionRequest.newBuilder().apply {
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
