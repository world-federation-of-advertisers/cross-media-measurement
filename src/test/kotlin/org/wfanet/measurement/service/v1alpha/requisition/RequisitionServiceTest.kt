package org.wfanet.measurement.service.v1alpha.requisition

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v1alpha.Campaign
import org.wfanet.measurement.api.v1alpha.CreateMetricRequisitionRequest
import org.wfanet.measurement.api.v1alpha.FulfillMetricsRequisitionRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsResponse
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.RequisitionGrpc
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.db.kingdom.CampaignExternalKey
import org.wfanet.measurement.db.kingdom.MeasurementProviderStorage
import org.wfanet.measurement.db.kingdom.RequisitionExternalKey
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState

@RunWith(JUnit4::class)
class RequisitionServiceTest {
  @get:Rule
  val grpcCleanup = GrpcCleanupRule()

  private lateinit var blockingStub: RequisitionGrpc.RequisitionBlockingStub

  companion object {
    private val DATA_PROVIDER_ID = ExternalId(1)
    private val CAMPAIGN_ID = ExternalId(2)
    val REQUISITION_ID = ExternalId(3)

    val CAMPAIGN_API_KEY: Campaign.Key =
      Campaign.Key.newBuilder().apply {
        dataProviderId = DATA_PROVIDER_ID.apiId.value
        campaignId = CAMPAIGN_ID.apiId.value
      }.build()

    val REQUISITION_API_KEY: MetricRequisition.Key =
      MetricRequisition.Key.newBuilder().apply {
        dataProviderId = DATA_PROVIDER_ID.apiId.value
        campaignId = CAMPAIGN_ID.apiId.value
        metricRequisitionId = REQUISITION_ID.apiId.value
      }.build()

    var IRRELEVANT_INSTANT: Instant = Instant.EPOCH
    var WINDOW_START_TIME: Instant = Instant.ofEpochSecond(123)
    var WINDOW_END_TIME: Instant = Instant.ofEpochSecond(456)

    var IRRELEVANT_DETAILS: RequisitionDetails = RequisitionDetails.getDefaultInstance()
  }
  object FakeMeasurementProviderStorage :
    MeasurementProviderStorage {
    private fun RequisitionExternalKey.toRequisitionBuilder(): Requisition.Builder =
      Requisition.newBuilder().apply {
        externalDataProviderId = this@toRequisitionBuilder.dataProviderExternalId.value
        externalCampaignId = this@toRequisitionBuilder.campaignExternalId.value
        externalRequisitionId = this@toRequisitionBuilder.externalId.value
      }

    private fun makeRequisition(
      key: RequisitionExternalKey,
      requisitionDetails: RequisitionDetails,
      windowStartTime: Instant,
      windowEndTime: Instant,
      state: RequisitionState
    ): Requisition {
      return key.toRequisitionBuilder()
        .setRequisitionDetails(requisitionDetails)
        .setWindowStartTime(windowStartTime.toProtoTime())
        .setWindowEndTime(windowEndTime.toProtoTime())
        .setState(state)
        .build()
    }

    private fun makeRequisitionWithState(
      key: RequisitionExternalKey,
      state: RequisitionState
    ): Requisition =
      makeRequisition(key, IRRELEVANT_DETAILS, IRRELEVANT_INSTANT, IRRELEVANT_INSTANT, state)

    override suspend fun createRequisition(
      campaignExternalKey: CampaignExternalKey,
      requisitionDetails: RequisitionDetails,
      windowStartTime: Instant,
      windowEndTime: Instant
    ): Requisition {
      require(windowStartTime == WINDOW_START_TIME)
      require(windowEndTime == WINDOW_END_TIME)
      return makeRequisition(
        RequisitionExternalKey(
          campaignExternalKey,
          REQUISITION_ID
        ),
        requisitionDetails, windowStartTime, windowEndTime,
        RequisitionState.UNFULFILLED
      )
    }

    override suspend fun fulfillRequisition(
      requisitionExternalKey: RequisitionExternalKey
    ): Requisition =
      makeRequisition(
        requisitionExternalKey,
        IRRELEVANT_DETAILS,
        IRRELEVANT_INSTANT,
        IRRELEVANT_INSTANT,
        RequisitionState.FULFILLED
      )

    override suspend fun listRequisitions(
      campaignExternalKey: CampaignExternalKey,
      states: Set<RequisitionState>,
      pagination: Pagination
    ): List<Requisition> {
      require(pagination == Pagination(2, "some-page-token"))
      require(states == setOf(RequisitionState.FULFILLED, RequisitionState.UNFULFILLED))
      val key =
        RequisitionExternalKey(
          campaignExternalKey,
          REQUISITION_ID
        )
      return listOf(
        makeRequisitionWithState(key, RequisitionState.UNFULFILLED),
        makeRequisitionWithState(key, RequisitionState.FULFILLED)
      )
    }
  }

  @Before
  fun setup() {
    val serverName = InProcessServerBuilder.generateName()
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(RequisitionService(FakeMeasurementProviderStorage))
        .build()
        .start()
    )

    val channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
    blockingStub = RequisitionGrpc.newBlockingStub(grpcCleanup.register(channel))
  }

  @Test
  fun createMetricRequisition() = runBlocking {
    val request = CreateMetricRequisitionRequest.newBuilder().apply {
      parent = CAMPAIGN_API_KEY
      metricsRequisitionBuilder.apply {
        collectionIntervalBuilder.apply {
          startTime = WINDOW_START_TIME.toProtoTime()
          endTime = WINDOW_END_TIME.toProtoTime()
        }
        metricDefinitionBuilder.apply {
          // TODO: add a definition
        }
      }
    }.build()

    val result = blockingStub.createMetricRequisition(request)

    val expected = MetricRequisition.newBuilder().apply {
      key = REQUISITION_API_KEY
      state = MetricRequisition.State.UNFULFILLED
    }.build()

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun fulfillMetricRequisition() = runBlocking {
    val request = FulfillMetricsRequisitionRequest.newBuilder().apply {
      key = REQUISITION_API_KEY
    }.build()

    val result = blockingStub.fulfillMetricRequisition(request)

    val expected = MetricRequisition.newBuilder().apply {
      key = REQUISITION_API_KEY
      state = MetricRequisition.State.FULFILLED
    }.build()

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun listMetricRequisitions() = runBlocking {
    val request = ListMetricRequisitionsRequest.newBuilder().apply {
      parent = CAMPAIGN_API_KEY
      filterBuilder.addAllStates(
        listOf(
          MetricRequisition.State.UNFULFILLED,
          MetricRequisition.State.FULFILLED
        )
      )
      pageSize = 2
      pageToken = "some-page-token"
    }.build()

    val result = blockingStub.listMetricRequisitions(request)

    val expected = ListMetricRequisitionsResponse.newBuilder().apply {
      addMetricRequisitionsBuilder().apply {
        key = REQUISITION_API_KEY
        state = MetricRequisition.State.FULFILLED
      }
      addMetricRequisitionsBuilder().apply {
        key = REQUISITION_API_KEY
        state = MetricRequisition.State.UNFULFILLED
      }
    }.build()

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }
}
