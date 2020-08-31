package org.wfanet.measurement.integration

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.FinishGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.FulfillMetricRequisitionRequest
import org.wfanet.measurement.api.v1alpha.GetGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.GlobalComputation
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsRequest
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.testing.CloseableResource
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.launchAsAutoCloseable
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.internal.MetricDefinition
import org.wfanet.measurement.internal.SketchMetricDefinition
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.TimePeriod

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of KingdomRelationalDatabase can all run the
 * same tests easily.
 */
abstract class InProcessKingdomIntegrationTest {
  abstract val kingdomRelationalDatabaseRule: ProviderRule<KingdomRelationalDatabase>

  private val kingdomRelationalDatabase: KingdomRelationalDatabase
    get() = kingdomRelationalDatabaseRule.value

  private var duchyId: String = "some-duchy"

  private val kingdom = InProcessKingdom { kingdomRelationalDatabase }

  private val globalComputations = mutableListOf<GlobalComputation>()
  private val globalComputationsReader = CloseableResource {
    GlobalScope.launchAsAutoCloseable {
      var continuationToken = ""
      while (true) {
        val request =
          StreamActiveGlobalComputationsRequest.newBuilder()
            .setContinuationToken(continuationToken)
            .build()
        logger.info("Reading global computations: $request")
        globalComputationsStub
          .streamActiveGlobalComputations(request)
          .onEach { continuationToken = it.continuationToken }
          .map { it.globalComputation }
          .onEach { logger.info("Found GlobalComputation: $it") }
          .toList(globalComputations)
      }
    }
  }

  @get:Rule
  val ruleChain: TestRule by lazy {
    chainRulesSequentially(
      DuchyIdSetter(duchyId), kingdomRelationalDatabaseRule, kingdom, globalComputationsReader
    )
  }

  private val requisitionsStub by lazy {
    RequisitionCoroutineStub(kingdom.publicApiChannel).withDuchyId(duchyId)
  }

  private val globalComputationsStub by lazy {
    GlobalComputationsCoroutineStub(kingdom.publicApiChannel).withDuchyId(duchyId)
  }

  @Test
  fun `entire computation`() = runBlocking<Unit> {
    val advertiser = kingdomRelationalDatabase.createAdvertiser()
    logger.info("Created an Advertiser: $advertiser")

    val dataProvider1 = kingdomRelationalDatabase.createDataProvider()
    logger.info("Created a DataProvider: $dataProvider1")

    val dataProvider2 = kingdomRelationalDatabase.createDataProvider()
    logger.info("Created a DataProvider: $dataProvider2")

    val externalAdvertiserId = ExternalId(advertiser.externalAdvertiserId)
    val externalDataProviderId1 = ExternalId(dataProvider1.externalDataProviderId)
    val externalDataProviderId2 = ExternalId(dataProvider2.externalDataProviderId)

    val campaign1 = kingdomRelationalDatabase.createCampaign(
      externalDataProviderId1,
      externalAdvertiserId,
      "Springtime Sale Campaign"
    )
    logger.info("Created a Campaign: $campaign1")

    val campaign2 = kingdomRelationalDatabase.createCampaign(
      externalDataProviderId2,
      externalAdvertiserId,
      "Summer Savings Campaign"
    )
    logger.info("Created a Campaign: $campaign2")

    val externalCampaignId1 = ExternalId(campaign1.externalCampaignId)
    val externalCampaignId2 = ExternalId(campaign2.externalCampaignId)

    val metricDefinition = MetricDefinition.newBuilder().apply {
      sketchBuilder.apply {
        sketchConfigId = 12345L
        type = SketchMetricDefinition.Type.IMPRESSION_REACH_AND_FREQUENCY
      }
    }.build()

    val reportConfig = kingdomRelationalDatabase.createReportConfig(
      ReportConfig.newBuilder()
        .setExternalAdvertiserId(externalAdvertiserId.value)
        .apply {
          numRequisitions = 2
          reportConfigDetailsBuilder.apply {
            addMetricDefinitions(metricDefinition)
            reportDurationBuilder.apply {
              unit = TimePeriod.Unit.DAY
              count = 7
            }
          }
        }
        .build(),
      listOf(externalCampaignId1, externalCampaignId2)
    )
    logger.info("Created a ReportConfig: $reportConfig")

    val externalReportConfigId = ExternalId(reportConfig.externalReportConfigId)

    val schedule = kingdomRelationalDatabase.createSchedule(
      ReportConfigSchedule.newBuilder()
        .setExternalAdvertiserId(externalAdvertiserId.value)
        .setExternalReportConfigId(externalReportConfigId.value)
        .apply {
          repetitionSpecBuilder.apply {
            start = Instant.now().toProtoTime()
            repetitionPeriodBuilder.apply {
              unit = TimePeriod.Unit.DAY
              count = 7
            }
          }
          nextReportStartTime = repetitionSpec.start
        }
        .build()
    )
    logger.info("Created a ReportConfigSchedule: $schedule")

    // At this point, the ReportMaker daemon should pick up pick up on the ReportConfigSchedule and
    // create a Report.
    //
    // Next, the RequisitionLinker daemon should create two Requisitions for the Report.

    val requisitions1 = pollForSize(1) {
      readRequisition(externalDataProviderId1, externalCampaignId1)
    }

    val requisitions2 = pollForSize(1) {
      readRequisition(externalDataProviderId2, externalCampaignId2)
    }

    val requisitions = requisitions1 + requisitions2
    logger.info("Requisitions were made: $requisitions")

    requisitions.forEach { fulfillRequisition(it) }

    val expectedMetricRequisition1 = MetricRequisition.newBuilder().apply {
      keyBuilder.apply {
        dataProviderId = externalDataProviderId1.apiId.value
        campaignId = externalCampaignId1.apiId.value
      }
    }.build()

    val expectedMetricRequisition2 = MetricRequisition.newBuilder().apply {
      keyBuilder.apply {
        dataProviderId = externalDataProviderId2.apiId.value
        campaignId = externalCampaignId2.apiId.value
      }
    }.build()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(expectedMetricRequisition1, expectedMetricRequisition2)

    // When the Report is first created, it will be in state AWAITING_REQUISITIONS.
    // After the RequisitionLinker is done, the ReportStarter daemon will transition it to state
    // AWAITING_DUCHY_CONFIRMATION.
    //
    // These states are exposed in GlobalComputation as CREATED and CONFIRMING.
    logger.info("Awaiting first two GlobalComputation messages")
    val firstTwoComputations = pollForSize(2) { globalComputations }
    assertThat(firstTwoComputations)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        GlobalComputation.newBuilder().setState(GlobalComputation.State.CREATED).build(),
        GlobalComputation.newBuilder().setState(GlobalComputation.State.CONFIRMING).build()
      )
      .inOrder()
    val computation = firstTwoComputations.last()

    logger.info("Confirming Duchy readiness")
    globalComputationsStub.confirmGlobalComputation(
      ConfirmGlobalComputationRequest.newBuilder().apply {
        key = computation.key
        addAllReadyRequisitions(requisitions.map { it.key })
      }.build()
    )

    logger.info("Awaiting third GlobalComputation message")
    val startedComputation = pollForSize(3) { globalComputations }.last()

    assertThat(startedComputation)
      .isEqualTo(computation.toBuilder().setState(GlobalComputation.State.RUNNING).build())

    logger.info("Finishing GlobalComputation")
    globalComputationsStub.finishGlobalComputation(
      FinishGlobalComputationRequest.newBuilder().apply {
        key = computation.key
        resultBuilder.apply {
          reach = 12345L
          putFrequency(6L, 7L)
          putFrequency(8L, 9L)
        }
      }.build()
    )

    val finalComputation = globalComputationsStub.getGlobalComputation(
      GetGlobalComputationRequest.newBuilder().setKey(computation.key).build()
    )
    assertThat(finalComputation)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        startedComputation.toBuilder().apply {
          state = GlobalComputation.State.SUCCEEDED
          resultBuilder.apply {
            reach = 12345L
            putFrequency(6L, 7L)
            putFrequency(8L, 9L)
          }
        }.build()
      )
  }

  private suspend fun readRequisition(
    dataProviderId: ExternalId,
    campaignId: ExternalId
  ): List<MetricRequisition> {
    val request = ListMetricRequisitionsRequest.newBuilder().apply {
      parentBuilder.apply {
        this.dataProviderId = dataProviderId.apiId.value
        this.campaignId = campaignId.apiId.value
      }
      pageSize = 1
      filterBuilder.apply {
        addStates(MetricRequisition.State.UNFULFILLED)
        addStates(MetricRequisition.State.FULFILLED)
      }
    }.build()
    logger.info("Listing requisitions: $request")
    val response = requisitionsStub.listMetricRequisitions(request)
    logger.info("Got requisitions: $response")
    return response.metricRequisitionsList
  }

  private suspend fun <T> pollForSize(size: Int, block: suspend () -> List<T>): List<T> {
    var items: List<T> = emptyList()
    withTimeout(3_000) {
      while (items.size < size) {
        delay(250)
        items = block()
      }
    }
    return items
  }

  private suspend fun fulfillRequisition(metricRequisition: MetricRequisition) {
    logger.info("Fulfilling requisition: $metricRequisition")
    requisitionsStub.fulfillMetricRequisition(
      FulfillMetricRequisitionRequest.newBuilder().apply {
        key = metricRequisition.key
      }.build()
    )
  }

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
