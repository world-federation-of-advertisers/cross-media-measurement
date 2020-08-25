package org.wfanet.measurement.kingdom

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.rules.RuleChain
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v1alpha.FulfillMetricRequisitionRequest
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.identity.withDuchyIdentities
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.Campaign
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportLogEntryStorageGrpcKt.ReportLogEntryStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub
import org.wfanet.measurement.service.internal.kingdom.buildStorageServices
import org.wfanet.measurement.service.testing.GrpcTestServerRule
import org.wfanet.measurement.service.v1alpha.globalcomputation.GlobalComputationService
import org.wfanet.measurement.service.v1alpha.requisition.RequisitionService

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of KingdomRelationalDatabase can all run the
 * same tests easily.
 */
abstract class InProcessKingdomIntegrationTest {
  abstract val kingdomRelationalDatabase: KingdomRelationalDatabase
  abstract val rules: List<TestRule>

  private var duchyId: String = "some-duchy"
  private val duchyIdProvider = { DuchyIdentity(duchyId) }

  private val databaseServices = GrpcTestServerRule {
    buildStorageServices(kingdomRelationalDatabase).forEach(this::addService)
  }

  private val reportConfigStorage = ReportConfigStorageCoroutineStub(databaseServices.channel)
  private val reportConfigScheduleStorage =
    ReportConfigScheduleStorageCoroutineStub(databaseServices.channel)
  private val reportStorage = ReportStorageCoroutineStub(databaseServices.channel)
  private val reportLogEntryStorage = ReportLogEntryStorageCoroutineStub(databaseServices.channel)
  private val requisitionStorage = RequisitionStorageCoroutineStub(databaseServices.channel)

  private val apiServices = GrpcTestServerRule {
    addService(
      GlobalComputationService(reportStorage, reportLogEntryStorage, duchyIdProvider)
        .withDuchyIdentities()
    )

    addService(RequisitionService(requisitionStorage).withDuchyIdentities())
  }

  @get:Rule
  val ruleChain: TestRule by lazy {
    rules
      .fold(RuleChain.emptyRuleChain()) { chain, rule -> chain.around(rule) }
      .around(databaseServices)
      .around(apiServices)
      .around(DuchyIdSetter(duchyId))
  }

  private val requisitionsStub =
    RequisitionCoroutineStub(apiServices.channel).withDuchyId(duchyId)
  private val globalComputationsStub =
    GlobalComputationsCoroutineStub(apiServices.channel).withDuchyId(duchyId)

  private val daemonThrottler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1))
  private val daemonDatabaseServicesClient = DaemonDatabaseServicesClientImpl(
    reportConfigStorage, reportConfigScheduleStorage, reportStorage, requisitionStorage
  )

  private val daemon = Daemon(daemonThrottler, 1, daemonDatabaseServicesClient)

  private lateinit var daemonJobs: Job

  @Before
  fun startDaemons() {
    daemonJobs = GlobalScope.launch {
      val exceptionHandler = CoroutineExceptionHandler { _, exception ->
        System.err.println("Daemon exception: $exception")
      }
      supervisorScope {
        launch(exceptionHandler) { daemon.runRequisitionLinker() }
        launch(exceptionHandler) { daemon.runReportStarter() }
        launch(exceptionHandler) { daemon.runReportMaker() }
      }
    }
  }

  @After
  fun cancelDaemons() = runBlocking { daemonJobs.cancelAndJoin() }

  @Test
  fun noop() {
    // Ensure @Before/@After work.
  }

  @Ignore // TODO: remove this @Ignore
  @Test
  fun `entire computation`() = runBlocking {
    val advertiser = kingdomRelationalDatabase.createAdvertiser()
    val dataProvider1 = kingdomRelationalDatabase.createDataProvider()
    val dataProvider2 = kingdomRelationalDatabase.createDataProvider()

    val externalAdvertiserId = ExternalId(advertiser.externalAdvertiserId)
    val externalDataProviderId1 = ExternalId(dataProvider1.externalDataProviderId)
    val externalDataProviderId2 = ExternalId(dataProvider2.externalDataProviderId)

    val campaign1 = kingdomRelationalDatabase.createCampaign(
      externalDataProviderId1,
      externalAdvertiserId,
      "Springtime Sale Campaign"
    )
    assertThat(campaign1)
      .comparingExpectedFieldsOnly()
      .isEqualTo(Campaign.newBuilder().setProvidedCampaignId("Springtime Sale Campaign").build())

    val campaign2 = kingdomRelationalDatabase.createCampaign(
      externalDataProviderId2,
      externalAdvertiserId,
      "Summer Savings Campaign"
    )
    assertThat(campaign2)
      .comparingExpectedFieldsOnly()
      .isEqualTo(Campaign.newBuilder().setProvidedCampaignId("Summer Savings Campaign").build())

    // TODO: add APIs to kingdomRelationalDatabase to insert ReportConfigs and Schedules, and do so.

    val requisitions = waitForRequisitions(2)

    for (requisition in requisitions) {
      requisitionsStub.fulfillMetricRequisition(
        FulfillMetricRequisitionRequest.newBuilder().apply {
          key = requisition.key
        }.build()
      )
    }

    // TODO: wait for a GlobalComputation to be ready
    // TODO: confirm the GlobalComputation
    // TODO: wait for the GlobalComputation to be marked as started
    // TODO: report that the GlobalComputation is done
    // TODO: ensure that the GlobalComputation is marked as done
  }

  private suspend fun waitForRequisitions(numberOfRequisitions: Int): List<MetricRequisition> =
    withTimeout(3_000) {
      waitForSize(numberOfRequisitions) {
        delay(200)
        readRequisitions(numberOfRequisitions)
      }
    }

  private suspend fun readRequisitions(limit: Int): List<MetricRequisition> {
    val request = ListMetricRequisitionsRequest.newBuilder().setPageSize(limit).build()
    val response = requisitionsStub.listMetricRequisitions(request)
    return response.metricRequisitionsList
  }

  private suspend fun <T> waitForSize(size: Int, block: suspend () -> List<T>): List<T> {
    var items: List<T> = emptyList()
    while (items.size < size) {
      items = block()
    }
    return items
  }
}
