package org.wfanet.measurement.kingdom

import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.RuleChain
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportLogEntryStorageGrpcKt.ReportLogEntryStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub
import org.wfanet.measurement.service.internal.kingdom.buildStorageServices
import org.wfanet.measurement.service.testing.GrpcTestServerRule
import org.wfanet.measurement.service.v1alpha.common.DuchyAuth
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
  private val duchyIdProvider = { DuchyAuth(duchyId) }

  private val databaseServices = GrpcTestServerRule {
    buildStorageServices(kingdomRelationalDatabase)
  }

  private val reportConfigStorage = ReportConfigStorageCoroutineStub(databaseServices.channel)
  private val reportConfigScheduleStorage =
    ReportConfigScheduleStorageCoroutineStub(databaseServices.channel)
  private val reportStorage = ReportStorageCoroutineStub(databaseServices.channel)
  private val reportLogEntryStorage = ReportLogEntryStorageCoroutineStub(databaseServices.channel)
  private val requisitionStorage = RequisitionStorageCoroutineStub(databaseServices.channel)

  private val apiServices = GrpcTestServerRule {
    listOf(
      GlobalComputationService(reportStorage, reportLogEntryStorage, duchyIdProvider),
      RequisitionService(requisitionStorage)
    )
  }

  @get:Rule
  val ruleChain: TestRule by lazy {
    rules
      .fold(RuleChain.emptyRuleChain()) { chain, rule -> chain.around(rule) }
      .around(databaseServices)
      .around(apiServices)
  }

  private val requisitionsApi = RequisitionCoroutineStub(apiServices.channel)
  private val globalComputationsApi = GlobalComputationsCoroutineStub(apiServices.channel)

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
    // Ensure that everything loads up properly.
    assert(true)
  }

  @Test
  fun `entire computation`() {
    // TODO: add APIs to kingdomRelationalDatabase so this test case can insert Advertisers,
    // DataProviders, ReportConfigs, etc.

    // 1. Insert an advertiser
    // 2. Insert some data providers
    // 3. Insert some campaigns
    // 4. Create a ReportConfig
    // 5. Create a ReportConfigSchedule
    // 6. Wait until Requisitions are created and mark those as fulfilled.
    // 7. Wait until a GlobalComputation is ready to be confirmed and confirm it.
    // 8. Wait until the GlobalComputation is ready to be started and then report that it's done.
    // 9. Ensure that the GlobalComputation is done.
  }
}
