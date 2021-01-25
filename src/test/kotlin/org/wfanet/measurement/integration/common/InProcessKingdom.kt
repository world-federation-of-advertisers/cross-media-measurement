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

package org.wfanet.measurement.integration.common

import io.grpc.Channel
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.withDuchyIdentities
import org.wfanet.measurement.common.testing.CloseableResource
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.launchAsAutoCloseable
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.duchy.testing.DUCHY_PUBLIC_KEYS
import org.wfanet.measurement.internal.MetricDefinition
import org.wfanet.measurement.internal.SketchMetricDefinition
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedulesGrpcKt.ReportConfigSchedulesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportConfigsGrpcKt.ReportConfigsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportLogEntriesGrpcKt.ReportLogEntriesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.kingdom.daemon.Daemon
import org.wfanet.measurement.kingdom.daemon.DaemonDatabaseServicesClientImpl
import org.wfanet.measurement.kingdom.daemon.runReportMaker
import org.wfanet.measurement.kingdom.daemon.runReportStarter
import org.wfanet.measurement.kingdom.daemon.runRequisitionLinker
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase
import org.wfanet.measurement.kingdom.service.api.v1alpha.RequisitionService
import org.wfanet.measurement.kingdom.service.internal.buildDataServices
import org.wfanet.measurement.kingdom.service.system.v1alpha.GlobalComputationService
import org.wfanet.measurement.kingdom.service.system.v1alpha.RequisitionService as SystemRequisitionService

/**
 * TestRule that starts and stops all Kingdom gRPC services and daemons.
 *
 * @param kingdomRelationalDatabaseProvider called exactly once to produce KingdomRelationalDatabase
 */
class InProcessKingdom(
  verboseGrpcLogging: Boolean = true,
  kingdomRelationalDatabaseProvider: () -> KingdomRelationalDatabase
) : TestRule {
  private val kingdomRelationalDatabase by lazy { kingdomRelationalDatabaseProvider() }

  private val databaseServices = GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
    logger.info("Building Kingdom's internal services")
    for (service in buildDataServices(kingdomRelationalDatabase)) {
      addService(service.withVerboseLogging(verboseGrpcLogging))
    }
  }

  private val kingdomApiServices = GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
    logger.info("Building Kingdom's public API services")
    val reportsClient = ReportsCoroutineStub(databaseServices.channel)
    val reportLogEntriesClient = ReportLogEntriesCoroutineStub(databaseServices.channel)
    val requisitionsClient = RequisitionsCoroutineStub(databaseServices.channel)

    addService(
      GlobalComputationService(reportsClient, reportLogEntriesClient)
        .withDuchyIdentities()
        .withVerboseLogging(verboseGrpcLogging)
    )
    addService(
      RequisitionService(requisitionsClient)
        .withDuchyIdentities()
        .withVerboseLogging(verboseGrpcLogging)
    )
    addService(
      SystemRequisitionService(requisitionsClient)
        .withDuchyIdentities()
        .withVerboseLogging(verboseGrpcLogging)
    )
  }

  private val daemonRunner = CloseableResource {
    GlobalScope.launchAsAutoCloseable {
      logger.info("Launching Kingdom's daemons")
      val exceptionHandler = CoroutineExceptionHandler { context, exception ->
        val name = context[CoroutineName.Key]
        if (exception is CancellationException) {
          logger.warning("Daemon $name cancelled")
        } else {
          logger.warning("Daemon $name exception: $exception")
        }
      }
      supervisorScope {
        val reportConfigStorage = ReportConfigsCoroutineStub(databaseServices.channel)
        val reportConfigScheduleStorage =
          ReportConfigSchedulesCoroutineStub(databaseServices.channel)
        val reportStorage = ReportsCoroutineStub(databaseServices.channel)
        val requisitionStorage = RequisitionsCoroutineStub(databaseServices.channel)
        val daemonThrottler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(200))
        val daemonDatabaseServicesClient = DaemonDatabaseServicesClientImpl(
          reportConfigStorage,
          reportConfigScheduleStorage,
          reportStorage,
          requisitionStorage
        )
        val daemon = Daemon(daemonThrottler, 1, daemonDatabaseServicesClient)
        launch(exceptionHandler + CoroutineName("RequisitionLinker")) {
          daemon.runRequisitionLinker()
        }
        launch(exceptionHandler + CoroutineName("ReportStarter")) {
          daemon.runReportStarter()
        }
        launch(exceptionHandler + CoroutineName("ReportMaker")) {
          daemon.runReportMaker(DUCHY_PUBLIC_KEYS.latest.combinedPublicKeyId)
        }
      }
    }
  }

  /**
   * Provides a gRPC channel to the Kingdom's public APIs.
   */
  val publicApiChannel: Channel
    get() = kingdomApiServices.channel

  override fun apply(statement: Statement, description: Description): Statement {
    return chainRulesSequentially(databaseServices, kingdomApiServices, daemonRunner)
      .apply(statement, description)
  }

  data class SetupIdentifiers(
    val externalDataProviderIds: List<ExternalId>,
    val externalCampaignIds: List<ExternalId>
  )

  /**
   * Adds an Advertiser, two DataProviders, two Campaigns, a ReportConfig, and a
   * ReportConfigSchedule to [kingdomRelationalDatabase].
   */
  fun populateKingdomRelationalDatabase(): SetupIdentifiers = runBlocking {
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

    val campaign3 = kingdomRelationalDatabase.createCampaign(
      externalDataProviderId2,
      externalAdvertiserId,
      "Yet Another Campaign"
    )
    logger.info("Created a Campaign: $campaign3")

    val externalCampaignId1 = ExternalId(campaign1.externalCampaignId)
    val externalCampaignId2 = ExternalId(campaign2.externalCampaignId)
    val externalCampaignId3 = ExternalId(campaign3.externalCampaignId)

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
          numRequisitions = 3
          reportConfigDetailsBuilder.apply {
            addMetricDefinitions(metricDefinition)
            reportDurationBuilder.apply {
              unit = TimePeriod.Unit.DAY
              count = 7
            }
          }
        }
        .build(),
      listOf(externalCampaignId1, externalCampaignId2, externalCampaignId3)
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

    SetupIdentifiers(
      listOf(externalDataProviderId1, externalDataProviderId2),
      listOf(externalCampaignId1, externalCampaignId2, externalCampaignId3)
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
