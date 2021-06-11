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
import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.withDuchyInfo
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.MetricDefinition
import org.wfanet.measurement.internal.SketchMetricDefinition
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportLogEntriesGrpcKt.ReportLogEntriesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.kingdom.db.testing.KingdomDatabases
import org.wfanet.measurement.kingdom.service.api.v1alpha.RequisitionService
import org.wfanet.measurement.kingdom.service.internal.buildLegacyDataServices
import org.wfanet.measurement.kingdom.service.system.v1alpha.GlobalComputationService
import org.wfanet.measurement.kingdom.service.system.v1alpha.RequisitionService as SystemRequisitionService

/**
 * TestRule that starts and stops all Kingdom gRPC services and daemons.
 *
 * @param verboseGrpcLogging whether to log all gRPCs
 * @param databasesProvider called once to get the Kingdom's database wrappers
 */
class InProcessKingdom(
  verboseGrpcLogging: Boolean = true,
  databasesProvider: () -> KingdomDatabases
) : TestRule {
  private val databases by lazy { databasesProvider() }

  private val databaseServices =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Kingdom's internal services")
      val services =
        buildLegacyDataServices(databases.reportDatabase, databases.requisitionDatabase)
      for (service in services) {
        addService(service.withVerboseLogging(verboseGrpcLogging))
      }
    }

  private val kingdomApiServices =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Kingdom's public API services")
      val reportsClient = ReportsCoroutineStub(databaseServices.channel)
      val reportLogEntriesClient = ReportLogEntriesCoroutineStub(databaseServices.channel)
      val requisitionsClient = RequisitionsCoroutineStub(databaseServices.channel)

      addService(
        GlobalComputationService(reportsClient, reportLogEntriesClient)
          .withDuchyInfo()
          .withVerboseLogging(verboseGrpcLogging)
      )
      addService(
        RequisitionService(requisitionsClient)
          .withDuchyInfo()
          .withVerboseLogging(verboseGrpcLogging)
      )
      addService(
        SystemRequisitionService(requisitionsClient)
          .withDuchyInfo()
          .withVerboseLogging(verboseGrpcLogging)
      )
    }

  /** Provides a gRPC channel to the Kingdom's public APIs. */
  val publicApiChannel: Channel
    get() = kingdomApiServices.channel

  override fun apply(statement: Statement, description: Description): Statement {
    return chainRulesSequentially(databaseServices, kingdomApiServices)
      .apply(statement, description)
  }

  data class SetupIdentifiers(
    val externalDataProviderIds: List<ExternalId>,
    val externalCampaignIds: List<ExternalId>
  )

  /**
   * Adds an Advertiser, two DataProviders, two Campaigns, a ReportConfig, and a
   * ReportConfigSchedule to the database.
   */
  fun populateDatabases(): SetupIdentifiers = runBlocking {
    val databaseTestHelper = databases.databaseTestHelper

    val advertiser = databaseTestHelper.createAdvertiser()
    logger.info("Created an Advertiser: $advertiser")

    val dataProvider1 = databaseTestHelper.createDataProvider()
    logger.info("Created a DataProvider: $dataProvider1")

    val dataProvider2 = databaseTestHelper.createDataProvider()
    logger.info("Created a DataProvider: $dataProvider2")

    val externalAdvertiserId = ExternalId(advertiser.externalAdvertiserId)
    val externalDataProviderId1 = ExternalId(dataProvider1.externalDataProviderId)
    val externalDataProviderId2 = ExternalId(dataProvider2.externalDataProviderId)

    val campaign1 =
      databaseTestHelper.createCampaign(
        externalDataProviderId1,
        externalAdvertiserId,
        "Springtime Sale Campaign"
      )
    logger.info("Created a Campaign: $campaign1")

    val campaign2 =
      databaseTestHelper.createCampaign(
        externalDataProviderId2,
        externalAdvertiserId,
        "Summer Savings Campaign"
      )
    logger.info("Created a Campaign: $campaign2")

    val campaign3 =
      databaseTestHelper.createCampaign(
        externalDataProviderId2,
        externalAdvertiserId,
        "Yet Another Campaign"
      )
    logger.info("Created a Campaign: $campaign3")

    val externalCampaignId1 = ExternalId(campaign1.externalCampaignId)
    val externalCampaignId2 = ExternalId(campaign2.externalCampaignId)
    val externalCampaignId3 = ExternalId(campaign3.externalCampaignId)

    val metricDefinition =
      MetricDefinition.newBuilder()
        .apply {
          sketchBuilder.apply {
            sketchConfigId = 12345L
            type = SketchMetricDefinition.Type.IMPRESSION_REACH_AND_FREQUENCY
          }
        }
        .build()

    val reportConfig =
      databaseTestHelper.createReportConfig(
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

    val schedule =
      databaseTestHelper.createSchedule(
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
