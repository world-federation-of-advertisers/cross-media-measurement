// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common.reporting

import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.integration.common.InProcessDuchyConfig
import org.wfanet.measurement.integration.common.InProcessKingdomResourcesSetup
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.reporting.MeasurementConsumerData
import org.wfanet.measurement.loadtest.reporting.ReportingSimulator
import org.wfanet.measurement.reporting.deploy.common.service.DataServices as ReportingServerDataServices
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.storage.StorageClient

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAReportIntegrationTest {
  abstract val reportingServerDataServices: ReportingServerDataServices
  abstract val kingdomDataServicesRule: ProviderRule<DataServices>

  /** Provides a function from Duchy to the dependencies needed to start the Duchy to the test. */
  abstract val duchyDependenciesRule: ProviderRule<(String) -> InProcessDuchy.DuchyDependencies>

  abstract val storageClient: StorageClient

  private val resourcesSetup: InProcessKingdomResourcesSetup by lazy {
    InProcessKingdomResourcesSetup(kingdomDataServicesRule, duchyDependenciesRule, storageClient)
  }

  private val reportingServer: InProcessReportingServer by lazy {
    InProcessReportingServer(
      kingdomPublicApiChannel = resourcesSetup.kingdom.publicApiChannel,
      dataServicesProvider = { reportingServerDataServices },
      verboseGrpcLogging = false
    )
  }

  private val publicEventGroupsClient by lazy {
    EventGroupsCoroutineStub(reportingServer.publicApiChannel)
  }
  private val publicReportingSetsClient by lazy {
    ReportingSetsCoroutineStub(reportingServer.publicApiChannel)
  }
  private val publicReportsClient by lazy { ReportsCoroutineStub(reportingServer.publicApiChannel) }

  private val reportingSimulator: ReportingSimulator by lazy {
    ReportingSimulator(
      MeasurementConsumerData(resourcesSetup.mcResourceName),
      publicEventGroupsClient.withPrincipalName(resourcesSetup.mcResourceName),
      publicReportingSetsClient.withPrincipalName(resourcesSetup.mcResourceName),
      publicReportsClient.withPrincipalName(resourcesSetup.mcResourceName)
    )
  }

  @get:Rule
  val ruleChain: TestRule by lazy { chainRulesSequentially(resourcesSetup, reportingServer) }

  @Test
  fun `create a Report and check the result csv is created`() =
    runBlocking {
      // Use reporting server simulator to check event groups, create a reporting set, create a
      // report with the reporting set, and check the report result csv.
      // reportingSimulator.execute("1234")
    }

  companion object {
    @ClassRule @JvmField val duchyConfig = InProcessDuchyConfig()
  }
}
