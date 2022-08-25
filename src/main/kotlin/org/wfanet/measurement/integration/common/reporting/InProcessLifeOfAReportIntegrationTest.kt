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

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.integration.common.reporting.identity.withPrincipalName
import org.wfanet.measurement.loadtest.reporting.ReportingClient
import org.wfanet.measurement.reporting.deploy.common.server.ReportingDataServer
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineStub

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAReportIntegrationTest {
  abstract val reportingServerDataServices: ReportingDataServer.Services

  @get:Rule
  val reportingServer: InProcessReportingServer by lazy {
    InProcessReportingServer(
      reportingServerDataServices = reportingServerDataServices,
      verboseGrpcLogging = false,
    )
  }

  private val publicEventGroupsClient by lazy {
    EventGroupsCoroutineStub(reportingServer.publicApiChannel)
  }
  private val publicReportingSetsClient by lazy {
    ReportingSetsCoroutineStub(reportingServer.publicApiChannel)
  }
  private val publicReportsClient by lazy { ReportsCoroutineStub(reportingServer.publicApiChannel) }

  private val reportingClient: ReportingClient by lazy {
    ReportingClient(
      InProcessReportingServer.MEASUREMENT_CONSUMER_NAME,
      publicEventGroupsClient.withPrincipalName(InProcessReportingServer.MEASUREMENT_CONSUMER_NAME),
      publicReportingSetsClient.withPrincipalName(
        InProcessReportingServer.MEASUREMENT_CONSUMER_NAME
      ),
      publicReportsClient.withPrincipalName(InProcessReportingServer.MEASUREMENT_CONSUMER_NAME)
    )
  }

  @Test
  fun `create multiple Reports and check the results match the expected results`() = runBlocking {
    reportingClient.createReportingSet("1")
    reportingClient.createReportingSet("2")
    reportingClient.createReportingSet("3")
    for (i in 1..10) {
      launch { reportingClient.execute("$i") }
    }
  }
}
