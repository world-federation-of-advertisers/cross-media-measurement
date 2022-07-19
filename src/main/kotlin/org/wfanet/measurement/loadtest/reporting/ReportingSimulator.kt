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

package org.wfanet.measurement.loadtest.reporting

import java.util.logging.Logger
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.reportingSet

data class MeasurementConsumerData(
  // The MC's public API resource name
  val name: String
)

/** A simulator performing reporting operations. */
class ReportingSimulator(
  private val measurementConsumerData: MeasurementConsumerData,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val reportingSetsClient: ReportingSetsCoroutineStub,
  private val reportsClient: ReportsCoroutineStub,
  /** Map of event template names to filter expressions. */
  private val eventTemplateFilters: Map<String, String> = emptyMap()
) {
  /** A sequence of operations done in the simulator involving a report. */
  suspend fun execute(runId: String) {
    createReportingSet(runId)
  }

  private suspend fun createReportingSet(runId: String) {
    reportingSetsClient.createReportingSet(
      createReportingSetRequest {
        parent = measurementConsumerData.name
        reportingSet = reportingSet { displayName = "$runId - 1" }
      }
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
