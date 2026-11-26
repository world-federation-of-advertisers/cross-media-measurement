// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.job

import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.cancelMeasurementRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportComputation
import org.wfanet.measurement.internal.reporting.v2.failBasicReportRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey

/** Fails [BasicReport]s on the multimeasurement path without leaving their Measurements running. */
class MultimeasurementBasicReportFailer(
  private val internalBasicReportsStub: BasicReportsCoroutineStub,
  private val measurementsStub: MeasurementsCoroutineStub,
) {
  /**
   * Cancels the CMMS Measurement of each of [reportComputations] that has one, then fails
   * [basicReport].
   *
   * A Measurement that is already terminal or gone counts as cancelled, so a retry after a partial
   * failure completes.
   *
   * @param apiKey the API key of [basicReport]'s MeasurementConsumer
   * @throws Exception if a Measurement could not be cancelled, in which case [basicReport] is left
   *   for a retry, or if [basicReport] could not be failed
   */
  suspend fun fail(
    basicReport: BasicReport,
    reportComputations: List<ReportComputation>,
    apiKey: String,
  ) {
    for (reportComputation in reportComputations) {
      if (reportComputation.cmmsMeasurementId.isNotEmpty()) {
        cancelMeasurement(
          MeasurementKey(basicReport.cmmsMeasurementConsumerId, reportComputation.cmmsMeasurementId)
            .toName(),
          apiKey,
        )
      }
    }

    try {
      internalBasicReportsStub.failBasicReport(
        failBasicReportRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
        }
      )
    } catch (e: StatusException) {
      val name =
        BasicReportKey(basicReport.cmmsMeasurementConsumerId, basicReport.externalBasicReportId)
          .toName()
      throw Exception("Unable to fail $name", e)
    }
  }

  private suspend fun cancelMeasurement(name: String, apiKey: String) {
    try {
      measurementsStub
        .withAuthenticationKey(apiKey)
        .cancelMeasurement(cancelMeasurementRequest { this.name = name })
    } catch (e: StatusException) {
      if (e.status.code !in DONE_CODES) {
        throw Exception("Unable to cancel $name", e)
      }
    }
  }

  companion object {
    /** Codes of a cancellation whose Measurement is already terminal or gone. */
    private val DONE_CODES = setOf(Status.Code.FAILED_PRECONDITION, Status.Code.NOT_FOUND)
  }
}
