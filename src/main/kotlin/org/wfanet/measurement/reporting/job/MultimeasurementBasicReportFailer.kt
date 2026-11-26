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
import java.util.logging.Level
import java.util.logging.Logger
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
   * Every cancellation is attempted. A Measurement that is already terminal or gone counts as
   * cancelled, and one that cannot be cancelled for a persistent reason is logged and left, so that
   * [basicReport] is failed regardless.
   *
   * @param apiKey the API key of [basicReport]'s MeasurementConsumer
   * @throws Exception if a cancellation failed transiently, in which case [basicReport] is left for
   *   a retry, or if [basicReport] could not be failed
   */
  suspend fun fail(
    basicReport: BasicReport,
    reportComputations: List<ReportComputation>,
    apiKey: String,
  ) {
    val transientFailures = mutableListOf<StatusException>()
    for (reportComputation in reportComputations) {
      if (reportComputation.cmmsMeasurementId.isEmpty()) {
        continue
      }
      val name =
        MeasurementKey(basicReport.cmmsMeasurementConsumerId, reportComputation.cmmsMeasurementId)
          .toName()
      try {
        measurementsStub
          .withAuthenticationKey(apiKey)
          .cancelMeasurement(cancelMeasurementRequest { this.name = name })
      } catch (e: StatusException) {
        when (e.status.code) {
          in DONE_CODES -> {}
          in TRANSIENT_CODES -> transientFailures += e
          else -> logger.log(Level.WARNING, "Unable to cancel $name", e)
        }
      }
    }
    if (transientFailures.isNotEmpty()) {
      throw Exception("Unable to cancel ${transientFailures.size} Measurement(s)").apply {
        transientFailures.forEach { addSuppressed(it) }
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

  companion object {
    /** Codes of a cancellation whose Measurement is already terminal or gone. */
    private val DONE_CODES = setOf(Status.Code.FAILED_PRECONDITION, Status.Code.NOT_FOUND)

    /** Codes of a cancellation that may succeed if retried. */
    private val TRANSIENT_CODES = setOf(Status.Code.UNAVAILABLE, Status.Code.DEADLINE_EXCEEDED)

    private val logger: Logger =
      Logger.getLogger(MultimeasurementBasicReportFailer::class.java.name)
  }
}
