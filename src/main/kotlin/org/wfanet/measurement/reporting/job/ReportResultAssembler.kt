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

import com.google.type.DateTime
import io.grpc.Status
import io.grpc.StatusException
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.CreateReportingSetResultRequest
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt.ReportResultsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportResultAssemblyRequest
import org.wfanet.measurement.internal.reporting.v2.beginReportResultAssemblyRequest
import org.wfanet.measurement.internal.reporting.v2.discardReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.reportResult

/** Assembles the `ReportResult` of a [BasicReport] from its `ReportingSetResult`s. */
class ReportResultAssembler(private val reportResultsStub: ReportResultsCoroutineStub) {
  /** The outcome of [assemble]. */
  enum class Outcome {
    /** The `ReportingSetResult`s were written and the `ReportResult` associated. */
    ASSEMBLED,
    /** Another worker is assembling the [BasicReport]'s results. */
    ALREADY_ACTIVE,
  }

  /**
   * Begins a `ReportResult` of [basicReport] starting at [reportStart] and writes the
   * `ReportingSetResult` requests that [buildRequests] returns for it, in batches that each fit in
   * one transaction.
   *
   * The `ReportResult` is discarded if anything throws, including [buildRequests].
   *
   * @throws Exception if the `ReportResult` could not be begun
   * @throws StatusException if a batch could not be written
   */
  suspend fun assemble(
    basicReport: BasicReport,
    reportStart: DateTime,
    buildRequests: suspend (ReportResult) -> List<CreateReportingSetResultRequest>,
  ): Outcome {
    val reportResult: ReportResult =
      try {
        reportResultsStub.beginReportResultAssembly(
          beginReportResultAssemblyRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
            requestId = UUID.randomUUID().toString()
            reportResult = reportResult { this.reportStart = reportStart }
          }
        )
      } catch (e: StatusException) {
        if (e.status.code == Status.Code.ALREADY_EXISTS) {
          return Outcome.ALREADY_ACTIVE
        }
        throw Exception("Unable to begin report-result assembly", e)
      }

    var resultAssociated = false
    try {
      val requests: List<CreateReportingSetResultRequest> = buildRequests(reportResult)
      // Validate dimensions across the entire result set before any partial writes.
      require(requests.map { it.reportingSetResult.dimension }.toSet().size == requests.size) {
        "Report results contain duplicate dimensions"
      }
      val batches = ReportingSetResultBatching.batch(requests)
      for ((index, batch) in batches.withIndex()) {
        reportResultsStub.batchCreateReportResultAssembly(
          batchCreateReportResultAssemblyRequest {
            cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
            externalReportResultId = reportResult.externalReportResultId
            this.requests += batch
            externalBasicReportId = basicReport.externalBasicReportId
            finalBatch = index == batches.lastIndex
          }
        )
      }
      resultAssociated = true
      return Outcome.ASSEMBLED
    } finally {
      if (!resultAssociated) {
        discard(reportResult)
      }
    }
  }

  private suspend fun discard(reportResult: ReportResult) {
    try {
      reportResultsStub.discardReportResult(
        discardReportResultRequest {
          cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
          externalReportResultId = reportResult.externalReportResultId
        }
      )
    } catch (e: StatusException) {
      logger.log(Level.WARNING, "Unable to discard abandoned report results", e)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(ReportResultAssembler::class.java.name)
  }
}
