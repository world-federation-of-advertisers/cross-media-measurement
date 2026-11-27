/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.job

import org.wfanet.measurement.internal.reporting.v2.CreateReportingSetResultRequest

/** Splits `ReportingSetResult` writes into `BatchCreateReportResultAssembly` batches. */
object ReportingSetResultBatching {
  private const val REPORTING_SET_RESULT_BATCH_SIZE = 100
  // Conservative cell counts include secondary indexes and the noisy-value row for each window.
  private const val REPORTING_SET_RESULT_MUTATIONS = 32L
  private const val REPORTING_WINDOW_RESULT_MUTATIONS = 24L
  private const val REPORT_RESULT_BATCH_MUTATIONS = 60_000L
  private const val SPANNER_MUTATION_LIMIT = 80_000L
  private const val REPORT_RESULT_MUTATION_RESERVE = 1000L

  /**
   * Returns [requests] split into batches that each fit in one Spanner transaction.
   *
   * @return at least one batch, which is empty if [requests] is empty
   * @throws IllegalArgumentException if a single request has too many windows for one transaction
   */
  fun batch(
    requests: List<CreateReportingSetResultRequest>
  ): List<List<CreateReportingSetResultRequest>> {
    // TODO(world-federation-of-advertisers/cross-media-measurement#5020): Bound histogram
    // entries per batch before supporting 127 bins so requests and responses fit within 4 MiB.
    return buildList {
        // Preserve request boundaries so mutation splitting cannot regroup large payloads.
        for (chunk in requests.chunked(REPORTING_SET_RESULT_BATCH_SIZE)) {
          var batch = mutableListOf<CreateReportingSetResultRequest>()
          var batchMutations = 0L
          for (request in chunk) {
            val requestMutations =
              REPORTING_SET_RESULT_MUTATIONS +
                REPORTING_WINDOW_RESULT_MUTATIONS *
                  request.reportingSetResult.reportingWindowResultsCount
            require(requestMutations <= SPANNER_MUTATION_LIMIT - REPORT_RESULT_MUTATION_RESERVE) {
              "A reporting set result has too many windows for one Spanner transaction"
            }
            if (
              batch.isNotEmpty() &&
                batchMutations + requestMutations > REPORT_RESULT_BATCH_MUTATIONS
            ) {
              add(batch)
              batch = mutableListOf()
              batchMutations = 0L
            }
            batch += request
            batchMutations += requestMutations
          }
          if (batch.isNotEmpty()) {
            add(batch)
          }
        }
      }
      .ifEmpty { listOf(emptyList()) }
  }
}
