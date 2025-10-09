// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.postprocessing.v2alpha

import org.wfanet.measurement.internal.reporting.postprocessing.reportPostProcessorLog

/**
 * A no-op implementation of [ReportProcessor] that takes a serialized [Report] in JSON format and
 * returns the same [Report] without any modifications.
 */
class NoOpReportProcessor : ReportProcessor {
  /** Returns the input [report] without any modifications. */
  override fun processReportJson(report: String, verbose: Boolean): String {
    return report
  }

  /**
   * Returns the input [report] without any modifications, together with an empty report post
   * processor log.
   *
   * @param report The JSON [String] containing the report data to be processed.
   * @param projectId The GCS Project ID.
   * @param bucketName The GCS bucket name.
   * @param verbose If true, enables verbose logging from the underlying report processor library.
   *   Default value is false.
   * @return A [ReportProcessingOutput] that contains the corrected serialized [Report] in JSON
   *   format that is identical to the input [report] and a default [ReportPostProcessorLog] object.
   */
  override suspend fun processReportJsonAndLogResult(
    report: String,
    projectId: String,
    bucketName: String,
    verbose: Boolean,
  ): ReportProcessingOutput {
    return ReportProcessingOutput(report, reportPostProcessorLog {})
  }
}
