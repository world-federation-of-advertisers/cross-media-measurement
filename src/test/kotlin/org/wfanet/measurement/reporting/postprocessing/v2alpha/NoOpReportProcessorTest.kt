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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class NoOpReportProcessorTest {
  @Test
  fun `The default report processor successfully processes a report`() {
    val reportProcessor = NoOpReportProcessor()
    val processedReport = reportProcessor.processReportJson(SAMPLE_REPORT)
    assertThat(processedReport).isEqualTo(SAMPLE_REPORT)
  }

  @Test
  fun `The default report processor successfully processes a report and log result`() {
    val reportProcessor = NoOpReportProcessor()
    val reportProcessingOutput: ReportProcessingOutput = runBlocking {
      reportProcessor.processReportJsonAndLogResult(SAMPLE_REPORT, "projectId", "bucketName")
    }

    assertThat(reportProcessingOutput.updatedReportJson).isEqualTo(SAMPLE_REPORT)
  }

  companion object {
    private val SAMPLE_REPORT =
      """
      {
        "name": "Sample report",
        "reportingMetricEntries": [],
        "state": "STATE_UNSPECIFIED",
        "metricCalculationResults": [],
        "reportSchedule": "",
        "tags": {},
        "reportingInterval": {}
      }
      """
  }
}
