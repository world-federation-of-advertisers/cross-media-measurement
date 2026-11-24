/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.reporting.postprocessing.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.devtools.build.runfiles.Runfiles
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.internal.reporting.postprocessing.ReportPostProcessorLog
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class ReportProcessorDeterminismTest {
  private lateinit var inMemoryStorageClient: InMemoryStorageClient

  @Before
  fun setUp() {
    inMemoryStorageClient = InMemoryStorageClient()
    ReportProcessor.setTestStorageFactory(
      object : ReportProcessor.StorageFactory {
        override fun createStorageClient(projectId: String, bucketName: String): StorageClient {
          return inMemoryStorageClient
        }
      }
    )
  }

  @After
  fun tearDown() {
    ReportProcessor.resetToGcsStorageFactory()
  }

  @Test
  fun `processReportJsonAndLogResult corrects report with fixed hash seed`() = runBlocking {
    val reportAsJson = TEST_DATA_RUNTIME_DIR.resolve("sample_report_large.json").toFile().readText()

    val reportProcessingOutput =
      ReportProcessor.processReportJsonAndLogResult(reportAsJson, "projectId", "bucketName")

    assertThat(reportProcessingOutput.reportPostProcessorLog.issuesList)
      .containsExactly(ReportPostProcessorLog.ReportPostProcessorIssue.HAS_LARGE_CORRECTIONS)
    Unit
  }

  companion object {
    private val TEST_DATA_RUNTIME_DIR: Path =
      Runfiles.preload()
        .withSourceRepository("")
        .getRuntimePath(
          Paths.get(
            "wfa_measurement_system",
            "src",
            "test",
            "kotlin",
            "org",
            "wfanet",
            "measurement",
            "reporting",
            "postprocessing",
            "v2alpha",
          )
        )!!
  }
}
