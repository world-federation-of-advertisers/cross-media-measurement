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

package experimental.dp_consistency.src.test.kotlin.tools

import experimental.dp_consistency.src.main.kotlin.tools.CorrectOriginReport
import java.io.File
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class CorrectOriginReportTest {
  @Test
  fun `run correct_origin_report python binary from kotlin`() {
    val reportCorrector: CorrectOriginReport = CorrectOriginReport()
    val outputPath =
      "experimental/dp_consistency/src/test/kotlin/tools/example_origin_corrected_report.xlsx"

    // Makes sure that the output file does not exist before the execution.
    File(outputPath).delete()

    reportCorrector.correctReport(
      "experimental/dp_consistency/src/test/kotlin/tools/example_origin_report.xlsx",
      "Linear TV",
      outputPath,
    )

    assertTrue(File(outputPath).exists())
  }
}
