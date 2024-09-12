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

package experimental.dp_consistency.src.main.kotlin.tools

import java.util.logging.Logger
import java.io.File

class CorrectOriginReport {
  fun correctReport(inputPath: String, unnoisedEdp: String, outputPath: String) {
    logger.info { "Start correcting report.." }
    val pathToReport = "--path_to_report=" + inputPath
    val pathToCorrectedReport = "--path_to_corrected_report=" + outputPath
    val unnoisedEdp = "--unnoised_edps=" + unnoisedEdp
    val processHandler = ProcessBuilder(
      "experimental/dp_consistency/src/main/python/tools/correct_origin_report",
      pathToReport,
      unnoisedEdp,
      pathToCorrectedReport,
    )
      .redirectOutput(ProcessBuilder.Redirect.INHERIT)
      .redirectError(ProcessBuilder.Redirect.INHERIT)
      .start()
      .waitFor()
    logger.info { "Finished correcting report.." }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
