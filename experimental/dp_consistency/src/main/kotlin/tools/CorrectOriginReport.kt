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

import com.google.gson.GsonBuilder
import com.google.protobuf.util.JsonFormat
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.file.Path
import java.util.logging.Logger
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.common.load
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.report

/** Corrects noisy measurements in a report using the Python library `correct_origin_report`. */
class CorrectOriginReport {
  /**
   * Corrects the noisy measurements in the [reportAsJsonString] and returns a corrected report in
   * JSON format.
   */
  fun correctReport(reportAsJsonString: String): String {
    val report = getReportFromJsonString(reportAsJsonString)
    val correctedMeasurementsMap =
      correctReportSummary(JsonFormat.printer().print(report.toReportSummary()))
    val correctedReport = updateReport(report, correctedMeasurementsMap)
    return correctedReport.toJson()
  }

  /**
   * Corrects the noisy measurements in the [reportSummaryAsJsonString] and returns a map of metric
   * names to corrected reach values.
   *
   * Each metric name is tied to a measurement.
   */
  private fun correctReportSummary(reportSummaryAsJsonString: String): Map<String, Long> {
    logger.info { "Start correcting report.." }

    val process =
      ProcessBuilder("$PYTHON_LIBRARY_RESOURCE_NAME", "--report_summary=$reportSummaryAsJsonString")
        .start()

    // Reads the output of the above process.
    val processOutput = BufferedReader(InputStreamReader(process.inputStream)).use { it.readText() }

    val exitCode = process.waitFor()
    require(exitCode == 0) { "Failed to correct the report with exitCode $exitCode." }

    logger.info { "Finished correcting report.." }

    // Converts the process' output to the correction map.
    val gson = GsonBuilder().create()
    val correctedMeasurementsMap = mutableMapOf<String, Long>()
    gson.fromJson(processOutput, Map::class.java).forEach { (key, value) ->
      correctedMeasurementsMap[key as String] = (value as Double).toLong()
    }

    return correctedMeasurementsMap
  }

  /**
   * Updates a [MetricCalculationResult] with corrected reach values from the
   * [correctedMeasurementsMap].
   *
   * Only the reach-only and reach-and-frequency resultAttributes in the [MetricCalculationResult]
   * will be updated.
   */
  private fun updateMetricCalculationResult(
    metricCalculationResult: Report.MetricCalculationResult,
    correctedMeasurementsMap: Map<String, Long>,
  ): Report.MetricCalculationResult {
    val updatedMetricCalculationResult =
      metricCalculationResult.copy {
        resultAttributes.clear()
        resultAttributes +=
          metricCalculationResult.resultAttributesList.map { entry ->
            entry.copy {
              // The result attribute is updated only if its metric is in the correction map.
              if (entry.metric in correctedMeasurementsMap) {
                val correctedReach = correctedMeasurementsMap.getValue(entry.metric)
                when {
                  entry.metricResult.hasReach() -> {
                    metricResult =
                      metricResult.copy { reach = reach.copy { value = correctedReach } }
                  }
                  entry.metricResult.hasReachAndFrequency() -> {
                    val scale: Double =
                      correctedReach / entry.metricResult.reachAndFrequency.reach.value.toDouble()
                    metricResult =
                      metricResult.copy {
                        reachAndFrequency =
                          reachAndFrequency.copy {
                            reach = reach.copy { value = correctedReach }
                            frequencyHistogram =
                              frequencyHistogram.copy {
                                bins.clear()
                                bins +=
                                  entry.metricResult.reachAndFrequency.frequencyHistogram.binsList
                                    .map { bin ->
                                      bin.copy {
                                        binResult =
                                          binResult.copy { value = bin.binResult.value * scale }
                                      }
                                    }
                              }
                          }
                      }
                  }
                  else -> {}
                }
              }
            }
          }
      }
    return updatedMetricCalculationResult
  }

  /** Returns a corrected [Report] with updated reach values from the [correctedMeasurementsMap]. */
  private fun updateReport(report: Report, correctedMeasurementsMap: Map<String, Long>): Report {
    val correctedMetricCalculationResults =
      report.metricCalculationResultsList.map { result ->
        updateMetricCalculationResult(result, correctedMeasurementsMap)
      }
    val correctedReport =
      report.copy {
        metricCalculationResults.clear()
        metricCalculationResults += correctedMetricCalculationResults
      }
    return correctedReport
  }

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
    const val PYTHON_LIBRARY_RESOURCE_NAME =
      "experimental/dp_consistency/src/main/python/tools/correct_origin_report"

    init {
      val resourcePath: Path =
        this::class.java.classLoader.getJarResourcePath(PYTHON_LIBRARY_RESOURCE_NAME)
          ?: error("$PYTHON_LIBRARY_RESOURCE_NAME not found in JAR")
      Runtime.getRuntime().load(resourcePath)
    }
  }
}
