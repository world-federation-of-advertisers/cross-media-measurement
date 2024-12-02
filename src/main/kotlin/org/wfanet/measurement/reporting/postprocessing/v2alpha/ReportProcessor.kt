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

import com.google.gson.GsonBuilder
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.logging.Logger
import kotlin.io.path.name
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.copy

/** Corrects the inconsistent measurements in a serialized [Report]. */
interface ReportProcessor {
  /**
   * Processes a serialized [Report] and outputs a consistent one.
   *
   * @param report The serialized [Report] in JSON format.
   * @return The corrected serialized [Report] in JSON format.
   */
  fun processReportJson(report: String): String

  /** The default implementation of [ReportProcessor]. */
  companion object Default : ReportProcessor {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val PYTHON_LIBRARY_RESOURCE_NAME =
      "src/main/python/wfa/measurement/reporting/postprocessing/tools/post_process_origin_report.zip"
    private val resourcePath: Path =
      this::class.java.classLoader.getJarResourcePath(PYTHON_LIBRARY_RESOURCE_NAME)
        ?: error("$PYTHON_LIBRARY_RESOURCE_NAME not found in JAR")
    private val tempFile = File.createTempFile(resourcePath.name, "").apply { deleteOnExit() }

    init {
      // Copies python zip package from JAR to local directory.
      Files.copy(resourcePath, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
    }

    /**
     * Corrects the inconsistent measurements in the [report] and returns a corrected report in JSON
     * format.
     *
     * @param report standard JSON serialization of a Report message.
     * @return a corrected report, serialized as a standard JSON string.
     */
    override fun processReportJson(report: String): String {
      return processReport(ReportConversion.getReportFromJsonString(report)).toJson()
    }

    /**
     * Corrects the inconsistent measurements in the [report] and returns a corrected report .
     *
     * @param report a Report message.
     * @return a corrected Report.
     */
    private fun processReport(report: Report): Report {
      val reportSummaries = report.toReportSummaries()
      val correctedMeasurementsMap = mutableMapOf<String, Long>()
      for (reportSummary in reportSummaries) {
        correctedMeasurementsMap.putAll(processReportSummary(reportSummary))
      }
      val updatedReport = updateReport(report, correctedMeasurementsMap)
      return updatedReport
    }

    /**
     * Corrects the inconsistent measurements in the [reportSummary] and returns a map of metric
     * names to corrected reach values.
     *
     * Each metric name is tied to a measurement.
     */
    private fun processReportSummary(reportSummary: ReportSummary): Map<String, Long> {
      logger.info { "Start processing report.." }

      // TODO(bazelbuild/bazel#17629): Execute the Python zip directly once this bug is fixed.
      val processBuilder = ProcessBuilder("python3", tempFile.toPath().toString())

      val process = processBuilder.start()

      // Write the process' argument to its stdin.
      process.outputStream.use { outputStream ->
        reportSummary.writeTo(outputStream)
        outputStream.flush()
      }

      // Reads the output of the above process.
      val processOutput =
        BufferedReader(InputStreamReader(process.inputStream)).use { it.readText() }

      val exitCode = process.waitFor()
      require(exitCode == 0) { "Failed to process the report with exitCode $exitCode." }

      logger.info { "Finished processing report.." }

      // Converts the process output to the correction map.
      val correctedMeasurementsMap = mutableMapOf<String, Long>()
      GsonBuilder().create().fromJson(processOutput, Map::class.java).forEach { (key, value) ->
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

    /** Returns a [Report] with updated reach values from the [correctedMeasurementsMap]. */
    private fun updateReport(report: Report, correctedMeasurementsMap: Map<String, Long>): Report {
      val correctedMetricCalculationResults =
        report.metricCalculationResultsList.map { result ->
          updateMetricCalculationResult(result, correctedMeasurementsMap)
        }
      val updatedReport =
        report.copy {
          metricCalculationResults.clear()
          metricCalculationResults += correctedMetricCalculationResults
        }
      return updatedReport
    }
  }
}
