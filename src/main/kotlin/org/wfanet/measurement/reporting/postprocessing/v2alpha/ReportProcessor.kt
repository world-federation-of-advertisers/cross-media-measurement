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

import com.google.cloud.storage.StorageOptions
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
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.reporting.postprocessing.v2alpha.ReportPostProcessorLog.ReportPostProcessorIssue
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.storage.StorageClient

class GcsStorageFactory : ReportProcessor.StorageFactory {
  override fun createStorageClient(projectId: String, bucketName: String): StorageClient {
    require(projectId.isNotEmpty()) { "projectId cannot be empty." }
    require(bucketName.isNotEmpty()) { "bucketName cannot be empty." }
    return GcsStorageClient(
      StorageOptions.newBuilder().setProjectId(projectId).build().service,
      bucketName,
    )
  }
}

class ReportProcessorFailureException(message: String) : RuntimeException(message)

/** Corrects the inconsistent measurements in a serialized [Report]. */
interface ReportProcessor {

  /** A factory interface responsible for creating instances of [StorageClient]. */
  interface StorageFactory {
    /**
     * Creates and returns a [StorageClient] instance configured for the specified project and
     * bucket.
     *
     * @param projectId The project ID that the created [StorageClient] will be associated with.
     *   Must not be empty.
     * @param bucketName The bucket name that the created [StorageClient] will operate on. Must not
     *   be empty.
     * @return A [StorageClient] instance.
     */
    fun createStorageClient(projectId: String, bucketName: String): StorageClient
  }

  /**
   * Processes a serialized [Report] and outputs a consistent one.
   *
   * @param report The serialized [Report] in JSON format.
   * @param verbose If true, enables verbose logging from the underlying report processor library.
   *   Default value is false.
   * @return The corrected serialized [Report] in JSON format.
   */
  fun processReportJson(report: String, verbose: Boolean = false): String

  /**
   * Processes a serialized [Report], outputs a consistent one, generates detailed logs of this
   * processing, and returns both a string representation of the processing result and the generated
   * log object.
   *
   * @param report The JSON [String] containing the report data to be processed.
   * @param projectId The GCS Project ID.
   * @param bucketName The GCS bucket name.
   * @param verbose If true, enables verbose logging from the underlying report processor library.
   *   Default value is false.
   * @return A [Pair] of the corrected serialized [Report] in JSON format and a
   *   [ReportPostProcessorLog] object that encapsulates detailed logs, metrics, or errors
   *   encountered during the processing of the report.
   */
  suspend fun processReportJsonAndLogResult(
    report: String,
    projectId: String,
    bucketName: String,
    verbose: Boolean = false,
  ): Pair<String, ReportPostProcessorLog>

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

    // Default factory instance.
    private val gcsStorageFactory = GcsStorageFactory()

    /**
     * The currently active [StorageFactory] that will be used to create [StorageClient] instances.
     */
    var currentFactory: StorageFactory = gcsStorageFactory
      internal set

    /**
     * Replaces the [currentFactory] with a specific [StorageFactory] implementation.
     *
     * This method is intended for testing purposes only.
     *
     * @param testFactory The [StorageFactory] implementation to be used for subsequent
     *   [StorageClient] creations during a test.
     */
    fun setTestStorageFactory(testFactory: StorageFactory) {
      currentFactory = testFactory
    }

    /** Resets the storage factory to GCS storage factory. */
    fun resetToGcsStorageFactory() {
      currentFactory = gcsStorageFactory
    }

    /**
     * Creates a [StorageClient] instance configured for the given project and bucket, using the
     * [currentFactory].
     *
     * @param projectId The GCS Project ID.
     * @param bucketName The GCS bucket name.
     * @return A [StorageClient].
     */
    private suspend fun getStorageClient(projectId: String, bucketName: String): StorageClient {
      return currentFactory.createStorageClient(projectId, bucketName)
    }

    /**
     * Corrects the inconsistent measurements in the [report] and returns a corrected report in JSON
     * format.
     *
     * @param report standard JSON serialization of a Report message.
     * @return a corrected report, serialized as a standard JSON string.
     */
    override fun processReportJson(report: String, verbose: Boolean): String {
      return processReport(ReportConversion.getReportFromJsonString(report), verbose).toJson()
    }

    override suspend fun processReportJsonAndLogResult(
      report: String,
      projectId: String,
      bucketName: String,
      verbose: Boolean,
    ): Pair<String, ReportPostProcessorLog> {
      require(projectId.isNotEmpty()) { "projectId cannot be empty." }
      require(bucketName.isNotEmpty()) { "bucketName cannot be empty." }

      val (updatedReport, reportPostProcessorLog) =
        processReportAndLogResult(
          ReportConversion.getReportFromJsonString(report),
          projectId,
          bucketName,
          verbose,
        )
      return Pair(updatedReport.toJson(), reportPostProcessorLog)
    }

    /**
     * Corrects the inconsistent measurements in the [report] and returns a corrected report .
     *
     * @param report a Report message.
     * @return a corrected Report.
     */
    private fun processReport(report: Report, verbose: Boolean = false): Report {
      val reportSummaries = report.toReportSummaries()
      val correctedMeasurementsMap = mutableMapOf<String, Long>()
      for (reportSummary in reportSummaries) {
        val result: ReportPostProcessorResult = processReportSummary(reportSummary, verbose)
        if (result.status.statusCode != ReportPostProcessorStatus.StatusCode.SOLUTION_NOT_FOUND) {
          val updatedMeasurements = mutableMapOf<String, Long>()
          result.updatedMeasurementsMap.forEach { (key, value) -> updatedMeasurements[key] = value }
          correctedMeasurementsMap.putAll(updatedMeasurements)
        }
      }

      return updateReport(report, correctedMeasurementsMap)
    }

    /**
     * Corrects the inconsistent measurements in the [report], writes the report post processor log
     * to a GCS bucket, and returns a corrected report.
     *
     * @param report The input [Report] object that needs to be processed.
     * @param projectId The GCS project ID.
     * @param bucketName The GCS bucket name.
     * @param verbose A boolean flag indicating whether to perform verbose logging.
     * @return A pair of corrected [Report], and the [ReportPostProcessorLog].
     */
    private suspend fun processReportAndLogResult(
      report: Report,
      projectId: String,
      bucketName: String,
      verbose: Boolean = false,
    ): Pair<Report, ReportPostProcessorLog> {
      require(projectId.isNotEmpty()) { "projectId cannot be empty." }
      require(bucketName.isNotEmpty()) { "bucketName cannot be empty." }

      val reportSummaries = report.toReportSummaries()
      val correctedMeasurementsMap = mutableMapOf<String, Long>()
      val resultMap = mutableMapOf<String, ReportPostProcessorResult>()
      for (reportSummary in reportSummaries) {
        val result: ReportPostProcessorResult = processReportSummary(reportSummary, verbose)
        if (result.status.statusCode != ReportPostProcessorStatus.StatusCode.SOLUTION_NOT_FOUND) {
          val updatedMeasurements = mutableMapOf<String, Long>()
          result.updatedMeasurementsMap.forEach { (key, value) -> updatedMeasurements[key] = value }
          correctedMeasurementsMap.putAll(updatedMeasurements)
        }
        resultMap[reportSummary.demographicGroupsList.joinToString(separator = ",")] = result
      }

      val issues = mutableListOf<ReportPostProcessorIssue>()

      // Adds 1 issue for QP solver if there is any.
      for (result in resultMap.values) {
        if (result.status.statusCode == ReportPostProcessorStatus.StatusCode.SOLUTION_NOT_FOUND) {
          issues.add(ReportPostProcessorIssue.QP_SOLUTION_NOT_FOUND)
          break
        }
      }

      // Adds 1 issue for tv quality pre correction if there is any.
      for (result in resultMap.values) {
        if (result.preCorrectionQuality.tvStatus == ReportQuality.LinearTvStatus.INCONSISTENT) {
          issues.add(ReportPostProcessorIssue.LINEAR_TV_INCONSISTENT_PRE_CORRECTION)
        }
      }

      // Adds 1 issue for tv quality post correction if there is any.
      for (result in resultMap.values) {
        if (result.postCorrectionQuality.tvStatus == ReportQuality.LinearTvStatus.INCONSISTENT) {
          issues.add(ReportPostProcessorIssue.LINEAR_TV_INCONSISTENT_POST_CORRECTION)
        }
      }

      // Adds 1 issue for independence check for union reaches pre correction if there is any.
      for (result in resultMap.values) {
        if (
          result.preCorrectionQuality.unionStatus ==
            ReportQuality.IndependenceCheckStatus.OUTSIDE_CONFIDENCE_RANGE
        ) {
          issues.add(ReportPostProcessorIssue.INDEPENDENCE_CHECK_FAILS_PRE_CORRECTION)
          break
        }
      }

      // Adds 1 issue for independence check for union reaches post correction if there is any.
      for (result in resultMap.values) {
        if (
          result.postCorrectionQuality.unionStatus ==
            ReportQuality.IndependenceCheckStatus.OUTSIDE_CONFIDENCE_RANGE
        ) {
          issues.add(ReportPostProcessorIssue.INDEPENDENCE_CHECK_FAILS_POST_CORRECTION)
          break
        }
      }

      val updatedReport: Report = updateReport(report, correctedMeasurementsMap)

      val reportPostProcessorLog = reportPostProcessorLog {
        reportId = report.name
        createTime = report.createTime
        results.putAll(resultMap)
        issues.addAll(issues)
      }

      val storageClient: StorageClient = getStorageClient(projectId, bucketName)

      val blobKey: String =
        bucketName +
          "/" +
          report.name.substringAfterLast('/') +
          "-" +
          report.createTime.seconds +
          ".txt"

      storageClient.writeBlob(blobKey, reportPostProcessorLog.toByteString())

      return Pair(updatedReport, reportPostProcessorLog)
    }

    /**
     * Corrects the inconsistent measurements in the [reportSummary] and returns a map of metric
     * names to corrected reach values.
     *
     * Each metric name is tied to a measurement.
     */
    private fun processReportSummary(
      reportSummary: ReportSummary,
      verbose: Boolean = false,
    ): ReportPostProcessorResult {
      logger.info { "Start processing report summary.." }

      // TODO(bazelbuild/bazel#17629): Execute the Python zip directly once this bug is fixed.
      val processBuilder = ProcessBuilder("python3", tempFile.toPath().toString())

      // Sets verbosity for python program.
      if (verbose) {
        processBuilder.command().add("--debug")
      }
      val process = processBuilder.start()

      // Write the process' argument to its stdin.
      process.outputStream.use { outputStream ->
        reportSummary.writeTo(outputStream)
        outputStream.flush()
      }

      // Reads the report post processor result.
      val result: ReportPostProcessorResult =
        ReportPostProcessorResult.parseFrom(process.inputStream)

      // Logs from python program, which are written to stderr, are read and re-logged. When
      // encountering an error or a critical log, throws a RuntimeException.
      val processError =
        BufferedReader(InputStreamReader(process.errorStream)).use { it.readText() }

      val exitCode = process.waitFor()

      if (exitCode == 0) {
        logger.fine(processError)
      } else {
        throw ReportProcessorFailureException(processError)
      }

      logger.info { "Finished processing report summary.." }

      val correctedMeasurementsMap = mutableMapOf<String, Long>()

      // Extracts the list of updated measurements if a solution is found.
      if (result.status.statusCode == ReportPostProcessorStatus.StatusCode.SOLUTION_NOT_FOUND) {
        logger.info { "No solution found.." }
      }

      return result
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
                  when {
                    entry.metricResult.hasReach() -> {
                      metricResult =
                        metricResult.copy {
                          reach =
                            reach.copy { value = correctedMeasurementsMap.getValue(entry.metric) }
                        }
                    }
                    entry.metricResult.hasReachAndFrequency() -> {
                      metricResult =
                        metricResult.copy {
                          reachAndFrequency =
                            reachAndFrequency.copy {
                              reach =
                                reach.copy {
                                  value = correctedMeasurementsMap.getValue(entry.metric)
                                }
                              frequencyHistogram =
                                frequencyHistogram.copy {
                                  bins.clear()
                                  bins +=
                                    entry.metricResult.reachAndFrequency.frequencyHistogram.binsList
                                      .map { bin ->
                                        bin.copy {
                                          binResult =
                                            binResult.copy {
                                              value =
                                                correctedMeasurementsMap
                                                  .getValue(
                                                    entry.metric + "-frequency-" + bin.label
                                                  )
                                                  .toDouble()
                                            }
                                        }
                                      }
                                }
                            }
                        }
                    }
                    entry.metricResult.hasImpressionCount() -> {
                      metricResult =
                        metricResult.copy {
                          impressionCount =
                            impressionCount.copy {
                              value = correctedMeasurementsMap.getValue(entry.metric)
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
