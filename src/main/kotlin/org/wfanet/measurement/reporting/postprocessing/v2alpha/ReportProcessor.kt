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
import com.google.protobuf.Timestamp
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.logging.Logger
import kotlin.io.path.name
import kotlin.math.roundToLong
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.internal.reporting.postprocessing.ReportPostProcessorLog
import org.wfanet.measurement.internal.reporting.postprocessing.ReportPostProcessorLog.ReportPostProcessorIssue
import org.wfanet.measurement.internal.reporting.postprocessing.ReportPostProcessorResult
import org.wfanet.measurement.internal.reporting.postprocessing.ReportPostProcessorStatus
import org.wfanet.measurement.internal.reporting.postprocessing.ReportQuality
import org.wfanet.measurement.internal.reporting.postprocessing.ReportSummary
import org.wfanet.measurement.internal.reporting.postprocessing.reportPostProcessorLog
import org.wfanet.measurement.internal.reporting.postprocessing.reportPostProcessorResult
import org.wfanet.measurement.internal.reporting.postprocessing.reportPostProcessorStatus
import org.wfanet.measurement.reporting.postprocessing.v2alpha.ReportProcessor.Default.currentStorageFactory
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.storage.StorageClient

/**
 * Represents the combined output of processing a report, including the updated report and the
 * associated processing log.
 *
 * @property updatedReportJson A string representation of the processed report's outcome.
 * @property reportPostProcessorLog The [ReportPostProcessorLog] object containing detailed logs and
 *   metadata generated during the report processing.
 */
data class ReportProcessingOutput(
  val updatedReportJson: String,
  val reportPostProcessorLog: ReportPostProcessorLog,
)

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
   * Processes a serialized [Report] and outputs a serialized consistent one.
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
   * @return A [ReportProcessingOutput] that contains the corrected serialized [Report] in JSON
   *   format and a [ReportPostProcessorLog] object that encapsulates detailed logs, metrics, or
   *   errors encountered during the processing of the report.
   */
  suspend fun processReportJsonAndLogResult(
    report: String,
    projectId: String,
    bucketName: String,
    verbose: Boolean = false,
  ): ReportProcessingOutput

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
    var currentStorageFactory: StorageFactory = gcsStorageFactory
      internal set

    /**
     * Replaces the [currentStorageFactory] with a specific [StorageFactory] implementation.
     *
     * This method is intended for testing purposes only.
     *
     * @param testFactory The [StorageFactory] implementation to be used for subsequent
     *   [StorageClient] creations during a test.
     */
    fun setTestStorageFactory(testFactory: StorageFactory) {
      currentStorageFactory = testFactory
    }

    /** Resets the storage factory to GCS storage factory. */
    fun resetToGcsStorageFactory() {
      currentStorageFactory = gcsStorageFactory
    }

    /**
     * Creates a [StorageClient] instance configured for the given project and bucket, using the
     * [currentStorageFactory].
     *
     * @param projectId The GCS Project ID.
     * @param bucketName The GCS bucket name.
     * @return A [StorageClient].
     */
    private suspend fun getStorageClient(projectId: String, bucketName: String): StorageClient {
      return currentStorageFactory.createStorageClient(projectId, bucketName)
    }

    /**
     * Corrects the inconsistent measurements in the [report] and returns a corrected report in JSON
     * format.
     *
     * @param report standard JSON serialization of a Report message.
     * @return a corrected report, serialized as a standard JSON string.
     */
    override fun processReportJson(report: String, verbose: Boolean): String {
      return processReport(ReportConversion.getReportFromJsonString(report), verbose)
        .updatedReportJson
    }

    /**
     * Processes a serialized [Report], outputs a consistent one, generates detailed logs of this
     * processing, and returns both a string representation of the processing result and the
     * generated log object.
     *
     * @param report The JSON [String] containing the report data to be processed.
     * @param projectId The GCS Project ID.
     * @param bucketName The GCS bucket name.
     * @param verbose If true, enables verbose logging from the underlying report processor library.
     *   Default value is false.
     * @return A [ReportProcessingOutput] that contains the corrected serialized [Report] in JSON
     *   format and a [ReportPostProcessorLog] object that encapsulates detailed logs, metrics, or
     *   errors encountered during the processing of the report.
     */
    override suspend fun processReportJsonAndLogResult(
      report: String,
      projectId: String,
      bucketName: String,
      verbose: Boolean,
    ): ReportProcessingOutput {
      require(projectId.isNotEmpty()) { "projectId cannot be empty." }
      require(bucketName.isNotEmpty()) { "bucketName cannot be empty." }

      return processReportAndLogResult(
        ReportConversion.getReportFromJsonString(report),
        projectId,
        bucketName,
        verbose,
      )
    }

    /**
     * Corrects the inconsistent measurements in the [report], writes the report post processor log
     * to a GCS bucket, and returns a [ReportProcessingOutput] object.
     *
     * @param report The input [Report] object that needs to be processed.
     * @param projectId The GCS project ID.
     * @param bucketName The GCS bucket name.
     * @param verbose A boolean flag indicating whether to perform verbose logging.
     * @return A [ReportProcessingOutput] object which contains an updated [Report] and a
     *   [ReportPostProcessorLog] object.
     */
    private suspend fun processReportAndLogResult(
      report: Report,
      projectId: String,
      bucketName: String,
      verbose: Boolean = false,
    ): ReportProcessingOutput {
      require(projectId.isNotEmpty()) { "projectId cannot be empty." }
      require(bucketName.isNotEmpty()) { "bucketName cannot be empty." }

      val reportProcessingOutput: ReportProcessingOutput = processReport(report, verbose)

      val storageClient: StorageClient = getStorageClient(projectId, bucketName)
      val blobKey: String = getBlobKey(report)
      storageClient.writeBlob(blobKey, reportProcessingOutput.reportPostProcessorLog.toByteString())

      return reportProcessingOutput
    }

    /**
     * Processes the given [Report] object and returns a [ReportProcessingOutput] object.
     *
     * @param report The input [Report] object that needs to be processed.
     * @param verbose A boolean flag indicating whether to perform verbose logging.
     * @return A [ReportProcessingOutput] object which contains an updated [Report] and a
     *   [ReportPostProcessorLog] object.
     */
    private fun processReport(report: Report, verbose: Boolean = false): ReportProcessingOutput {
      val reportSummaries = report.toReportSummaries()
      val correctedMeasurementsMap = mutableMapOf<String, Double>()
      val resultMap = mutableMapOf<String, ReportPostProcessorResult>()
      val foundIssues = mutableSetOf<ReportPostProcessorIssue>()

      for (reportSummary in reportSummaries) {
        val result: ReportPostProcessorResult =
          try {
            processReportSummary(reportSummary, verbose)
          } catch (e: Exception) {
            val errorMessage =
              "Report processing for the demographic groups " +
                "${reportSummary.demographicGroupsList.joinToString(separator = ",")} failed " +
                "with an exception: ${e.message}"

            logger.warning(errorMessage)

            // Logs the report summary when report post processor fails.
            reportPostProcessorResult {
              preCorrectionReportSummary = reportSummary
              status = reportPostProcessorStatus {
                statusCode = ReportPostProcessorStatus.StatusCode.INTERNAL_ERROR
              }
              this.errorMessage = errorMessage
            }
          }

        if (
          result.status.statusCode != ReportPostProcessorStatus.StatusCode.SOLUTION_NOT_FOUND &&
            result.status.statusCode != ReportPostProcessorStatus.StatusCode.INTERNAL_ERROR
        ) {
          val updatedMeasurements = mutableMapOf<String, Double>()
          result.updatedMeasurementsMap.forEach { (key, value) -> updatedMeasurements[key] = value }
          correctedMeasurementsMap.putAll(updatedMeasurements)
        }
        resultMap[reportSummary.demographicGroupsList.joinToString(separator = ",")] = result
      }

      // Iterate over the results only once.
      for (result in resultMap.values) {
        // Checks for QP solver solution not found.
        if (result.status.statusCode == ReportPostProcessorStatus.StatusCode.SOLUTION_NOT_FOUND) {
          foundIssues.add(ReportPostProcessorIssue.QP_SOLUTION_NOT_FOUND)
        }

        // Checks for internal error failure.
        if (result.status.statusCode == ReportPostProcessorStatus.StatusCode.INTERNAL_ERROR) {
          foundIssues.add(ReportPostProcessorIssue.INTERNAL_ERROR)
        }

        // Checks for zero variance measurements quality pre-correction inconsistency.
        if (
          result.preCorrectionQuality.zeroVarianceMeasurementsStatus ==
            ReportQuality.ZeroVarianceMeasurementsStatus.INCONSISTENT
        ) {
          foundIssues.add(
            ReportPostProcessorIssue.ZERO_VARIANCE_MEASUREMENTS_INCONSISTENT_PRE_CORRECTION
          )
        }

        // Checks for zero variance measurements quality post-correction inconsistency.
        if (
          result.postCorrectionQuality.zeroVarianceMeasurementsStatus ==
            ReportQuality.ZeroVarianceMeasurementsStatus.INCONSISTENT
        ) {
          foundIssues.add(
            ReportPostProcessorIssue.ZERO_VARIANCE_MEASUREMENTS_INCONSISTENT_POST_CORRECTION
          )
        }

        // Checks for independence check pre-correction failure.
        if (
          result.preCorrectionQuality.unionStatus ==
            ReportQuality.IndependenceCheckStatus.OUTSIDE_CONFIDENCE_RANGE
        ) {
          foundIssues.add(ReportPostProcessorIssue.INDEPENDENCE_CHECK_FAILS_PRE_CORRECTION)
        }

        // Checks for independence check post-correction failure.
        if (
          result.postCorrectionQuality.unionStatus ==
            ReportQuality.IndependenceCheckStatus.OUTSIDE_CONFIDENCE_RANGE
        ) {
          foundIssues.add(ReportPostProcessorIssue.INDEPENDENCE_CHECK_FAILS_POST_CORRECTION)
        }

        // Checks for large corrections.
        if (result.largeCorrectionsList.isNotEmpty()) {
          foundIssues.add(ReportPostProcessorIssue.HAS_LARGE_CORRECTIONS)
        }
      }

      val isSuccessful =
        foundIssues
          .intersect(
            listOf(
              ReportPostProcessorIssue.INTERNAL_ERROR,
              ReportPostProcessorIssue.HAS_LARGE_CORRECTIONS,
              ReportPostProcessorIssue.QP_SOLUTION_NOT_FOUND,
            )
          )
          .isEmpty()

      val updatedReport: Report = updateReport(report, correctedMeasurementsMap)

      val reportPostProcessorLog = reportPostProcessorLog {
        reportId = report.name
        createTime = report.createTime
        results.putAll(resultMap)
        issues.addAll(foundIssues)
        postProcessingSuccessful = isSuccessful
      }

      return ReportProcessingOutput(
        updatedReportJson = updatedReport.toJson(),
        reportPostProcessorLog = reportPostProcessorLog,
      )
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
      correctedMeasurementsMap: Map<String, Double>,
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
                            reach.copy {
                              value = correctedMeasurementsMap.getValue(entry.metric).roundToLong()
                            }
                        }
                    }
                    entry.metricResult.hasReachAndFrequency() -> {
                      metricResult =
                        metricResult.copy {
                          reachAndFrequency =
                            reachAndFrequency.copy {
                              reach =
                                reach.copy {
                                  value =
                                    correctedMeasurementsMap.getValue(entry.metric).roundToLong()
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
                              value = correctedMeasurementsMap.getValue(entry.metric).roundToLong()
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
    private fun updateReport(
      report: Report,
      correctedMeasurementsMap: Map<String, Double>,
    ): Report {
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

    /** Get blobKey from the bucket name and the report create time. */
    private fun getBlobKey(report: Report): String {
      val createTime: String = timestampToString(report.createTime)
      val blobKey: String =
        createTime.substring(0, "yyyyMMdd".length) +
          "/" +
          createTime +
          "_" +
          report.name.substringAfterLast('/') +
          ".textproto"
      return blobKey
    }

    /** Converts [timestamp] to a string with the format yyyyMMddHHmmss. */
    private fun timestampToString(timestamp: Timestamp): String {
      val instant = Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong())
      val zonedDateTime = instant.atZone(ZoneId.systemDefault())
      val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss", Locale.getDefault())
      return zonedDateTime.format(formatter)
    }
  }
}
