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

package org.wfanet.measurement.edpaggregator.validation

import io.opentelemetry.api.common.Attributes
import java.time.Duration
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeSource
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionQueryResponse
import org.wfanet.measurement.api.v2alpha.ImpressionQueryKt.entityKey
import org.wfanet.measurement.api.v2alpha.dataProviderImpressionQueryRequest
import org.wfanet.measurement.api.v2alpha.impressionQuery
import org.wfanet.measurement.config.edpaggregator.DataProviderValidationConfig
import org.wfanet.measurement.config.edpaggregator.DataProviderValidationConfigs

/**
 * Orchestrates per-DataProvider impression validation for a report.
 *
 * For each opted-in DataProvider, builds a validation request from the report's per-EDP impression
 * data, calls the DataProvider's cloud function via [ValidationCloudFunctionClient], evaluates the
 * response using [ToleranceEvaluator], and fails the report when a deviation exceeds the configured
 * failure threshold.
 *
 * @param configs per-DataProvider validation configurations.
 * @param client HTTP client for calling cloud functions.
 * @param metrics OpenTelemetry metrics.
 */
class EdpValidationPostProcessor(
  private val configs: DataProviderValidationConfigs,
  private val client: ValidationCloudFunctionClient,
  private val metrics: EdpValidationMetrics = EdpValidationMetrics(),
) {

  /**
   * Per-DataProvider impression data extracted from a report for validation.
   *
   * @param dataProviderName resource name of the `DataProvider`.
   * @param entityType type of entity (e.g. "campaign").
   * @param entityId identifier of the entity (e.g. event_group_reference_id).
   * @param startTimeSeconds start of the measurement period (epoch seconds, UTC).
   * @param endTimeSeconds end of the measurement period (epoch seconds, UTC).
   * @param reportedImpressions impression count from the EDPA report.
   * @param vidSamplingWidth VID sampling fraction applied by the EDPA.
   */
  data class ImpressionDataRow(
    val dataProviderName: String,
    val entityType: String,
    val entityId: String,
    val startTimeSeconds: Long,
    val endTimeSeconds: Long,
    val reportedImpressions: Long,
    val vidSamplingWidth: Double,
  )

  /** Aggregate validation result for a report. */
  enum class ReportValidationResult {
    /** At least one query produced a verdict and none failed. */
    PASSED,
    /** At least one query exceeded the failure threshold. */
    FAILED,
    /** No query produced a verdict (no matching config, every call errored, or every skip). */
    SKIPPED,
  }

  /**
   * Validates impression data for all opted-in DataProviders.
   *
   * @param rows per-DataProvider impression data from the report.
   * @return aggregate validation result.
   */
  fun validate(rows: List<ImpressionDataRow>): ReportValidationResult {
    val rowsByDataProvider = rows.groupBy { it.dataProviderName }
    val configByDataProvider = configs.configsList.associateBy { it.dataProvider }
    var anyFailed = false
    var anyPassed = false

    for ((dataProviderName, dataProviderRows) in rowsByDataProvider) {
      val config = configByDataProvider[dataProviderName] ?: continue

      when (validateDataProvider(dataProviderName, config, dataProviderRows)) {
        ReportValidationResult.FAILED -> anyFailed = true
        ReportValidationResult.PASSED -> anyPassed = true
        ReportValidationResult.SKIPPED -> {}
      }
    }

    return when {
      anyFailed -> ReportValidationResult.FAILED
      anyPassed -> ReportValidationResult.PASSED
      else -> ReportValidationResult.SKIPPED
    }
  }

  private fun validateDataProvider(
    dataProviderName: String,
    config: DataProviderValidationConfig,
    rows: List<ImpressionDataRow>,
  ): ReportValidationResult {
    var anyFailed = false
    var anyVerdict = false
    val attrs = Attributes.of(EdpValidationMetrics.DATA_PROVIDER_KEY, dataProviderName)

    for (row in rows) {
      val request = dataProviderImpressionQueryRequest {
        requestId = UUID.randomUUID().toString()
        this.dataProvider = dataProviderName
        query = impressionQuery {
          entityKeys += entityKey {
            entityType = row.entityType
            entityId = row.entityId
          }
          timeInterval =
            com.google.type.interval {
              startTime = com.google.protobuf.timestamp { seconds = row.startTimeSeconds }
              endTime = com.google.protobuf.timestamp { seconds = row.endTimeSeconds }
            }
        }
      }

      val timer = TimeSource.Monotonic.markNow()
      val timeout =
        if (config.hasRequestTimeoutOverride()) {
          Duration.ofSeconds(config.requestTimeoutOverride.seconds)
        } else {
          DEFAULT_TIMEOUT
        }

      val response: DataProviderImpressionQueryResponse =
        try {
          client.call(config.endpoint.endpointUri, request, timeout)
        } catch (e: CloudFunctionException) {
          metrics.requestDurationHistogram.record(
            timer.elapsedNow().inWholeMilliseconds / 1000.0,
            attrs,
          )
          logger.log(Level.WARNING, "Cloud function call failed for $dataProviderName", e)
          val errorType = if (e.httpStatus != null) "http_${e.httpStatus}" else "request_error"
          metrics.cloudFunctionErrorsCounter.add(
            1,
            Attributes.of(
              EdpValidationMetrics.DATA_PROVIDER_KEY,
              dataProviderName,
              EdpValidationMetrics.ERROR_TYPE_KEY,
              errorType,
            ),
          )
          metrics.skippedQueriesCounter.add(
            1,
            Attributes.of(
              EdpValidationMetrics.DATA_PROVIDER_KEY,
              dataProviderName,
              EdpValidationMetrics.SKIP_REASON_KEY,
              "cf_error",
            ),
          )
          continue
        }

      metrics.requestDurationHistogram.record(
        timer.elapsedNow().inWholeMilliseconds / 1000.0,
        attrs,
      )

      when {
        response.hasSkipped() -> {
          logger.info(
            "Validation skipped for $dataProviderName: ${response.skipped.reason} - ${response.skipped.detail}"
          )
          metrics.skippedQueriesCounter.add(
            1,
            Attributes.of(
              EdpValidationMetrics.DATA_PROVIDER_KEY,
              dataProviderName,
              EdpValidationMetrics.SKIP_REASON_KEY,
              response.skipped.reason.name,
            ),
          )
        }
        response.hasResult() -> {
          val evaluation =
            ToleranceEvaluator.evaluate(
              reportedCount = row.reportedImpressions,
              publisherCount = response.result.value,
              config = config.tolerance,
              vidSamplingWidth = row.vidSamplingWidth,
            )

          when (evaluation.verdict) {
            ToleranceEvaluator.Verdict.SKIPPED -> {
              // A skipped evaluation made no comparison; count it as a skip (consistent with the
              // other skip paths) and emit neither a deviation sample nor a verdict query.
              logger.info(
                "Validation SKIPPED for $dataProviderName entity ${row.entityId}: " +
                  "below minimum count or no expected impressions"
              )
              metrics.skippedQueriesCounter.add(
                1,
                Attributes.of(
                  EdpValidationMetrics.DATA_PROVIDER_KEY,
                  dataProviderName,
                  EdpValidationMetrics.SKIP_REASON_KEY,
                  "below_minimum_count",
                ),
              )
            }
            ToleranceEvaluator.Verdict.PASS,
            ToleranceEvaluator.Verdict.WARNING,
            ToleranceEvaluator.Verdict.FAIL -> {
              val deviation =
                checkNotNull(evaluation.deviationFraction) {
                  "A non-SKIPPED verdict must carry a deviation fraction"
                }
              val verdictAttrs =
                Attributes.of(
                  EdpValidationMetrics.DATA_PROVIDER_KEY,
                  dataProviderName,
                  EdpValidationMetrics.VERDICT_KEY,
                  evaluation.verdict.name,
                )
              metrics.deviationFractionHistogram.record(deviation, verdictAttrs)
              metrics.queriesCounter.add(1, verdictAttrs)
              anyVerdict = true

              when (evaluation.verdict) {
                ToleranceEvaluator.Verdict.FAIL -> {
                  logger.log(
                    Level.SEVERE,
                    "Validation FAILED for $dataProviderName entity ${row.entityId}: " +
                      "reported=${evaluation.reportedCount}, publisher=${evaluation.publisherCount}, " +
                      "deviation=${evaluation.deviationFraction}, tolerance=${evaluation.effectiveTolerance}",
                  )
                  anyFailed = true
                }
                ToleranceEvaluator.Verdict.WARNING ->
                  logger.log(
                    Level.WARNING,
                    "Validation WARNING for $dataProviderName entity ${row.entityId}: " +
                      "reported=${evaluation.reportedCount}, publisher=${evaluation.publisherCount}, " +
                      "deviation=${evaluation.deviationFraction}",
                  )
                ToleranceEvaluator.Verdict.PASS ->
                  logger.fine("Validation PASSED for $dataProviderName entity ${row.entityId}")
                ToleranceEvaluator.Verdict.SKIPPED -> error("SKIPPED handled in the outer branch")
              }
            }
          }
        }
      }
    }

    if (anyFailed) {
      metrics.reportFailuresCounter.add(1, attrs)
    }

    return when {
      anyFailed -> ReportValidationResult.FAILED
      anyVerdict -> ReportValidationResult.PASSED
      else -> ReportValidationResult.SKIPPED
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(EdpValidationPostProcessor::class.java.name)
    private val DEFAULT_TIMEOUT: Duration = Duration.ofSeconds(30)
  }
}
