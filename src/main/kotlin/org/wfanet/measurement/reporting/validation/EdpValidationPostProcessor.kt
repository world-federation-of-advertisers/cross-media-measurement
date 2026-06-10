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

package org.wfanet.measurement.reporting.validation

import io.opentelemetry.api.common.Attributes
import java.time.Duration
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeSource
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionValidationResponse
import org.wfanet.measurement.api.v2alpha.dataProviderImpressionValidationRequest
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionValidationResponseKt.impressionCount
import org.wfanet.measurement.api.v2alpha.ImpressionValidationQueryKt.entityKey
import org.wfanet.measurement.api.v2alpha.impressionValidationQuery
import org.wfanet.measurement.config.edpaggregator.EdpValidationConfig
import org.wfanet.measurement.config.edpaggregator.EdpValidationConfigs
import org.wfanet.measurement.config.edpaggregator.FailurePolicy

/**
 * Orchestrates per-DataProvider impression validation for a report.
 *
 * For each opted-in DataProvider, builds a validation request from the report's per-EDP impression
 * data, calls the DataProvider's cloud function via [ValidationCloudFunctionClient], evaluates the
 * response using [ToleranceEvaluator], and applies the configured [FailurePolicy].
 *
 * @param configs per-DataProvider validation configurations.
 * @param client HTTP client for calling cloud functions.
 * @param metrics OpenTelemetry metrics.
 */
class EdpValidationPostProcessor(
  private val configs: EdpValidationConfigs,
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
    val vidSamplingWidth: Double = 1.0,
  )

  /** Aggregate validation result for a report. */
  enum class ReportValidationResult {
    PASSED,
    FAILED,
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
    var anyFailed = false

    for ((dataProviderName, dataProviderRows) in rowsByDataProvider) {
      val config = configs.configsMap[dataProviderName] ?: continue
      if (!config.enabled) continue

      val result = validateDataProvider(dataProviderName, config, dataProviderRows)
      if (result == ReportValidationResult.FAILED &&
        config.failurePolicy == FailurePolicy.FAIL_REPORT
      ) {
        anyFailed = true
      }
    }

    return if (anyFailed) ReportValidationResult.FAILED else ReportValidationResult.PASSED
  }

  private fun validateDataProvider(
    dataProviderName: String,
    config: EdpValidationConfig,
    rows: List<ImpressionDataRow>,
  ): ReportValidationResult {
    var anyFailed = false
    val attrs = Attributes.of(EdpValidationMetrics.DATA_PROVIDER_KEY, dataProviderName)

    for (row in rows) {
      val request = dataProviderImpressionValidationRequest {
        requestId = UUID.randomUUID().toString()
        this.dataProviderName = dataProviderName
        query = impressionValidationQuery {
          entityKeys += entityKey {
            entityType = row.entityType
            entityId = row.entityId
          }
          timeInterval = com.google.type.interval {
            startTime = com.google.protobuf.timestamp { seconds = row.startTimeSeconds }
            endTime = com.google.protobuf.timestamp { seconds = row.endTimeSeconds }
          }
        }
      }

      val timer = TimeSource.Monotonic.markNow()
      val timeout =
        if (config.requestTimeout.seconds > 0) {
          Duration.ofSeconds(config.requestTimeout.seconds)
        } else {
          DEFAULT_TIMEOUT
        }

      val callResult = client.call(config.endpoint.endpointUri, request, timeout)

      val durationSeconds = timer.elapsedNow().inWholeMilliseconds / 1000.0
      metrics.requestDurationHistogram.record(durationSeconds, attrs)

      if (callResult.response == null) {
        logger.log(Level.WARNING, "Cloud function call failed for $dataProviderName: ${callResult.error}")
        metrics.cloudFunctionErrorsCounter.add(
          1,
          Attributes.of(
            EdpValidationMetrics.DATA_PROVIDER_KEY,
            dataProviderName,
            EdpValidationMetrics.ERROR_TYPE_KEY,
            "http_${callResult.httpStatus}",
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

      val response = callResult.response

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

          metrics.deviationFractionHistogram.record(
            evaluation.deviationFraction,
            Attributes.of(
              EdpValidationMetrics.DATA_PROVIDER_KEY,
              dataProviderName,
              EdpValidationMetrics.VERDICT_KEY,
              evaluation.verdict.name,
            ),
          )
          metrics.queriesCounter.add(
            1,
            Attributes.of(
              EdpValidationMetrics.DATA_PROVIDER_KEY,
              dataProviderName,
              EdpValidationMetrics.VERDICT_KEY,
              evaluation.verdict.name,
            ),
          )

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
            ToleranceEvaluator.Verdict.WARNING -> {
              logger.log(
                Level.WARNING,
                "Validation WARNING for $dataProviderName entity ${row.entityId}: " +
                  "reported=${evaluation.reportedCount}, publisher=${evaluation.publisherCount}, " +
                  "deviation=${evaluation.deviationFraction}",
              )
            }
            ToleranceEvaluator.Verdict.PASS -> {
              logger.fine("Validation PASSED for $dataProviderName entity ${row.entityId}")
            }
            ToleranceEvaluator.Verdict.SKIPPED -> {
              logger.info("Validation SKIPPED for $dataProviderName entity ${row.entityId}: below minimum count")
            }
          }
        }
      }
    }

    if (anyFailed) {
      metrics.reportFailuresCounter.add(1, attrs)
    }

    return if (anyFailed) ReportValidationResult.FAILED else ReportValidationResult.PASSED
  }

  companion object {
    private val logger: Logger = Logger.getLogger(EdpValidationPostProcessor::class.java.name)
    private val DEFAULT_TIMEOUT: Duration = Duration.ofSeconds(30)
  }
}
