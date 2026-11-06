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

package org.wfanet.measurement.edpaggregator

import java.time.DateTimeException
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.config.edpaggregator.ModelLineCutoverConfig

/** Validated model-line cutover configuration shared by EDPA components. */
data class ModelLineCutover(
  val externalModelLine: String,
  val historicalModelLine: String,
  val replacementModelLine: String,
  val cutoverDate: LocalDate,
  val legacyRequiredThroughDate: LocalDate,
  val maximumReportDuration: Duration,
) {
  val cutoverInstant: Instant = cutoverDate.atStartOfDay(ZoneOffset.UTC).toInstant()
  val legacyRequiredEndExclusive: Instant =
    legacyRequiredThroughDate.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant()

  init {
    validateModelLineName("external_model_line", externalModelLine)
    validateModelLineName("historical_model_line", historicalModelLine)
    validateModelLineName("replacement_model_line", replacementModelLine)
    require(historicalModelLine != replacementModelLine) {
      "historical_model_line and replacement_model_line must differ for $externalModelLine"
    }
    require(!maximumReportDuration.isZero && !maximumReportDuration.isNegative) {
      "maximum_report_duration must be positive for $externalModelLine"
    }
    require(
      maximumReportDuration.nano == 0 && maximumReportDuration.seconds % SECONDS_PER_DAY == 0L
    ) {
      "maximum_report_duration must be a whole number of days for $externalModelLine"
    }
    val minimumLegacyRequiredThroughDate =
      cutoverDate.plusDays(maximumReportDuration.seconds / SECONDS_PER_DAY)
    require(legacyRequiredThroughDate >= minimumLegacyRequiredThroughDate) {
      "legacy_required_through_date must be on or after cutover_date plus " +
        "maximum_report_duration for $externalModelLine"
    }
  }

  /** Selects one internal model line for the complete Report. */
  fun selectModelLine(reportStart: Instant): String =
    if (reportStart < cutoverInstant) historicalModelLine else replacementModelLine

  companion object {
    fun from(config: ModelLineCutoverConfig): ModelLineCutover {
      require(config.hasCutoverDate()) { "Missing 'cutover_date' for ${config.externalModelLine}" }
      require(config.hasLegacyRequiredThroughDate()) {
        "Missing 'legacy_required_through_date' for ${config.externalModelLine}"
      }
      require(config.hasMaximumReportDuration()) {
        "Missing 'maximum_report_duration' for ${config.externalModelLine}"
      }
      return ModelLineCutover(
        externalModelLine = config.externalModelLine,
        historicalModelLine = config.historicalModelLine,
        replacementModelLine = config.replacementModelLine,
        cutoverDate =
          config.cutoverDate.toValidatedLocalDate("cutover_date", config.externalModelLine),
        legacyRequiredThroughDate =
          config.legacyRequiredThroughDate.toValidatedLocalDate(
            "legacy_required_through_date",
            config.externalModelLine,
          ),
        maximumReportDuration =
          Duration.ofSeconds(
            config.maximumReportDuration.seconds,
            config.maximumReportDuration.nanos.toLong(),
          ),
      )
    }

    private fun com.google.type.Date.toValidatedLocalDate(
      fieldName: String,
      externalModelLine: String,
    ): LocalDate {
      require(year in 1..9999) {
        "Invalid '$fieldName' for $externalModelLine: a full year is required"
      }
      return try {
        toLocalDate()
      } catch (e: DateTimeException) {
        throw IllegalArgumentException("Invalid '$fieldName' for $externalModelLine: $this", e)
      }
    }

    private fun validateModelLineName(fieldName: String, value: String) {
      requireNotNull(ModelLineKey.fromName(value)) {
        "Invalid '$fieldName' in model_line_cutovers: $value"
      }
    }

    private const val SECONDS_PER_DAY = 86400L
  }
}

/** Validates cutovers and their interaction with legacy model-line mappings. */
object ModelLineCutoverValidator {
  fun validate(
    cutovers: List<ModelLineCutover>,
    legacyExternalModelLines: Set<String> = emptySet(),
  ) {
    val duplicateExternalModelLines =
      cutovers.groupingBy { it.externalModelLine }.eachCount().filterValues { it > 1 }.keys
    require(duplicateExternalModelLines.isEmpty()) {
      "Duplicate model-line cutover(s) for: $duplicateExternalModelLines"
    }
    for (cutover in cutovers) {
      require(cutover.externalModelLine !in legacyExternalModelLines) {
        "Model line ${cutover.externalModelLine} is configured in both model_line_map and " +
          "model_line_cutovers"
      }
    }
  }
}
