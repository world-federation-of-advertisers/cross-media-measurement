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

import com.google.type.Date
import com.google.type.Interval
import com.google.type.interval
import java.time.DateTimeException
import java.time.Instant
import java.time.ZoneOffset
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.ModelLineCutover

/**
 * A model line and the closed-open event-time interval to read from it.
 *
 * @property modelLine Internal model line used for impression lookup.
 * @property interval Event-time interval to read from [modelLine].
 */
data class ModelLineRoute(val modelLine: String, val interval: Interval)

/**
 * Validated model-line cutover configuration shared by runtime and static configuration consumers.
 *
 * @property externalModelLine Model line used in requisitions and published data availability.
 * @property historicalModelLine Internal model line used before [cutoverInstant].
 * @property replacementModelLine Internal model line used on or after [cutoverInstant].
 * @property cutoverInstant Midnight UTC at which routing switches internal model lines.
 */
class ModelLineCutoverConfig
private constructor(
  val externalModelLine: String,
  val historicalModelLine: String,
  val replacementModelLine: String,
  val cutoverInstant: Instant,
) {
  init {
    validateModelLineName("external_model_line", externalModelLine)
    validateModelLineName("historical_model_line", historicalModelLine)
    validateModelLineName("replacement_model_line", replacementModelLine)
    require(historicalModelLine != replacementModelLine) {
      "historical_model_line and replacement_model_line must differ for " + externalModelLine
    }
  }

  /**
   * Splits [requestedInterval] into the internal model-line routes selected by this cutover.
   *
   * The cutover boundary is closed-open: the before route ends at the boundary and the on-or-after
   * route starts at the same instant.
   *
   * @param requestedInterval closed-open event-time interval requested by a report.
   */
  fun routesFor(requestedInterval: Interval): List<ModelLineRoute> {
    val start = requestedInterval.startTime.toInstant()
    val end = requestedInterval.endTime.toInstant()
    require(start < end) { "Requested interval start time must be before end time" }

    return buildList {
      if (start < cutoverInstant) {
        add(
          ModelLineRoute(
            historicalModelLine,
            interval {
              startTime = start.toProtoTime()
              endTime = minOf(end, cutoverInstant).toProtoTime()
            },
          )
        )
      }
      if (end > cutoverInstant) {
        add(
          ModelLineRoute(
            replacementModelLine,
            interval {
              startTime = maxOf(start, cutoverInstant).toProtoTime()
              endTime = end.toProtoTime()
            },
          )
        )
      }
    }
  }

  companion object {
    /** Converts the Results Fulfiller wire representation to validated configuration. */
    fun from(modelLineCutover: ModelLineCutover): ModelLineCutoverConfig {
      require(modelLineCutover.hasCutoverDate()) {
        "Missing 'cutover_date' for ${modelLineCutover.externalModelLine}"
      }
      return from(
        externalModelLine = modelLineCutover.externalModelLine,
        historicalModelLine = modelLineCutover.historicalModelLine,
        replacementModelLine = modelLineCutover.replacementModelLine,
        cutoverDate = modelLineCutover.cutoverDate,
      )
    }

    /** Creates validated configuration from a static-config representation. */
    fun from(
      externalModelLine: String,
      historicalModelLine: String,
      replacementModelLine: String,
      cutoverDate: Date,
    ): ModelLineCutoverConfig {
      require(cutoverDate.year in 1..9999) {
        "Invalid 'cutover_date' for $externalModelLine: a full year is required"
      }
      val cutoverInstant =
        try {
          cutoverDate.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant()
        } catch (e: DateTimeException) {
          throw IllegalArgumentException(
            "Invalid 'cutover_date' for $externalModelLine: $cutoverDate",
            e,
          )
        }
      return ModelLineCutoverConfig(
        externalModelLine,
        historicalModelLine,
        replacementModelLine,
        cutoverInstant,
      )
    }

    private fun validateModelLineName(fieldName: String, value: String) {
      requireNotNull(ModelLineKey.fromName(value)) {
        "Invalid '$fieldName' in model_line_cutovers: $value"
      }
    }
  }
}

/** Converts this Results Fulfiller wire value to validated cutover configuration. */
fun ModelLineCutover.toModelLineCutoverConfig(): ModelLineCutoverConfig =
  ModelLineCutoverConfig.from(this)

/** Validates a set of model-line cutovers and their interaction with legacy mapping keys. */
object ModelLineCutoverValidator {
  /**
   * Validates [cutovers].
   *
   * @param legacyExternalModelLines external model lines already handled by a legacy mapping.
   */
  fun validate(
    cutovers: List<ModelLineCutoverConfig>,
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
