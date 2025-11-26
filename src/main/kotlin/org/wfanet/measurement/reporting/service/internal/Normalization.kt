/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.internal

import com.google.common.collect.Ordering
import com.google.common.hash.Hashing
import java.util.logging.Logger
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.EventTemplateField
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.copy

object Normalization {
  private val logger = Logger.getLogger(this::class.java.name)

  private val fieldValueComparator: Comparator<EventTemplateField.FieldValue> = compareBy {
    when (it.selectorCase) {
      EventTemplateField.FieldValue.SelectorCase.STRING_VALUE -> it.stringValue
      EventTemplateField.FieldValue.SelectorCase.ENUM_VALUE -> it.enumValue
      EventTemplateField.FieldValue.SelectorCase.BOOL_VALUE -> it.boolValue
      EventTemplateField.FieldValue.SelectorCase.FLOAT_VALUE -> it.floatValue
      EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET -> error("value not set")
    }
  }

  private val eventFilterTermComparator: Comparator<EventTemplateField> =
    compareBy { it: EventTemplateField -> it.path }
      .thenComparing({ it.value }, fieldValueComparator)

  private val eventFilterComparator: Comparator<EventFilter> =
    compareBy(Ordering.from(eventFilterTermComparator).lexicographical()) { it.termsList }

  /** Returns a normalized copy of [eventFilters]. */
  fun normalizeEventFilters(eventFilters: Iterable<EventFilter>): List<EventFilter> {
    return eventFilters
      .map {
        it.copy {
          val normalizedTerms = terms.sortedWith(eventFilterTermComparator)
          terms.clear()
          terms += normalizedTerms
        }
      }
      .sortedWith(eventFilterComparator)
  }

  /** Computes the fingerprint of [metricFrequencySpec]. */
  fun computeFingerprint(metricFrequencySpec: MetricFrequencySpec): Long {
    val serialization = FingerprintSerialization.serialize { value(metricFrequencySpec) }

    return fingerprint(serialization)
  }

  /** Computes the fingerprint of a grouping. */
  fun computeFingerprint(
    eventMessageVersion: Int,
    grouping: ReportingSetResult.Dimension.Grouping,
  ): Long {
    require(eventMessageVersion > 0) { "Invalid event message version" }
    val serialization =
      FingerprintSerialization.serialize {
        value { value(eventMessageVersion) }
        value(grouping)
      }

    return fingerprint(serialization)
  }

  /** Computes the fingerprint of [normalizedEventFilters]. */
  fun computeFingerprint(normalizedEventFilters: Iterable<EventFilter>): Long {
    val serialization =
      FingerprintSerialization.serialize { value { list(normalizedEventFilters) } }

    return fingerprint(serialization)
  }

  private fun FingerprintSerialization.value(fieldValue: EventTemplateField.FieldValue) {
    when (fieldValue.selectorCase) {
      EventTemplateField.FieldValue.SelectorCase.STRING_VALUE ->
        field(EventTemplateField.FieldValue.STRING_VALUE_FIELD_NUMBER) {
          value(fieldValue.stringValue)
        }

      EventTemplateField.FieldValue.SelectorCase.ENUM_VALUE ->
        field(EventTemplateField.FieldValue.ENUM_VALUE_FIELD_NUMBER) { value(fieldValue.enumValue) }

      EventTemplateField.FieldValue.SelectorCase.BOOL_VALUE ->
        field(EventTemplateField.FieldValue.BOOL_VALUE_FIELD_NUMBER) { value(fieldValue.boolValue) }

      EventTemplateField.FieldValue.SelectorCase.FLOAT_VALUE ->
        field(EventTemplateField.FieldValue.FLOAT_VALUE_FIELD_NUMBER) {
          value(fieldValue.floatValue)
        }

      EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET -> error("selector not set")
    }
  }

  private fun FingerprintSerialization.value(field: EventTemplateField) {
    field(EventTemplateField.PATH_FIELD_NUMBER) { value(field.path) }
    field(EventTemplateField.VALUE_FIELD_NUMBER) { value(field.value) }
  }

  private fun FingerprintSerialization.ValueSerialization.list(
    normalizedEventFilters: Iterable<EventFilter>
  ) {
    list {
      for (eventFilter in normalizedEventFilters) {
        listItem {
          message {
            field(EventFilter.TERMS_FIELD_NUMBER) {
              list {
                for (term in eventFilter.termsList) {
                  listItem { message { value(term) } }
                }
              }
            }
          }
        }
      }
    }
  }

  private fun FingerprintSerialization.value(grouping: ReportingSetResult.Dimension.Grouping) {
    field(ReportingSetResult.Dimension.Grouping.VALUE_BY_PATH_FIELD_NUMBER) {
      for ((path, fieldValue) in grouping.valueByPathMap.toSortedMap()) {
        message {
          field(1) { value(path) }
          field(2) { message { value(fieldValue) } }
        }
      }
    }
  }

  private fun FingerprintSerialization.value(metricFrequencySpec: MetricFrequencySpec) {
    when (metricFrequencySpec.selectorCase) {
      MetricFrequencySpec.SelectorCase.WEEKLY ->
        field(MetricFrequencySpec.WEEKLY_FIELD_NUMBER) { value(metricFrequencySpec.weeklyValue) }
      MetricFrequencySpec.SelectorCase.TOTAL ->
        field(MetricFrequencySpec.TOTAL_FIELD_NUMBER) { value(metricFrequencySpec.total) }
      MetricFrequencySpec.SelectorCase.SELECTOR_NOT_SET -> error("selector not set")
    }
  }

  /** Rudimentary one-way serialization DSL for fingerprinting. */
  private class FingerprintSerialization
  private constructor(private val stringBuilder: StringBuilder) {
    @DslMarker @Target(AnnotationTarget.CLASS, AnnotationTarget.TYPE) private annotation class Dsl

    fun value(fill: @Dsl ValueSerialization.() -> Unit) {
      ValueSerialization(stringBuilder).fill()
      stringBuilder.appendLine()
    }

    fun field(number: Int, fill: @Dsl ValueSerialization.() -> Unit) {
      stringBuilder.append(number).append(":")
      ValueSerialization(stringBuilder).fill()
      stringBuilder.appendLine()
    }

    class ValueSerialization(private val stringBuilder: StringBuilder) {
      fun message(fill: @Dsl FingerprintSerialization.() -> Unit) {
        stringBuilder.appendLine("{")
        FingerprintSerialization(stringBuilder).fill()
        stringBuilder.append("}")
      }

      fun list(fill: @Dsl ListSerialization.() -> Unit) {
        stringBuilder.appendLine("[")
        ListSerialization(stringBuilder).fill()
        stringBuilder.append("]")
      }

      fun value(stringValue: String) {
        // Escape newlines since they are significant in this serialization format.
        stringBuilder.append(stringValue.replace("\n", "\\n"))
      }

      fun value(boolValue: Boolean) {
        stringBuilder.append(boolValue)
      }

      fun value(floatValue: Float) {
        stringBuilder.append(floatValue)
      }

      fun value(intValue: Int) {
        stringBuilder.append(intValue)
      }
    }

    class ListSerialization(private val stringBuilder: StringBuilder) {
      fun listItem(fill: @Dsl ValueSerialization.() -> Unit) {
        ValueSerialization(stringBuilder).fill()
        stringBuilder.appendLine(",")
      }
    }

    companion object {
      fun serialize(fill: @Dsl FingerprintSerialization.() -> Unit): String {
        return buildString { FingerprintSerialization(this).fill() }
      }
    }
  }

  private fun fingerprint(serialization: String): Long {
    logger.fine { "Computing fingerprint of serialization \n$serialization" }
    return Hashing.farmHashFingerprint64().hashString(serialization, Charsets.UTF_8).asLong()
  }
}
