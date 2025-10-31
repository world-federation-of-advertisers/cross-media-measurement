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
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.EventTemplateField
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.copy

object Normalization {
  private val fieldValueComparator: Comparator<EventTemplateField.FieldValue> = compareBy {
    when (it.selectorCase) {
      EventTemplateField.FieldValue.SelectorCase.STRING_VALUE -> it.stringValue
      EventTemplateField.FieldValue.SelectorCase.ENUM_VALUE -> it.enumValue
      EventTemplateField.FieldValue.SelectorCase.BOOL_VALUE -> it.boolValue
      EventTemplateField.FieldValue.SelectorCase.FLOAT_VALUE -> it.floatValue
      EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET -> error("value not set")
    }
  }

  private val groupingComparator: Comparator<EventTemplateField> = compareBy { it.path }

  private val eventFilterTermComparator: Comparator<EventTemplateField> =
    compareBy { it: EventTemplateField -> it.path }
      .thenComparing({ it.value }, fieldValueComparator)

  private val eventFilterComparator: Comparator<EventFilter> =
    compareBy(Ordering.from(eventFilterTermComparator).lexicographical()) { it.termsList }

  /** Returns a sorted copy of [groupings]. */
  fun sortGroupings(groupings: Iterable<EventTemplateField>): List<EventTemplateField> {
    return groupings.sortedWith(groupingComparator)
  }

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
    val serialization = buildString {
      when (metricFrequencySpec.selectorCase) {
        MetricFrequencySpec.SelectorCase.WEEKLY -> {
          append(MetricFrequencySpec.WEEKLY_FIELD_NUMBER)
          append(":")
          appendLine(metricFrequencySpec.weeklyValue)
        }
        MetricFrequencySpec.SelectorCase.TOTAL -> {
          append(MetricFrequencySpec.TOTAL_FIELD_NUMBER)
          append(":")
          appendLine(metricFrequencySpec.total)
        }
        MetricFrequencySpec.SelectorCase.SELECTOR_NOT_SET -> Unit
      }
    }

    return fingerprint(serialization)
  }

  /** Computes the fingerprint of a grouping slice. */
  fun computeFingerprint(
    eventMessageVersion: Int,
    sortedGroupings: Iterable<EventTemplateField>,
  ): Long {
    val serialization = buildString {
      appendLine(eventMessageVersion)
      appendLine("[")
      for (grouping in sortedGroupings) {
        appendLine("{")
        append(serialize(grouping))
        appendLine("},")
      }
      appendLine("]")
    }

    return fingerprint(serialization)
  }

  /** Computes the fingerprint of [EventFilter]s. */
  fun computeFingerprint(normalizedEventFilters: Iterable<EventFilter>): Long {
    val serialization = buildString {
      appendLine("[")
      for (eventFilter in normalizedEventFilters) {
        appendLine("{")

        append(EventFilter.TERMS_FIELD_NUMBER)
        append(":")
        appendLine("[")
        for (term in eventFilter.termsList) {
          appendLine("{")
          append(serialize(term))
          appendLine("},")
        }
        appendLine("]")

        appendLine("},")
      }
      appendLine("]")
    }

    return fingerprint(serialization)
  }

  private fun serialize(field: EventTemplateField) = buildString {
    append(EventTemplateField.PATH_FIELD_NUMBER)
    append(":")
    appendLine(field.path)
    when (field.value.selectorCase) {
      EventTemplateField.FieldValue.SelectorCase.STRING_VALUE -> {
        append(EventTemplateField.FieldValue.STRING_VALUE_FIELD_NUMBER)
        append(":")
        appendLine(field.value.stringValue.replace("\n", "\\n"))
      }
      EventTemplateField.FieldValue.SelectorCase.ENUM_VALUE -> {
        append(EventTemplateField.FieldValue.ENUM_VALUE_FIELD_NUMBER)
        append(":")
        appendLine(field.value.enumValue)
      }
      EventTemplateField.FieldValue.SelectorCase.BOOL_VALUE -> {
        append(EventTemplateField.FieldValue.BOOL_VALUE_FIELD_NUMBER)
        append(":")
        appendLine(field.value.boolValue)
      }
      EventTemplateField.FieldValue.SelectorCase.FLOAT_VALUE -> {
        append(EventTemplateField.FieldValue.FLOAT_VALUE_FIELD_NUMBER)
        append(":")
        appendLine(field.value.boolValue)
      }
      EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET -> Unit
    }
  }

  private fun fingerprint(serialization: String): Long {
    return Hashing.farmHashFingerprint64().hashString(serialization, Charsets.UTF_8).asLong()
  }
}
