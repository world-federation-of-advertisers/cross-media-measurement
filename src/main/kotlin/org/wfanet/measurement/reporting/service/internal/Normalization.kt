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
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.EventTemplateField
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
}
