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

  val eventTemplateFieldComparator: Comparator<EventTemplateField> =
    compareBy { it: EventTemplateField -> it.path }
      .thenComparing({ it.value }, fieldValueComparator)

  val eventFilterComparator: Comparator<EventFilter> =
    compareBy(Ordering.from(eventTemplateFieldComparator).lexicographical()) { it.termsList }
}
