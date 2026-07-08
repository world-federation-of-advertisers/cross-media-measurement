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

package org.wfanet.measurement.reporting.service.internal

import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.common.cel.CelValueEncoding
import org.wfanet.measurement.internal.reporting.v2.EventTemplateField as InternalEventTemplateField

/**
 * Encodes reporting-internal `EventTemplateField.FieldValue`s as CEL value literals.
 *
 * Shared between the request-time CEL builder ([org.wfanet.measurement.reporting.service.api.v2alpha
 * .BasicReportTransformations.buildCelExpression]) and the server-startup validator
 * ([ImpressionQualificationFilterMapping]) so both sites agree on how each `selectorCase` renders
 * into CEL.
 */
object EventTemplateFieldCelEncoding {
  fun toCelValue(
    value: InternalEventTemplateField.FieldValue,
    fieldInfo: EventMessageDescriptor.EventTemplateFieldInfo,
  ): String {
    return when (value.selectorCase) {
      InternalEventTemplateField.FieldValue.SelectorCase.STRING_VALUE ->
        CelValueEncoding.toCelStringLiteral(value.stringValue)
      InternalEventTemplateField.FieldValue.SelectorCase.ENUM_VALUE ->
        checkNotNull(fieldInfo.enumType?.findValueByName(value.enumValue)).number.toString()
      InternalEventTemplateField.FieldValue.SelectorCase.BOOL_VALUE -> value.boolValue.toString()
      InternalEventTemplateField.FieldValue.SelectorCase.FLOAT_VALUE ->
        CelValueEncoding.toCelNumericLiteral(value.floatValue)
      InternalEventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET -> error("No field value")
    }
  }
}
