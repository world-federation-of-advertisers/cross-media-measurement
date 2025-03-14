/*
 * Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import java.util.Locale
import org.wfanet.measurement.common.ResourceNameParser

internal enum class IdVariable {
  BASIC_REPORT,
  IMPRESSION_QUALIFICATION_FILTER,
  DATA_PROVIDER,
  EVENT_GROUP,
  MEASUREMENT_CONSUMER,
  REPORTING_SET,
  METRIC,
  METRIC_CALCULATION_SPEC,
  REPORT,
  REPORT_SCHEDULE,
  REPORT_SCHEDULE_ITERATION,
}

internal fun ResourceNameParser.assembleName(idMap: Map<IdVariable, String>): String {
  return assembleName(idMap.mapKeys { it.key.name.lowercase(Locale.getDefault()) })
}

internal fun ResourceNameParser.parseIdVars(resourceName: String): Map<IdVariable, String>? {
  return parseIdSegments(resourceName)?.mapKeys {
    IdVariable.valueOf(it.key.uppercase(Locale.getDefault()))
  }
}
