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

package org.wfanet.measurement.edpaggregator.service

import org.wfanet.measurement.common.ResourceNameParser

internal enum class IdVariable {
  REQUISITION,
  REQUISITION_METADATA,
  REQUISITION_METADATA_ACTION,
  IMPRESSION_METADATA,
  DATA_PROVIDER,
  MODEL_LINE,
  REPORT,
  WORK_ITEM,
}

internal fun ResourceNameParser.assembleName(idMap: Map<IdVariable, String>): String {
  return assembleName(idMap.mapKeys { it.key.name.lowercase() })
}

internal fun ResourceNameParser.parseIdVars(resourceName: String): Map<IdVariable, String>? {
  return parseIdSegments(resourceName)?.mapKeys { IdVariable.valueOf(it.key.uppercase()) }
}
