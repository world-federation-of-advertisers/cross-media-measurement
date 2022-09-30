// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser

internal enum class IdVariable {
  RECURRING_EXCHANGE,
  EVENT_GROUP,
  EVENT_GROUP_METADATA_DESCRIPTOR,
  EXCHANGE,
  EXCHANGE_STEP,
  EXCHANGE_STEP_ATTEMPT,
  DATA_PROVIDER,
  MODEL_PROVIDER,
  REQUISITION,
  PROTOCOL_CONFIG,
  DUCHY,
  MEASUREMENT,
  MEASUREMENT_CONSUMER,
  CERTIFICATE,
  ACCOUNT,
  API_KEY,
}

internal fun ResourceNameParser.assembleName(idMap: Map<IdVariable, String>): String {
  return assembleName(idMap.mapKeys { it.key.name.lowercase() })
}

internal fun ResourceNameParser.parseIdVars(resourceName: String): Map<IdVariable, String>? {
  return parseIdSegments(resourceName)?.mapKeys { IdVariable.valueOf(it.key.uppercase()) }
}
