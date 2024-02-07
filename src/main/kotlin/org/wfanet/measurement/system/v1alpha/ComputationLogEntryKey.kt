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

package org.wfanet.measurement.system.v1alpha

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ResourceKey

private val parser =
  ResourceNameParser(
    "computations/{computation}/participants/{duchy}/logEntries/{computation_log_entry}"
  )

/** [ResourceKey] of a ComputationLogEntry. */
data class ComputationLogEntryKey(
  val computationId: String,
  val duchyId: String,
  val computationLogEntryId: String,
) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.COMPUTATION to computationId,
        IdVariable.DUCHY to duchyId,
        IdVariable.COMPUTATION_LOG_ENTRY to computationLogEntryId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ComputationLogEntryKey> {
    val defaultValue = ComputationLogEntryKey("", "", "")

    override fun fromName(resourceName: String): ComputationLogEntryKey? {
      return parser.parseIdVars(resourceName)?.let {
        ComputationLogEntryKey(
          it.getValue(IdVariable.COMPUTATION),
          it.getValue(IdVariable.DUCHY),
          it.getValue(IdVariable.COMPUTATION_LOG_ENTRY),
        )
      }
    }
  }
}
