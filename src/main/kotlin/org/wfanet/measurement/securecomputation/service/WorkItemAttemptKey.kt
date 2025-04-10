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

package org.wfanet.measurement.securecomputation.service

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

class WorkItemAttemptKey(val workItemId: String, val workItemAttemptId: String) : ChildResourceKey {

  override fun toName(): String {
    return parser.assembleName(
      mapOf(IdVariable.WORK_ITEM to workItemId, IdVariable.WORK_ITEM_ATTEMPT to workItemAttemptId)
    )
  }

  override val parentKey = WorkItemKey(workItemId)

  companion object : ResourceKey.Factory<WorkItemAttemptKey> {
    private val parser =
      ResourceNameParser("workItems/{work_item}/workItemAttempts/{work_item_attempt}")

    override fun fromName(resourceName: String): WorkItemAttemptKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return WorkItemAttemptKey(
        idVars.getValue(IdVariable.WORK_ITEM),
        idVars.getValue(IdVariable.WORK_ITEM_ATTEMPT),
      )
    }
  }
}
