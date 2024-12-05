/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.service

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ResourceKey

class RoleKey(val roleId: String) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(mapOf(IdVariable.ROLE to roleId))
  }

  companion object : ResourceKey.Factory<RoleKey> {
    private val parser = ResourceNameParser("roles/{role}")

    override fun fromName(resourceName: String): RoleKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return RoleKey(idVars.getValue(IdVariable.ROLE))
    }
  }
}
