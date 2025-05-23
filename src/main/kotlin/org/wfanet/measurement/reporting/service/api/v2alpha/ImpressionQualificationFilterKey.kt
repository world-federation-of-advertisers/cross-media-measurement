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

package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of an ImpressionQualificationFilter. */
class ImpressionQualificationFilterKey(val impressionQualificationFilterId: String) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(IdVariable.IMPRESSION_QUALIFICATION_FILTER to impressionQualificationFilterId)
    )
  }

  companion object FACTORY : ResourceKey.Factory<ImpressionQualificationFilterKey> {
    const val PATTERN = "impressionQualificationFilters/{impression_qualification_filter}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): ImpressionQualificationFilterKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return ImpressionQualificationFilterKey(
        idVars.getValue(IdVariable.IMPRESSION_QUALIFICATION_FILTER)
      )
    }
  }
}
