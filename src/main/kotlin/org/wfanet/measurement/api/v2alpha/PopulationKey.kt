/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of a Population. */
data class PopulationKey(override val parentKey: DataProviderKey, val populationId: String) :
  ChildResourceKey {
  constructor(
    dataProviderId: String,
    populationId: String,
  ) : this(DataProviderKey(dataProviderId), populationId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(IdVariable.DATA_PROVIDER to dataProviderId, IdVariable.POPULATION to populationId)
    )
  }

  companion object FACTORY : ResourceKey.Factory<PopulationKey> {
    val parser = ResourceNameParser("dataProviders/{data_provider}/populations/{population}")
    val defaultValue = PopulationKey("", "")

    override fun fromName(resourceName: String): PopulationKey? {
      return parser.parseIdVars(resourceName)?.let {
        PopulationKey(it.getValue(IdVariable.DATA_PROVIDER), it.getValue(IdVariable.POPULATION))
      }
    }
  }
}
