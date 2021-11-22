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

package org.wfanet.panelmatch.client.common

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ResourceKey

/** Set of valid parties -- to allow excluding unknown or unspecified proto enum values. */
private val VALID_PARTIES = setOf(Party.DATA_PROVIDER, Party.MODEL_PROVIDER)

/** Compact representation of one of the two parties along with its id. */
data class Identity(val id: String, val party: Party) {
  init {
    require(party in VALID_PARTIES)
  }

  fun toName(): String {
    return when (party) {
      Party.DATA_PROVIDER -> DataProviderKey(id).toName()
      Party.MODEL_PROVIDER -> ModelProviderKey(id).toName()
      else -> error("Invalid Identity: $this")
    }
  }

  companion object {
    fun fromResourceKey(resourceKey: ResourceKey): Identity {
      return when (resourceKey) {
        is DataProviderKey -> Identity(resourceKey.dataProviderId, Party.DATA_PROVIDER)
        is ModelProviderKey -> Identity(resourceKey.modelProviderId, Party.MODEL_PROVIDER)
        else -> error("Invalid ResourceKey type: $resourceKey")
      }
    }
  }
}
