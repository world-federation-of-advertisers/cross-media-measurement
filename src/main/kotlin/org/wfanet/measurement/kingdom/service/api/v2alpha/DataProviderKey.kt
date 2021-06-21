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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser

private val parser = ResourceNameParser("dataProviders/{data_provider}")

/** [DataProviderKey] of a Data Provider. */
data class DataProviderKey(val dataProviderId: String) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(mapOf(IdVariable.DATA_PROVIDER to dataProviderId))
  }

  companion object {
    val defaultValue = DataProviderKey("")

    fun fromName(resourceName: String): DataProviderKey? {
      return parser.parseIdVars(resourceName)?.let {
        DataProviderKey(it.getValue(IdVariable.DATA_PROVIDER))
      }
    }
  }
}
