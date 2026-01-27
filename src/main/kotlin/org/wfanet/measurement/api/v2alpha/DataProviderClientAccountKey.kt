// Copyright 2026 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.common.api.ResourceKey

private val parser =
  ResourceNameParser("dataProviders/{data_provider}/clientAccounts/{client_account}")

/** [ResourceKey] of a DataProvider ClientAccount. */
data class DataProviderClientAccountKey(
  val dataProviderId: String,
  override val clientAccountId: String,
) : ClientAccountKey {
  override val parentKey = DataProviderKey(dataProviderId)

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.CLIENT_ACCOUNT to clientAccountId,
      )
    )
  }

  companion object {
    val defaultValue = DataProviderClientAccountKey("", "")

    fun fromName(resourceName: String): DataProviderClientAccountKey? {
      return parser.parseIdVars(resourceName)?.let {
        DataProviderClientAccountKey(
          it.getValue(IdVariable.DATA_PROVIDER),
          it.getValue(IdVariable.CLIENT_ACCOUNT),
        )
      }
    }
  }
}
