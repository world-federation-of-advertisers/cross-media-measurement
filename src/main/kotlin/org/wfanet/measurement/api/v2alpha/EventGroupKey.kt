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
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

private val parser = ResourceNameParser("dataProviders/{data_provider}/eventGroups/{event_group}")

/** [ResourceKey] of an EventGroup. */
data class EventGroupKey(val dataProviderId: String, val eventGroupId: String) : ChildResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(IdVariable.DATA_PROVIDER to dataProviderId, IdVariable.EVENT_GROUP to eventGroupId)
    )
  }

  override val parentKey = DataProviderKey(dataProviderId)

  companion object FACTORY : ResourceKey.Factory<EventGroupKey> {
    val defaultValue = EventGroupKey("", "")

    override fun fromName(resourceName: String): EventGroupKey? {
      return parser.parseIdVars(resourceName)?.let {
        EventGroupKey(it.getValue(IdVariable.DATA_PROVIDER), it.getValue(IdVariable.EVENT_GROUP))
      }
    }
  }
}
