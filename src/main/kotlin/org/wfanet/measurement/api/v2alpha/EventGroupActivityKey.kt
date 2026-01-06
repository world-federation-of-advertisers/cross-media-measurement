// Copyright 2025 The Cross-Media Measurement Authors
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

/** [ResourceKey] of an EventGroupActivity. */
data class EventGroupActivityKey(
  override val parentKey: EventGroupKey,
  val eventGroupActivityId: String,
) : ChildResourceKey {
  val dataProviderId: String
    get() = parentKey.dataProviderId
  val eventGroupId: String
    get() = parentKey.eventGroupId

  constructor(
    dataProviderId: String,
    eventGroupId: String,
    eventGroupActivityId: String
  ) : this(EventGroupKey(dataProviderId, eventGroupId), eventGroupActivityId)

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.EVENT_GROUP to eventGroupId,
        IdVariable.EVENT_GROUP_ACTIVITY to eventGroupActivityId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<EventGroupActivityKey> {
    private val parser =
      ResourceNameParser(
        "dataProviders/{data_provider}/eventGroups/{event_group}/eventGroupActivities/{event_group_activity}"
      )

    val defaultValue = EventGroupActivityKey("", "", "")

    override fun fromName(resourceName: String): EventGroupActivityKey? {
      return parser.parseIdVars(resourceName)?.let {
        EventGroupActivityKey(
          it.getValue(IdVariable.DATA_PROVIDER),
          it.getValue(IdVariable.EVENT_GROUP),
          it.getValue(IdVariable.EVENT_GROUP_ACTIVITY),
        )
      }
    }
  }
}
