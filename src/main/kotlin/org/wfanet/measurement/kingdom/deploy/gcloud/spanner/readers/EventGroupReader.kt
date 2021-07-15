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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.internal.kingdom.EventGroup

class EventGroupReader : SpannerReader<EventGroupReader.Result>() {
  data class Result(val eventGroup: EventGroup, val eventGroupId: Long)

  override val baseSql: String =
    """
    SELECT
      EventGroups.EventGroupId,
      EventGroups.ExternalEventGroupId,
      EventGroups.MeasurementConsumerId,
      EventGroups.DataProviderId,
      EventGroups.ProvidedEventGroupId,
      EventGroups.CreateTime,
      MeasurementConsumers.ExternalMeasurementConsumerId,
      DataProviders.ExternalDataProviderId
    FROM EventGroups
    JOIN MeasurementConsumers USING (MeasurementConsumerId)
    JOIN DataProviders USING (DataProviderId)
    """.trimIndent()

  override val externalIdColumn: String = "EventGroups.ExternalEventGroupId"

  override suspend fun translate(struct: Struct): Result =
    Result(buildEventGroup(struct), struct.getLong("EventGroupId"))

  private fun buildEventGroup(struct: Struct): EventGroup =
    EventGroup.newBuilder()
      .apply {
        externalEventGroupId = struct.getLong("ExternalEventGroupId")
        externalDataProviderId = struct.getLong("ExternalDataProviderId")
        externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
        providedEventGroupId = struct.getString("ProvidedEventGroupId")
        createTime = struct.getTimestamp("CreateTime").toProto()
      }
      .build()
}
