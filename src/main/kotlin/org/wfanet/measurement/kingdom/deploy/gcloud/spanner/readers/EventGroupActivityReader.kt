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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.type.Date
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.EventGroupActivity
import org.wfanet.measurement.internal.kingdom.eventGroupActivity

class EventGroupActivityReader : BaseSpannerReader<EventGroupActivityReader.Result>() {
  data class Result(
    val eventGroupActivity: EventGroupActivity,
    val internalDataProviderId: InternalId,
    val internalEventGroupId: InternalId,
    val internalEventGroupActivityId: InternalId,
  )

  override val builder: Statement.Builder = Statement.newBuilder(BASE_SQL)

  /** Fills [builder], returning this [EventGroupActivityReader] for chaining. */
  fun fillStatementBuilder(fill: Statement.Builder.() -> Unit): EventGroupActivityReader {
    builder.fill()
    return this
  }

  suspend fun batchReadByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    dataProviderId: InternalId,
    externalEventGroupId: ExternalId,
    activityDates: Collection<Date>,
  ): Map<Date, Result> {
    return buildMap {
      fillStatementBuilder {
          appendClause(
            """
            WHERE EventGroups.DataProviderId = @${Params.DATA_PROVIDER_ID}
            AND EventGroups.ExternalEventGroupId = @${Params.EXTERNAL_EVENT_GROUP_ID}
            AND   EventGroupActivities.ActivityDate IN UNNEST(@${Params.ACTIVITY_DATES})
          """
              .trimIndent()
          )
          bind(Params.DATA_PROVIDER_ID to dataProviderId)
          bind(Params.EXTERNAL_EVENT_GROUP_ID to externalEventGroupId)
          bind(Params.ACTIVITY_DATES).toDateArray(activityDates.map { it.toCloudDate() })
        }
        .execute(readContext)
        .collect { put(it.eventGroupActivity.date, it) }
    }
  }

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildEventGroupActivity(struct),
      InternalId(struct.getLong("DataProviderId")),
      InternalId(struct.getLong("EventGroupId")),
      InternalId(struct.getLong("EventGroupActivityId")),
    )

  private fun buildEventGroupActivity(struct: Struct): EventGroupActivity {
    return eventGroupActivity {
      externalEventGroupId = struct.getLong("ExternalEventGroupId")
      date = struct.getDate("ActivityDate").toProtoDate()
      createTime = struct.getTimestamp("CreateTime").toProto()
    }
  }

  companion object {
    private val BASE_SQL =
      """
      SELECT
        EventGroupActivities.DataProviderId,
        EventGroupActivities.EventGroupId,
        EventGroupActivities.EventGroupActivityId,
        EventGroupActivities.ActivityDate,
        EventGroupActivities.CreateTime,
        EventGroups.ExternalEventGroupId,
      FROM
        EventGroupActivities
        JOIN EventGroups USING (DataProviderId, EventGroupId)
      """
        .trimIndent()

    private object Params {
      const val DATA_PROVIDER_ID = "dataProviderId"
      const val EXTERNAL_EVENT_GROUP_ID = "externalEventGroupId"
      const val ACTIVITY_DATES = "externalEventGroupActivityIds"
    }
  }
}
