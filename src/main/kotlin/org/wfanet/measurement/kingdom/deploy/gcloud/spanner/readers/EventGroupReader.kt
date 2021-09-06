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

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.EventGroup

private val BASE_SQL =
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

class EventGroupReader : BaseSpannerReader<EventGroup>() {
  override val builder = Statement.newBuilder(BASE_SQL)
  data class Result(val eventGroup: EventGroup, val eventGroupId: Long)

  /** Fills [builder], returning this [RequisitionReader] for chaining. */
  fun fillStatementBuilder(fill: Statement.Builder.() -> Unit): EventGroupReader {
    builder.fill()
    return this
  }

  fun bindWhereClause(dataProviderId: Long, providedEventGroupId: String): EventGroupReader {
    return fillStatementBuilder {
      appendClause(
        """
        WHERE EventGroups.DataProviderId = @data_provider_id
        AND EventGroups.ProvidedEventGroupId = @provided_event_group_id
        """.trimIndent()
      )
      bind("data_provider_id" to dataProviderId)
      bind("provided_event_group_id" to providedEventGroupId)
    }
  }

  suspend fun readByExternalId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalEventGroupId: Long,
  ): EventGroup? {
    val externalEventGroupIdParam = "externalEventGroupId"

    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            ExternalEventGroupId = @$externalEventGroupIdParam
          """.trimIndent()
        )
        bind(externalEventGroupIdParam to externalEventGroupId)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  //   override suspend fun translate(struct: Struct): Result =
  //     Result(buildEventGroup(struct), struct.getLong("EventGroupId"))
  override suspend fun translate(struct: Struct): EventGroup = buildEventGroup(struct)

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
