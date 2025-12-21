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

import com.google.cloud.Timestamp as CloudTimestamp
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.type.interval
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.getNullableTimestamp
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupDetails
import org.wfanet.measurement.internal.kingdom.MediaType
import org.wfanet.measurement.internal.kingdom.eventGroup

class EventGroupReader : BaseSpannerReader<EventGroupReader.Result>() {
  data class Result(
    val eventGroup: EventGroup,
    val internalEventGroupId: InternalId,
    val internalDataProviderId: InternalId,
    val createRequestId: String?,
  )

  override val builder: Statement.Builder = Statement.newBuilder(BASE_SQL)

  /** Fills [builder], returning this [RequisitionReader] for chaining. */
  fun fillStatementBuilder(fill: Statement.Builder.() -> Unit): EventGroupReader {
    builder.fill()
    return this
  }

  suspend fun readByCreateRequestId(
    readContext: AsyncDatabaseClient.ReadContext,
    dataProviderId: InternalId,
    createRequestId: String,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            DataProviderId = @${Params.DATA_PROVIDER_ID}
            AND CreateRequestId = @${Params.CREATE_REQUEST_ID}
          """
            .trimIndent()
        )
        bind(Params.DATA_PROVIDER_ID to dataProviderId)
        bind(Params.CREATE_REQUEST_ID to createRequestId)
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByCreateRequestIds(
    readContext: AsyncDatabaseClient.ReadContext,
    dataProviderId: InternalId,
    createRequestIds: Collection<String>,
  ): Map<String, Result> {
    return buildMap {
      fillStatementBuilder {
          appendClause(
            """
          WHERE
            DataProviderId = @${Params.DATA_PROVIDER_ID}
            AND CreateRequestId IN UNNEST (@${Params.CREATE_REQUEST_IDS})
          """
              .trimIndent()
          )
          bind(Params.DATA_PROVIDER_ID to dataProviderId)
          bind(Params.CREATE_REQUEST_IDS).toStringArray(createRequestIds)
        }
        .execute(readContext)
        .collect { result ->
          if (!result.createRequestId.isNullOrEmpty()) {
            put(result.createRequestId, result)
          }
        }
    }
  }

  suspend fun readByDataProvider(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderId: ExternalId,
    externalEventGroupId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            ExternalEventGroupId = @${Params.EXTERNAL_EVENT_GROUP_ID}
            AND ExternalDataProviderId = @${Params.EXTERNAL_DATA_PROVIDER_ID}
          """
            .trimIndent()
        )
        bind(Params.EXTERNAL_EVENT_GROUP_ID to externalEventGroupId)
        bind(Params.EXTERNAL_DATA_PROVIDER_ID to externalDataProviderId)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun batchReadByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderId: ExternalId,
    externalEventGroupIds: Collection<ExternalId>,
  ): Map<ExternalId, Result> {
    return buildMap {
      fillStatementBuilder {
          appendClause(
            """
          WHERE ExternalDataProviderId = @${Params.EXTERNAL_DATA_PROVIDER_ID}
            AND ExternalEventGroupId IN UNNEST(@${Params.EXTERNAL_EVENT_GROUP_IDS})
          """
              .trimIndent()
          )
          bind(Params.EXTERNAL_EVENT_GROUP_IDS).toInt64Array(externalEventGroupIds.map { it.value })
          bind(Params.EXTERNAL_DATA_PROVIDER_ID to externalDataProviderId)
        }
        .execute(readContext)
        .collect { put(ExternalId(it.eventGroup.externalEventGroupId), it) }
    }
  }

  suspend fun readByMeasurementConsumer(
    readContext: AsyncDatabaseClient.ReadContext,
    externalMeasurementConsumerId: ExternalId,
    externalEventGroupId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
        WHERE
          ExternalMeasurementConsumerId = @${Params.EXTERNAL_MEASUREMENT_CONSUMER_ID}
          AND ExternalEventGroupId = @${Params.EXTERNAL_EVENT_GROUP_ID}
        """
            .trimIndent()
        )
        bind(Params.EXTERNAL_MEASUREMENT_CONSUMER_ID to externalMeasurementConsumerId)
        bind(Params.EXTERNAL_EVENT_GROUP_ID to externalEventGroupId)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildEventGroup(struct),
      InternalId(struct.getLong("EventGroupId")),
      InternalId(struct.getLong("DataProviderId")),
      if (struct.isNull("CreateRequestId")) null else struct.getString("CreateRequestId"),
    )

  private fun buildEventGroup(struct: Struct): EventGroup {
    val dataAvailabilityStartTime: CloudTimestamp? =
      struct.getNullableTimestamp("DataAvailabilityStartTime")
    val dataAvailabilityEndTime: CloudTimestamp? =
      struct.getNullableTimestamp("DataAvailabilityEndTime")

    return eventGroup {
      externalEventGroupId = struct.getLong("ExternalEventGroupId")
      externalDataProviderId = struct.getLong("ExternalDataProviderId")
      externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
      if (!struct.isNull("ProvidedEventGroupId")) {
        providedEventGroupId = struct.getString("ProvidedEventGroupId")
      }
      createTime = struct.getTimestamp("CreateTime").toProto()
      updateTime = struct.getTimestamp("UpdateTime").toProto()
      mediaTypes += struct.getProtoEnumList("MediaTypes", MediaType::forNumber)
      if (dataAvailabilityStartTime != null) {
        dataAvailabilityInterval = interval {
          startTime = dataAvailabilityStartTime.toProto()
          if (dataAvailabilityEndTime != null) {
            endTime = dataAvailabilityEndTime.toProto()
          }
        }
      }
      if (!struct.isNull("EventGroupDetails")) {
        details =
          struct.getProtoMessage("EventGroupDetails", EventGroupDetails.getDefaultInstance())
      }
      state = struct.getProtoEnum("State", EventGroup.State::forNumber)
    }
  }

  companion object {
    private val BASE_SQL =
      """
      SELECT
        EventGroups.EventGroupId,
        EventGroups.ExternalEventGroupId,
        EventGroups.MeasurementConsumerId,
        EventGroups.CreateRequestId,
        EventGroups.DataProviderId,
        EventGroups.ProvidedEventGroupId,
        EventGroups.CreateTime,
        EventGroups.UpdateTime,
        EventGroups.DataAvailabilityStartTime,
        EventGroups.DataAvailabilityEndTime,
        EventGroups.EventGroupDetails,
        MeasurementConsumers.ExternalMeasurementConsumerId,
        DataProviders.ExternalDataProviderId,
        EventGroups.State,
        ARRAY(
          SELECT MediaType
          FROM EventGroupMediaTypes
          WHERE
            EventGroupMediaTypes.DataProviderId = EventGroups.DataProviderId
            AND EventGroupMediaTypes.EventGroupId = EventGroups.EventGroupId
        ) AS MediaTypes,
      FROM
        EventGroups
        JOIN DataProviders USING (DataProviderId)
        JOIN MeasurementConsumers USING (MeasurementConsumerId)
      """
        .trimIndent()

    private object Params {
      const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
      const val EXTERNAL_MEASUREMENT_CONSUMER_ID = "externalMeasurementConsumerId"
      const val EXTERNAL_EVENT_GROUP_ID = "externalEventGroupId"
      const val EXTERNAL_EVENT_GROUP_IDS = "externalEventGroupIds"
      const val DATA_PROVIDER_ID = "dataProviderId"
      const val CREATE_REQUEST_ID = "createRequestId"
      const val CREATE_REQUEST_IDS = "createRequestIds"
    }
  }
}
