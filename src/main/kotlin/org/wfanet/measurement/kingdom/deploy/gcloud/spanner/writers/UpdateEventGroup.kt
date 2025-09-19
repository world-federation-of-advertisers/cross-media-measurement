// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.type.endTimeOrNull
import com.google.type.startTimeOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toSet
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.getExternalId
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupDetails
import org.wfanet.measurement.internal.kingdom.MediaType
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException

/**
 * Update [EventGroup] in the database.
 *
 * Throws one of the following [KingdomInternalException] types on [execute].
 * * [EventGroupNotFoundException] EventGroup not found
 * * [EventGroupStateIllegalException] EventGroup state is DELETED
 * * [EventGroupInvalidArgsException] MeasurementConsumer ids mismatch
 */
class UpdateEventGroup(private val eventGroup: EventGroup) :
  SpannerWriter<EventGroup, EventGroup>() {
  override suspend fun TransactionScope.runTransaction(): EventGroup {
    val externalDataProviderId = ExternalId(eventGroup.externalDataProviderId)
    val externalEventGroupId = ExternalId(eventGroup.externalEventGroupId)
    val externalMeasurementConsumerId = ExternalId(eventGroup.externalMeasurementConsumerId)
    val internalEventGroupResult =
      transactionContext.readEventGroup(externalDataProviderId, externalEventGroupId)
        ?: throw EventGroupNotFoundException(externalDataProviderId, externalEventGroupId)
    if (internalEventGroupResult.state == EventGroup.State.DELETED) {
      throw EventGroupStateIllegalException(
        externalEventGroupId,
        externalEventGroupId,
        internalEventGroupResult.state,
      )
    }
    if (internalEventGroupResult.externalMeasurementConsumerId != externalMeasurementConsumerId) {
      throw EventGroupInvalidArgsException(
        internalEventGroupResult.externalMeasurementConsumerId,
        externalMeasurementConsumerId,
      )
    }
    val providedEventGroupId: String? = eventGroup.providedEventGroupId.ifBlank { null }

    transactionContext.bufferUpdateMutation(Table.EVENT_GROUPS) {
      set("DataProviderId").to(internalEventGroupResult.dataProviderId)
      set("EventGroupId").to(internalEventGroupResult.eventGroupId)
      set("ProvidedEventGroupId" to providedEventGroupId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("DataAvailabilityStartTime")
        .to(eventGroup.dataAvailabilityInterval.startTimeOrNull?.toGcloudTimestamp())
      set("DataAvailabilityEndTime")
        .to(eventGroup.dataAvailabilityInterval.endTimeOrNull?.toGcloudTimestamp())

      val detailsValue =
        if (eventGroup.hasDetails()) {
          Value.protoMessage(eventGroup.details)
        } else {
          Value.protoMessage(null, EventGroupDetails.getDescriptor())
        }
      set("EventGroupDetails").to(detailsValue)
    }

    transactionContext.syncMediaTypes(
      internalEventGroupResult.dataProviderId,
      internalEventGroupResult.eventGroupId,
      eventGroup.mediaTypesList.toSet(),
    )

    return eventGroup
  }

  private suspend fun AsyncDatabaseClient.TransactionContext.syncMediaTypes(
    dataProviderId: InternalId,
    eventGroupId: InternalId,
    replacementMediaTypes: Set<MediaType>,
  ) {
    val existingMediaTypes: Set<MediaType> =
      read(
          Table.EVENT_GROUP_MEDIA_TYPES,
          KeySet.prefixRange(Key.of(dataProviderId.value, eventGroupId.value)),
          listOf("MediaType"),
        )
        .map { row -> row.getProtoEnum<MediaType>("MediaType", MediaType::forNumber) }
        .toSet()

    if (replacementMediaTypes == existingMediaTypes) {
      return // Optimization.
    }

    for (mediaType in replacementMediaTypes subtract existingMediaTypes) {
      bufferInsertMutation(Table.EVENT_GROUP_MEDIA_TYPES) {
        set("DataProviderId").to(dataProviderId)
        set("EventGroupId").to(eventGroupId)
        set("MediaType").to(mediaType)
      }
    }
    for (mediaType in existingMediaTypes subtract replacementMediaTypes) {
      buffer(
        Mutation.delete(
          Table.EVENT_GROUP_MEDIA_TYPES,
          Key.of(dataProviderId.value, eventGroupId.value, mediaType),
        )
      )
    }
  }

  override fun ResultScope<EventGroup>.buildResult(): EventGroup {
    return eventGroup.toBuilder().apply { updateTime = commitTimestamp.toProto() }.build()
  }

  private suspend fun AsyncDatabaseClient.ReadContext.readEventGroup(
    externalDataProviderId: ExternalId,
    externalEventGroupId: ExternalId,
  ): EventGroupResult? {
    val sql =
      """
      SELECT
        DataProviderId,
        EventGroupId,
        ExternalMeasurementConsumerId,
        State,
      FROM
        ${Table.EVENT_GROUPS}
        JOIN DataProviders USING (DataProviderId)
        JOIN MeasurementConsumers USING (MeasurementConsumerId)
      WHERE
        ExternalDataProviderId = @externalDataProviderId
        AND ExternalEventGroupId = @externalEventGroupId
      """
        .trimIndent()
    val query =
      statement(sql) {
        bind("externalDataProviderId").to(externalDataProviderId)
        bind("externalEventGroupId").to(externalEventGroupId)
      }

    val row: Struct =
      executeQuery(query, Options.tag("writer=$writerName,action=readEventGroup"))
        .singleOrNullIfEmpty() ?: return null
    return EventGroupResult(
      row.getInternalId("DataProviderId"),
      row.getInternalId("EventGroupId"),
      row.getExternalId("ExternalMeasurementConsumerId"),
      row.getProtoEnum("State", EventGroup.State::forNumber),
    )
  }

  private data class EventGroupResult(
    val dataProviderId: InternalId,
    val eventGroupId: InternalId,
    val externalMeasurementConsumerId: ExternalId,
    val state: EventGroup.State,
  )

  private object Table {
    const val EVENT_GROUPS = "EventGroups"
    const val EVENT_GROUP_MEDIA_TYPES = "EventGroupMediaTypes"
  }
}
