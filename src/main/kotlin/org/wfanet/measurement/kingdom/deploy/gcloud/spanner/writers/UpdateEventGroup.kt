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
import com.google.cloud.spanner.Value
import com.google.type.endTimeOrNull
import com.google.type.startTimeOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toSet
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupDetails
import org.wfanet.measurement.internal.kingdom.MediaType
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader

/**
 * Update [EventGroup] in the database.
 *
 * Throws one of the following [KingdomInternalException] types on [execute].
 * * [EventGroupNotFoundException] EventGroup not found
 * * [EventGroupStateIllegalException] EventGroup state is DELETED
 * * [EventGroupInvalidArgsException] MeasurementConsumer ids mismatch
 */
class UpdateEventGroup(private val request: EventGroup) : SpannerWriter<EventGroup, EventGroup>() {
  override suspend fun TransactionScope.runTransaction(): EventGroup {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val externalEventGroupId = ExternalId(request.externalEventGroupId)

    val result: EventGroupReader.Result =
      EventGroupReader()
        .readByDataProvider(transactionContext, externalDataProviderId, externalEventGroupId)
        ?: throw EventGroupNotFoundException(externalDataProviderId, externalEventGroupId)

    updateEventGroup(request, result)

    return request
  }

  override fun ResultScope<EventGroup>.buildResult(): EventGroup {
    return request.toBuilder().apply { updateTime = commitTimestamp.toProto() }.build()
  }
}

/** Buffers an update mutation for the EventGroups table. */
internal suspend fun SpannerWriter.TransactionScope.updateEventGroup(
  request: EventGroup,
  result: EventGroupReader.Result,
) {
  val externalEventGroupId = ExternalId(request.externalEventGroupId)
  val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)

  if (result.eventGroup.state == EventGroup.State.DELETED) {
    throw EventGroupStateIllegalException(
      externalEventGroupId,
      externalEventGroupId,
      result.eventGroup.state,
    )
  }

  if (result.eventGroup.externalMeasurementConsumerId != externalMeasurementConsumerId.value) {
    throw EventGroupInvalidArgsException(
      ExternalId(result.eventGroup.externalMeasurementConsumerId),
      externalMeasurementConsumerId,
    )
  }
  val providedEventGroupId: String? = request.providedEventGroupId.ifBlank { null }

  transactionContext.bufferUpdateMutation(Table.EVENT_GROUPS) {
    set("DataProviderId").to(result.internalDataProviderId)
    set("EventGroupId").to(result.internalEventGroupId)
    set("ProvidedEventGroupId" to providedEventGroupId)
    set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    set("DataAvailabilityStartTime")
      .to(request.dataAvailabilityInterval.startTimeOrNull?.toGcloudTimestamp())
    set("DataAvailabilityEndTime")
      .to(request.dataAvailabilityInterval.endTimeOrNull?.toGcloudTimestamp())

    val detailsValue =
      if (request.hasDetails()) {
        Value.protoMessage(request.details)
      } else {
        Value.protoMessage(null, EventGroupDetails.getDescriptor())
      }
    set("EventGroupDetails").to(detailsValue)
  }

  transactionContext.syncMediaTypes(
    result.internalDataProviderId,
    result.internalEventGroupId,
    request.mediaTypesList.toSet(),
  )
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

private object Table {
  const val EVENT_GROUPS = "EventGroups"
  const val EVENT_GROUP_MEDIA_TYPES = "EventGroupMediaTypes"
}
