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

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.EventGroup
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
class UpdateEventGroup(private val eventGroup: EventGroup) :
  SpannerWriter<EventGroup, EventGroup>() {
  override suspend fun TransactionScope.runTransaction(): EventGroup {
    val externalDataProviderId = ExternalId(eventGroup.externalDataProviderId)
    val externalEventGroupId = ExternalId(eventGroup.externalEventGroupId)
    val internalEventGroupResult =
      EventGroupReader()
        .readByDataProvider(transactionContext, externalDataProviderId, externalEventGroupId)
        ?: throw EventGroupNotFoundException(externalDataProviderId, externalEventGroupId)
    if (internalEventGroupResult.eventGroup.state == EventGroup.State.DELETED) {
      throw EventGroupStateIllegalException(
        externalEventGroupId,
        externalEventGroupId,
        internalEventGroupResult.eventGroup.state,
      )
    }
    if (
      internalEventGroupResult.eventGroup.externalMeasurementConsumerId !=
        eventGroup.externalMeasurementConsumerId
    ) {
      throw EventGroupInvalidArgsException(
        ExternalId(internalEventGroupResult.eventGroup.externalMeasurementConsumerId),
        ExternalId(eventGroup.externalMeasurementConsumerId),
      )
    }
    val providedEventGroupId =
      if (eventGroup.providedEventGroupId.isNotBlank()) {
        eventGroup.providedEventGroupId
      } else null

    transactionContext.bufferUpdateMutation("EventGroups") {
      set("DataProviderId" to internalEventGroupResult.internalDataProviderId.value)
      set("EventGroupId" to internalEventGroupResult.internalEventGroupId.value)
      set("ProvidedEventGroupId" to providedEventGroupId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("EventGroupDetails").to(eventGroup.details)
    }

    return eventGroup
  }

  override fun ResultScope<EventGroup>.buildResult(): EventGroup {
    return eventGroup.toBuilder().apply { updateTime = commitTimestamp.toProto() }.build()
  }
}
