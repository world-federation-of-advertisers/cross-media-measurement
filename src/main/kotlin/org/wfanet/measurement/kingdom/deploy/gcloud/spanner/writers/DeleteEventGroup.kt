/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.DeleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupDetails
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader

/**
 * Soft Delete [EventGroup] in the database.
 *
 * Throws one of the following [KingdomInternalException] types on [execute]:
 * * [EventGroupNotFoundException] EventGroup not found
 * * [EventGroupStateIllegalException] EventGroup state is DELETED
 */
class DeleteEventGroup(private val request: DeleteEventGroupRequest) :
  SpannerWriter<EventGroup, EventGroup>() {

  override suspend fun TransactionScope.runTransaction(): EventGroup {
    val externalEventGroupId = ExternalId(request.externalEventGroupId)
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val result: EventGroupReader.Result =
      EventGroupReader()
        .readByDataProvider(transactionContext, externalDataProviderId, externalEventGroupId)
        ?: throw EventGroupNotFoundException(externalDataProviderId, externalEventGroupId)
    if (result.eventGroup.state == EventGroup.State.DELETED) {
      throw EventGroupStateIllegalException(
        ExternalId(result.eventGroup.externalDataProviderId),
        externalEventGroupId,
        result.eventGroup.state,
      )
    }
    transactionContext.bufferUpdateMutation("EventGroups") {
      set("DataProviderId" to result.internalDataProviderId.value)
      set("EventGroupId" to result.internalEventGroupId.value)
      set("MeasurementConsumerCertificateId" to null as Long?)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("EventGroupDetails").to(null, EventGroupDetails.getDescriptor())
      set("State").toInt64(EventGroup.State.DELETED)
    }

    return result.eventGroup.copy {
      this.state = EventGroup.State.DELETED
      clearDetails()
    }
  }

  override fun ResultScope<EventGroup>.buildResult(): EventGroup {
    val eventGroup = checkNotNull(transactionResult)
    return eventGroup.copy { updateTime = commitTimestamp.toProto() }
  }
}
