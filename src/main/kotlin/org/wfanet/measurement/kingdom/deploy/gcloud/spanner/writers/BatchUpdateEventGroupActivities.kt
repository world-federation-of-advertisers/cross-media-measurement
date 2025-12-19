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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value
import com.google.type.Date
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.BatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.BatchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.internal.kingdom.EventGroupActivity
import org.wfanet.measurement.internal.kingdom.UpdateEventGroupActivityRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupActivityNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupActivityReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader

class BatchUpdateEventGroupActivities(private val request: BatchUpdateEventGroupActivitiesRequest) :
  SpannerWriter<List<EventGroupActivity>, BatchUpdateEventGroupActivitiesResponse>() {
  override suspend fun TransactionScope.runTransaction(): List<EventGroupActivity> {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val dataProviderId: InternalId =
      DataProviderReader.readDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)

    val externalEventGroupId = ExternalId(request.externalEventGroupId)
    val eventGroupId: InternalId =
      EventGroupReader.readEventGroupId(transactionContext, dataProviderId, externalEventGroupId)
        ?: throw EventGroupNotFoundException(externalDataProviderId, externalEventGroupId)

    val activities: Map<Date, EventGroupActivityReader.Result> =
      EventGroupActivityReader()
        .batchReadByExternalIds(
          transactionContext,
          dataProviderId,
          externalEventGroupId,
          request.requestsList.map { it.eventGroupActivity.date },
        )

    val upsertedActivities = mutableListOf<EventGroupActivity>()
    for (child in request.requestsList) {
      val result: EventGroupActivityReader.Result? = activities[child.eventGroupActivity.date]
      if (result != null) {
        upsertedActivities.add(updateEventGroupActivity(child.eventGroupActivity, result))
      } else if (child.allowMissing) {
        upsertedActivities.add(
          createEventGroupActivity(idGenerator, dataProviderId, eventGroupId, child)
        )
      } else {
        throw EventGroupActivityNotFoundException(
          externalDataProviderId,
          externalEventGroupId,
          child.eventGroupActivity.date,
        )
      }
    }

    return upsertedActivities
  }

  override fun ResultScope<List<EventGroupActivity>>.buildResult():
    BatchUpdateEventGroupActivitiesResponse {
    checkNotNull(transactionResult)
    return batchUpdateEventGroupActivitiesResponse {
      transactionResult.forEach {
        if (it.hasCreateTime()) {
          eventGroupActivities += it
        } else {
          eventGroupActivities += it.copy { createTime = commitTimestamp.toProto() }
        }
      }
    }
  }
}

/** Buffers an update mutation for the EventGroupActivities table. */
private fun SpannerWriter.TransactionScope.updateEventGroupActivity(
  request: EventGroupActivity,
  result: EventGroupActivityReader.Result,
): EventGroupActivity {
  transactionContext.bufferUpdateMutation("EventGroupActivities") {
    set("DataProviderId" to result.internalDataProviderId)
    set("EventGroupId" to result.internalEventGroupId)
    set("EventGroupActivityId" to result.internalEventGroupActivityId)
    set("ActivityDate" to request.date.toCloudDate())
  }
  return result.eventGroupActivity
}

/** Buffers an insert mutation for the EventGroupActivities table. */
private fun SpannerWriter.TransactionScope.createEventGroupActivity(
  idGenerator: IdGenerator,
  dataProviderId: InternalId,
  eventGroupId: InternalId,
  request: UpdateEventGroupActivityRequest,
): EventGroupActivity {
  val internalEventGroupActivityId: InternalId = idGenerator.generateInternalId()

  transactionContext.bufferInsertMutation("EventGroupActivities") {
    set("DataProviderId" to dataProviderId)
    set("EventGroupId" to eventGroupId)
    set("EventGroupActivityId" to internalEventGroupActivityId)
    set("ActivityDate" to request.eventGroupActivity.date.toCloudDate())
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
  }

  return request.eventGroupActivity.copy { clearCreateTime() }
}
