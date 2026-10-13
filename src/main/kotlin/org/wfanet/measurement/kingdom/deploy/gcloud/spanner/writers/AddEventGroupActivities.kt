// Copyright 2026 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.AddEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.AddEventGroupActivitiesResponse
import org.wfanet.measurement.internal.kingdom.EventGroupActivity
import org.wfanet.measurement.internal.kingdom.addEventGroupActivitiesResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.eventGroupActivity
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupActivityReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader

/** Adds activity for every requested EventGroup/date pair that does not already exist. */
class AddEventGroupActivities(private val request: AddEventGroupActivitiesRequest) :
  SpannerWriter<List<EventGroupActivity>, AddEventGroupActivitiesResponse>() {
  override suspend fun TransactionScope.runTransaction(): List<EventGroupActivity> {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val dataProviderId: InternalId =
      DataProviderReader.readDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)

    val externalEventGroupIds: List<ExternalId> =
      request.externalEventGroupIdsList.map(::ExternalId)
    val eventGroups: Map<ExternalId, EventGroupReader.Result> =
      EventGroupReader()
        .batchReadByExternalIds(
          transactionContext,
          externalDataProviderId,
          externalEventGroupIds,
        )
    for (externalEventGroupId in externalEventGroupIds) {
      if (externalEventGroupId !in eventGroups) {
        throw EventGroupNotFoundException(externalDataProviderId, externalEventGroupId)
      }
    }

    val eventGroupIds = externalEventGroupIds.map { eventGroups.getValue(it).internalEventGroupId }
    val existingPairs: Map<Pair<InternalId, Date>, EventGroupActivityReader.Result> =
      EventGroupActivityReader()
        .readByEventGroupIdsAndDates(
          transactionContext,
          dataProviderId,
          eventGroupIds,
          request.datesList,
        )

    return buildList {
      for (externalEventGroupId in externalEventGroupIds) {
        val eventGroupId = eventGroups.getValue(externalEventGroupId).internalEventGroupId
        for (date in request.datesList) {
          val existingActivity = existingPairs[eventGroupId to date]
          if (existingActivity != null) {
            add(existingActivity.eventGroupActivity)
            continue
          }

          transactionContext.bufferInsertMutation("EventGroupActivities") {
            set("DataProviderId" to dataProviderId)
            set("EventGroupId" to eventGroupId)
            set("EventGroupActivityId" to idGenerator.generateInternalId())
            set("ActivityDate" to date.toCloudDate())
            set("CreateTime" to Value.COMMIT_TIMESTAMP)
          }
          add(
            eventGroupActivity {
              this.externalDataProviderId = request.externalDataProviderId
              this.externalEventGroupId = externalEventGroupId.value
              this.date = date
            }
          )
        }
      }
    }
  }

  override fun ResultScope<List<EventGroupActivity>>.buildResult():
    AddEventGroupActivitiesResponse {
    checkNotNull(transactionResult)
    return addEventGroupActivitiesResponse {
      eventGroupActivities +=
        transactionResult.map { activity ->
          if (activity.hasCreateTime()) {
            activity
          } else {
            activity.copy { createTime = commitTimestamp.toProto() }
          }
        }
    }
  }
}
