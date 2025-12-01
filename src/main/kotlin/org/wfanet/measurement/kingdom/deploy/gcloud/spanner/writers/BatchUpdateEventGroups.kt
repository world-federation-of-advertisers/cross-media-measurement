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

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.BatchUpdateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.BatchUpdateEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader

class BatchUpdateEventGroups(private val request: BatchUpdateEventGroupsRequest) :
  SpannerWriter<BatchUpdateEventGroupsResponse, BatchUpdateEventGroupsResponse>() {
  override suspend fun TransactionScope.runTransaction(): BatchUpdateEventGroupsResponse {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val eventGroups: Map<ExternalId, EventGroupReader.Result> =
      EventGroupReader()
        .batchReadByExternalIds(
          transactionContext,
          externalDataProviderId,
          request.requestsList.map { ExternalId(it.eventGroup.externalEventGroupId) },
        )

    for (subRequest in request.requestsList) {
      val result: EventGroupReader.Result =
        eventGroups[ExternalId(subRequest.eventGroup.externalEventGroupId)]
          ?: throw EventGroupNotFoundException(
            externalDataProviderId,
            ExternalId(subRequest.eventGroup.externalEventGroupId),
          )

      updateEventGroup(subRequest.eventGroup, result)
    }

    return batchUpdateEventGroupsResponse {
      this.eventGroups += request.requestsList.map { it.eventGroup }
    }
  }

  override fun ResultScope<BatchUpdateEventGroupsResponse>.buildResult():
    BatchUpdateEventGroupsResponse {
    checkNotNull(transactionResult)

    return batchUpdateEventGroupsResponse {
      eventGroups +=
        transactionResult.eventGroupsList.map { it.copy { updateTime = commitTimestamp.toProto() } }
    }
  }
}
