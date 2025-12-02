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

import com.google.protobuf.Timestamp
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.kingdom.BatchCreateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.BatchCreateEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.batchCreateEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Creates EventGroups in a batch.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [DataProviderNotFoundException] when the DataProvider for the EventGroups is not found
 * @throws [MeasurementConsumerNotFoundException] when a MeasurementConsumer for an EventGroup is
 *   not found
 */
class BatchCreateEventGroups(private val request: BatchCreateEventGroupsRequest) :
  SpannerWriter<BatchCreateEventGroupsResponse, BatchCreateEventGroupsResponse>() {

  override suspend fun TransactionScope.runTransaction(): BatchCreateEventGroupsResponse {
    val externalToInternalMeasurementConsumerId: Map<ExternalId, InternalId> =
      MeasurementConsumerReader.readInternalIdsByExternalIds(
        transactionContext,
        request.requestsList.map { ExternalId(it.eventGroup.externalMeasurementConsumerId) },
      )

    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val dataProviderId: InternalId =
      DataProviderReader.readDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)

    val existingEventGroups: Map<String, EventGroupReader.Result> =
      EventGroupReader()
        .readByCreateRequestIds(
          transactionContext,
          dataProviderId,
          request.requestsList.map { it.requestId },
        )

    val creations: List<EventGroup> =
      request.requestsList
        .filter { !existingEventGroups.containsKey(it.requestId) }
        .map { subRequest ->
          val measurementConsumerId: InternalId =
            externalToInternalMeasurementConsumerId[
              ExternalId(subRequest.eventGroup.externalMeasurementConsumerId)]
              ?: throw MeasurementConsumerNotFoundException(
                ExternalId(subRequest.eventGroup.externalMeasurementConsumerId)
              )

          createEventGroup(idGenerator, dataProviderId, measurementConsumerId, subRequest)
        }

    val creationIterator = creations.iterator()
    return batchCreateEventGroupsResponse {
      eventGroups +=
        request.requestsList.map {
          existingEventGroups[it.requestId]?.eventGroup ?: creationIterator.next()
        }
    }
  }

  override fun ResultScope<BatchCreateEventGroupsResponse>.buildResult():
    BatchCreateEventGroupsResponse {
    checkNotNull(transactionResult)
    return batchCreateEventGroupsResponse {
      this.eventGroups +=
        transactionResult.eventGroupsList.map {
          if (it.hasCreateTime() && it.hasUpdateTime()) {
            it
          } else {
            val commitTime: Timestamp = commitTimestamp.toProto()
            it.copy {
              createTime = commitTime
              updateTime = commitTime
            }
          }
        }
    }
  }
}
