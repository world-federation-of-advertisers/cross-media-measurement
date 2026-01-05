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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import com.google.protobuf.Empty
import com.google.type.Date
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.BatchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.BatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.BatchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.internal.kingdom.DeleteEventGroupActivityRequest
import org.wfanet.measurement.internal.kingdom.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.batchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupActivityNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchDeleteEventGroupActivities
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchUpdateEventGroupActivities

class SpannerEventGroupActivitiesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : EventGroupActivitiesCoroutineImplBase(coroutineContext) {
  override suspend fun batchUpdateEventGroupActivities(
    request: BatchUpdateEventGroupActivitiesRequest
  ): BatchUpdateEventGroupActivitiesResponse {
    if (request.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.externalEventGroupId == 0L) {
      throw RequiredFieldNotSetException("external_event_group_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val externalDataProviderId = request.externalDataProviderId
    val externalEventGroupId = request.externalEventGroupId
    request.requestsList.forEachIndexed { index, child ->
      val childExternalDataProviderId = child.eventGroupActivity.externalDataProviderId
      if (
        childExternalDataProviderId != 0L && childExternalDataProviderId != externalDataProviderId
      ) {
        throw InvalidFieldValueException(
            "requests.$index.event_group_activity.external_data_provider_id"
          ) { fieldPath ->
            "Value of $fieldPath does not match parent request"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      val childExternalEventGroupId = child.eventGroupActivity.externalEventGroupId

      if (childExternalEventGroupId != 0L && childExternalEventGroupId != externalEventGroupId) {
        throw InvalidFieldValueException(
            "requests.$index.event_group_activity.external_event_group_id"
          ) { fieldPath ->
            "Value of $fieldPath does not match parent request"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (!child.hasEventGroupActivity()) {
        throw RequiredFieldNotSetException("requests.$index.event_group_activity")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (!child.eventGroupActivity.hasDate()) {
        throw RequiredFieldNotSetException("requests.$index.event_group_activity.date")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    try {
      return BatchUpdateEventGroupActivities(request).execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: EventGroupNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun deleteEventGroupActivity(request: DeleteEventGroupActivityRequest): Empty {
    if (request.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.externalEventGroupId == 0L) {
      throw RequiredFieldNotSetException("external_event_group_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (!request.hasExternalEventGroupActivityId()) {
      throw RequiredFieldNotSetException("external_event_group_activity_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val externalActivityId = request.externalEventGroupActivityId
    if (externalActivityId.year == 0) {
      throw RequiredFieldNotSetException("external_event_group_activity_id.year")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (externalActivityId.month == 0) {
      throw RequiredFieldNotSetException("external_event_group_activity_id.month")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (externalActivityId.day == 0) {
      throw RequiredFieldNotSetException("external_event_group_activity_id.day")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    try {
      BatchDeleteEventGroupActivities(
          batchDeleteEventGroupActivitiesRequest {
            externalDataProviderId = request.externalDataProviderId
            externalEventGroupId = request.externalEventGroupId
            externalEventGroupActivityIds += request.externalEventGroupActivityId
          }
        )
        .execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EventGroupNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EventGroupActivityNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }

    return Empty.getDefaultInstance()
  }

  override suspend fun batchDeleteEventGroupActivities(
    request: BatchDeleteEventGroupActivitiesRequest
  ): Empty {
    if (request.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.externalEventGroupId == 0L) {
      throw RequiredFieldNotSetException("external_event_group_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.externalEventGroupActivityIdsList.isEmpty()) {
      throw RequiredFieldNotSetException("external_event_group_activity_ids")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val idSet = mutableSetOf<Date>()
    request.externalEventGroupActivityIdsList.forEachIndexed { index, it ->
      if (it.year == 0) {
        throw RequiredFieldNotSetException("external_event_group_activity_ids.$index.year")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (it.month == 0) {
        throw RequiredFieldNotSetException("external_event_group_activity_ids.$index.month")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (it.day == 0) {
        throw RequiredFieldNotSetException("external_event_group_activity_ids.$index.day")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!idSet.add(it)) {
        throw InvalidFieldValueException("external_event_group_activity_ids") { fieldPath ->
            "$fieldPath contains duplicate values."
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    try {
      BatchDeleteEventGroupActivities(request).execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EventGroupNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EventGroupActivityNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }

    return Empty.getDefaultInstance()
  }
}
