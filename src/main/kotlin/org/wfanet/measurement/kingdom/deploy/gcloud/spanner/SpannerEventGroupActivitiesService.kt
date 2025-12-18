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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import com.google.protobuf.Empty
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
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
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

    val externalEventGroupId = request.externalEventGroupId
    request.requestsList.forEachIndexed { index, child ->
      val childExternalEventGroupId = child.eventGroupActivity.externalEventGroupId

      if (childExternalEventGroupId != 0L && childExternalEventGroupId != externalEventGroupId) {
        throw InvalidFieldValueException(
            "requests.$index.event_group_activity.external_event_group_id"
          ) {
            "Child's externalEventGroupId $childExternalEventGroupId different from parent's externalEventGroupId $externalEventGroupId"
          }
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
    return super.deleteEventGroupActivity(request)
  }

  override suspend fun batchDeleteEventGroupActivities(
    request: BatchDeleteEventGroupActivitiesRequest
  ): Empty {
    return super.batchDeleteEventGroupActivities(request)
  }
}
