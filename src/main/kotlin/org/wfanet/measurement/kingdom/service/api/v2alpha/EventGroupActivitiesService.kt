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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.Empty
import com.google.type.Date
import io.grpc.Status
import io.grpc.StatusException
import java.time.LocalDate
import java.time.format.DateTimeParseException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.BatchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.BatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.BatchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.api.v2alpha.DeleteEventGroupActivityRequest
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase as EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupActivity
import org.wfanet.measurement.api.v2alpha.EventGroupActivityKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.api.v2alpha.eventGroupActivity
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub as InternalEventGroupActivitiesCoroutineStub
import org.wfanet.measurement.internal.kingdom.EventGroupActivity as InternalEventGroupActivity
import org.wfanet.measurement.internal.kingdom.UpdateEventGroupActivityRequest as InternalUpdateEventGroupActivityRequest
import org.wfanet.measurement.internal.kingdom.batchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupActivitiesRequest as internalBatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.deleteEventGroupActivityRequest as internalDeleteEventGroupActivityRequest
import org.wfanet.measurement.internal.kingdom.eventGroupActivity as internalEventGroupActivity
import org.wfanet.measurement.internal.kingdom.updateEventGroupActivityRequest as internalUpdateEventGroupActivityRequest

class EventGroupActivitiesService(
  private val internalEventGroupActivitiesStub: InternalEventGroupActivitiesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : EventGroupActivitiesCoroutineImplBase(coroutineContext) {

  private enum class Permission {
    DELETE,
    UPDATE;

    fun deniedStatus(name: String): Status =
      Status.PERMISSION_DENIED.withDescription(
        "Permission $this denied on resource $name (or it might not exist)"
      )
  }

  override suspend fun batchUpdateEventGroupActivities(
    request: BatchUpdateEventGroupActivitiesRequest
  ): BatchUpdateEventGroupActivitiesResponse {
    val parentKey: EventGroupKey =
      grpcRequireNotNull(EventGroupKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    if (authenticatedPrincipal.resourceKey != parentKey.parentKey) {
      throw Permission.UPDATE.deniedStatus(request.parent).asRuntimeException()
    }

    val internalRequests: List<InternalUpdateEventGroupActivityRequest> =
      request.requestsList.map { child ->
        grpcRequireNotNull(child.eventGroupActivity) {
          "Child request event group activity is unspecified"
        }

        val eventGroupActivityKey =
          grpcRequireNotNull(EventGroupActivityKey.fromName(child.eventGroupActivity.name)) {
            "Child request event group activity name is either unspecified or invalid"
          }

        if (eventGroupActivityKey.parentKey != parentKey) {
          throw Status.INVALID_ARGUMENT.withDescription(
              "the EventGroup component of parent and child's EventGroupActivity do not match"
            )
            .asRuntimeException()
        }

        internalUpdateEventGroupActivityRequest {
          eventGroupActivity =
            child.eventGroupActivity.toInternal(
              eventGroupActivityKey.dataProviderId,
              eventGroupActivityKey.eventGroupId,
            )
          allowMissing = child.allowMissing
        }
      }

    val internalBatchRequest = internalBatchUpdateEventGroupActivitiesRequest {
      externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
      externalEventGroupId = apiIdToExternalId(parentKey.eventGroupId)
      requests += internalRequests
    }

    return try {
      batchUpdateEventGroupActivitiesResponse {
        eventGroupActivities +=
          internalEventGroupActivitiesStub
            .batchUpdateEventGroupActivities(internalBatchRequest)
            .eventGroupActivitiesList
            .map { it.toEventGroupActivity() }
      }
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun deleteEventGroupActivity(request: DeleteEventGroupActivityRequest): Empty {
    val eventGroupActivityKey: EventGroupActivityKey =
      grpcRequireNotNull(EventGroupActivityKey.fromName(request.name)) {
        "Name is either unspecified or invalid"
      }

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    val dataProviderKey = eventGroupActivityKey.parentKey.parentKey
    if (authenticatedPrincipal.resourceKey != dataProviderKey) {
      throw Permission.DELETE.deniedStatus(request.name).asRuntimeException()
    }

    val date =
      try {
        LocalDate.parse(eventGroupActivityKey.eventGroupActivityId).toProtoDate()
      } catch (_: DateTimeParseException) {
        throw Status.INVALID_ARGUMENT.withDescription(
            "the Event Group Activity component of the name is not a string of a date in ISO-8601 format"
          )
          .asRuntimeException()
      }

    try {
      internalEventGroupActivitiesStub.deleteEventGroupActivity(
        internalDeleteEventGroupActivityRequest {
          externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
          externalEventGroupId = apiIdToExternalId(eventGroupActivityKey.eventGroupId)
          externalEventGroupActivityId = date
        }
      )
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }

    return Empty.getDefaultInstance()
  }

  override suspend fun batchDeleteEventGroupActivities(
    request: BatchDeleteEventGroupActivitiesRequest
  ): Empty {
    val parentKey: EventGroupKey =
      grpcRequireNotNull(EventGroupKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    val dataProviderKey = parentKey.parentKey
    if (authenticatedPrincipal.resourceKey != dataProviderKey) {
      throw Permission.DELETE.deniedStatus(request.parent).asRuntimeException()
    }

    val dates: List<Date> =
      request.namesList.map { name ->
        val eventGroupActivityKey: EventGroupActivityKey =
          grpcRequireNotNull(EventGroupActivityKey.fromName(name)) {
            "Name is either unspecified or invalid"
          }

        if (eventGroupActivityKey.parentKey != parentKey) {
          throw Status.INVALID_ARGUMENT.withDescription(
              "the EventGroup component of parent and child do not match"
            )
            .asRuntimeException()
        }

        try {
          LocalDate.parse(eventGroupActivityKey.eventGroupActivityId).toProtoDate()
        } catch (_: DateTimeParseException) {
          throw Status.INVALID_ARGUMENT.withDescription(
              "the Event Group Activity component of the name is not a string of a date in ISO-8601 format"
            )
            .asRuntimeException()
        }
      }

    try {
      internalEventGroupActivitiesStub.batchDeleteEventGroupActivities(
        batchDeleteEventGroupActivitiesRequest {
          externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
          externalEventGroupId = apiIdToExternalId(parentKey.eventGroupId)
          externalEventGroupActivityIds += dates
        }
      )
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }

    return Empty.getDefaultInstance()
  }
}

/** Converts a public [EventGroupActivity] to an internal [InternalEventGroupActivity]. */
private fun EventGroupActivity.toInternal(
  dataProviderId: String,
  eventGroupId: String,
): InternalEventGroupActivity {
  val source = this
  return internalEventGroupActivity {
    externalDataProviderId = apiIdToExternalId(dataProviderId)
    externalEventGroupId = apiIdToExternalId(eventGroupId)
    date = source.date
  }
}

/** Converts an internal [InternalEventGroupActivity] to a public [EventGroupActivity]. */
private fun InternalEventGroupActivity.toEventGroupActivity(): EventGroupActivity {
  val source = this

  return eventGroupActivity {
    name =
      EventGroupActivityKey(
          externalIdToApiId(externalDataProviderId),
          externalIdToApiId(externalEventGroupId),
          source.date.toLocalDate().toString(),
        )
        .toName()
    date = source.date
  }
}
