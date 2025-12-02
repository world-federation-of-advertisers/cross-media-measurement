// Copyright 2021 The Cross-Media Measurement Authors
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

import com.google.cloud.spanner.TimestampBound
import io.grpc.Status
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.BatchCreateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.BatchCreateEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.BatchUpdateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.BatchUpdateEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.CreateEventGroupRequest
import org.wfanet.measurement.internal.kingdom.DeleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.UpdateEventGroupRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateIsInvalidException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundByMeasurementConsumerException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamEventGroups
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchCreateEventGroups
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchUpdateEventGroups
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateEventGroup
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.DeleteEventGroup
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.UpdateEventGroup

class SpannerEventGroupsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  maxReadStaleness: Duration,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : EventGroupsCoroutineImplBase(coroutineContext) {
  private val staleReadTimestampBound =
    TimestampBound.ofMaxStaleness(maxReadStaleness.toMillis(), TimeUnit.MILLISECONDS)

  override suspend fun createEventGroup(request: CreateEventGroupRequest): EventGroup {
    try {
      return CreateEventGroup(request).execute(client, idGenerator)
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "MeasurementConsumer not found.",
      )
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
    } catch (e: CertificateIsInvalidException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "MeasurementConsumer's Certificate is invalid.",
      )
    } catch (e: MeasurementConsumerCertificateNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "MeasurementConsumer's Certificate not found.",
      )
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun batchCreateEventGroups(
    request: BatchCreateEventGroupsRequest
  ): BatchCreateEventGroupsResponse {
    val externalDataProviderId = request.externalDataProviderId
    if (externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val requestIdSet = mutableSetOf<String>()
    request.requestsList.forEachIndexed { index, subRequest ->
      val childExternalDataProviderId = subRequest.eventGroup.externalDataProviderId
      if (
        childExternalDataProviderId != 0L && childExternalDataProviderId != externalDataProviderId
      ) {
        throw InvalidFieldValueException("requests.$index.event_group.external_data_provider_id") {
            fieldPath ->
            "Value of $fieldPath differs from that of the parent request"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      val requestId = subRequest.requestId
      if (requestId.isNotEmpty()) {
        if (!requestIdSet.add(requestId)) {
          throw InvalidFieldValueException("requests.$index.request_id") { fieldPath ->
              "Value of $fieldPath is duplicate in the batch of requests"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

      if (!subRequest.hasEventGroup()) {
        throw RequiredFieldNotSetException("requests.$index.event_group")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    try {
      return BatchCreateEventGroups(request).execute(client, idGenerator)
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: CertificateIsInvalidException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: MeasurementConsumerCertificateNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun updateEventGroup(request: UpdateEventGroupRequest): EventGroup {
    grpcRequire(request.eventGroup.externalDataProviderId > 0L) {
      "ExternalDataProviderId unspecified"
    }
    grpcRequire(request.eventGroup.externalEventGroupId > 0L) { "ExternalEventGroupId unspecified" }
    try {
      return UpdateEventGroup(request.eventGroup).execute(client, idGenerator)
    } catch (e: EventGroupInvalidArgsException) {
      throw e.asStatusRuntimeException(
        Status.Code.INVALID_ARGUMENT,
        "EventGroup modification param is invalid.",
      )
    } catch (e: CertificateIsInvalidException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "MeasurementConsumer's Certificate is invalid.",
      )
    } catch (e: MeasurementConsumerCertificateNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "MeasurementConsumer's Certificate not found.",
      )
    } catch (e: EventGroupNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "EventGroup not found.")
    } catch (e: EventGroupStateIllegalException) {
      when (e.state) {
        EventGroup.State.DELETED -> {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "EventGroup state is DELETED.")
        }
        EventGroup.State.ACTIVE,
        EventGroup.State.STATE_UNSPECIFIED,
        EventGroup.State.UNRECOGNIZED -> {
          throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
        }
      }
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun batchUpdateEventGroup(
    request: BatchUpdateEventGroupsRequest
  ): BatchUpdateEventGroupsResponse {
    if (request.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val externalDataProviderId = request.externalDataProviderId

    request.requestsList.forEachIndexed { index, subRequest ->
      val subRequestExternalDataProviderId = subRequest.eventGroup.externalDataProviderId

      if (subRequestExternalDataProviderId == 0L) {
        throw RequiredFieldNotSetException("requests.$index.event_group.external_data_provider_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (subRequestExternalDataProviderId != externalDataProviderId) {
        throw InvalidFieldValueException("requests.$index.event_group.external_data_provider_id") {
            "Subrequest's externalDataProviderId $subRequestExternalDataProviderId different from parent's externalDataProviderId $externalDataProviderId"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (subRequest.eventGroup.externalEventGroupId == 0L) {
        throw RequiredFieldNotSetException("requests.$index.event_group.external_event_group_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    try {
      return BatchUpdateEventGroups(request).execute(client, idGenerator)
    } catch (e: EventGroupInvalidArgsException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: CertificateIsInvalidException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: MeasurementConsumerCertificateNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: EventGroupNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EventGroupStateIllegalException) {
      when (e.state) {
        EventGroup.State.DELETED -> {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "EventGroup state is DELETED.")
        }
        EventGroup.State.ACTIVE,
        EventGroup.State.STATE_UNSPECIFIED,
        EventGroup.State.UNRECOGNIZED -> {
          throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
        }
      }
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    grpcRequire(request.externalEventGroupId != 0L) { "external_event_group_id not specified" }
    val externalEventGroupId = ExternalId(request.externalEventGroupId)
    val reader = EventGroupReader()

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    return when (request.externalParentIdCase) {
      GetEventGroupRequest.ExternalParentIdCase.EXTERNAL_DATA_PROVIDER_ID -> {
        val externalDataProviderId = ExternalId(request.externalDataProviderId)
        reader.readByDataProvider(client.singleUse(), externalDataProviderId, externalEventGroupId)
          ?: throw EventGroupNotFoundException(externalDataProviderId, externalEventGroupId)
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
      GetEventGroupRequest.ExternalParentIdCase.EXTERNAL_MEASUREMENT_CONSUMER_ID -> {
        val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
        reader.readByMeasurementConsumer(
          client.singleUse(),
          externalMeasurementConsumerId,
          externalEventGroupId,
        )
          ?: throw EventGroupNotFoundByMeasurementConsumerException(
              externalMeasurementConsumerId,
              externalEventGroupId,
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
      GetEventGroupRequest.ExternalParentIdCase.EXTERNALPARENTID_NOT_SET ->
        throw Status.INVALID_ARGUMENT.withDescription("external_parent_id not specified")
          .asRuntimeException()
    }.eventGroup
  }

  override suspend fun deleteEventGroup(request: DeleteEventGroupRequest): EventGroup {
    grpcRequire(request.externalDataProviderId != 0L) { "external_data_provider_id unspecified" }
    grpcRequire(request.externalEventGroupId > 0L) { "external_event_group_id unspecified" }

    try {
      return DeleteEventGroup(request).execute(client, idGenerator)
    } catch (e: EventGroupNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EventGroupNotFoundByMeasurementConsumerException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EventGroupStateIllegalException) {
      throw when (e.state) {
        EventGroup.State.DELETED -> {
          e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
        EventGroup.State.ACTIVE,
        EventGroup.State.STATE_UNSPECIFIED,
        EventGroup.State.UNRECOGNIZED -> {
          e.asStatusRuntimeException(Status.Code.INTERNAL)
        }
      }
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL)
    }
  }

  override fun streamEventGroups(request: StreamEventGroupsRequest): Flow<EventGroup> {
    val timestampBound =
      if (request.allowStaleReads) {
        staleReadTimestampBound
      } else {
        TimestampBound.strong()
      }
    return StreamEventGroups(request.filter, request.orderBy, request.limit)
      .execute(client.singleUse(timestampBound))
      .map { it.eventGroup }
  }
}
