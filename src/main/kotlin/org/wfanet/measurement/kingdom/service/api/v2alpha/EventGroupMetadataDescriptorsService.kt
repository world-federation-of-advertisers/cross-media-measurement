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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.Descriptors.DescriptorValidationException
import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.BatchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.BatchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.CreateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupMetadataDescriptorsPageToken
import org.wfanet.measurement.api.v2alpha.ListEventGroupMetadataDescriptorsPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsPageToken
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor as InternalEventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupMetadataDescriptorsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptor as internalEventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptorDetails
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptorKey
import org.wfanet.measurement.internal.kingdom.getEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupMetadataDescriptorRequest

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000
private const val WILDCARD = ResourceKey.WILDCARD_ID
private val API_VERSION = Version.V2_ALPHA

class EventGroupMetadataDescriptorsService(
  private val internalEventGroupMetadataDescriptorsStub: EventGroupMetadataDescriptorsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : EventGroupMetadataDescriptorsCoroutineImplBase(coroutineContext) {

  override suspend fun getEventGroupMetadataDescriptor(
    request: GetEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    val key =
      grpcRequireNotNull(EventGroupMetadataDescriptorKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (principal.resourceKey.dataProviderId != key.dataProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get EventGroupMetadataDescriptors belonging to other DataProviders"
          }
        }
      }
      is MeasurementConsumerPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to get EventGroupMetadataDescriptors"
        }
      }
    }

    val getRequest = getEventGroupMetadataDescriptorRequest {
      externalDataProviderId = apiIdToExternalId(key.dataProviderId)
      externalEventGroupMetadataDescriptorId = apiIdToExternalId(key.eventGroupMetadataDescriptorId)
    }

    val internalEventGroupMetadataDescriptor =
      try {
        internalEventGroupMetadataDescriptorsStub.getEventGroupMetadataDescriptor(getRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalEventGroupMetadataDescriptor.toEventGroupMetadataDescriptor()
  }

  override suspend fun createEventGroupMetadataDescriptor(
    request: CreateEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (principal.resourceKey.toName() != request.parent) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create EventGroupMetadataDescriptors for another DataProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to create EventGroupMetadataDescriptors"
        }
      }
    }

    // Check if the FileDescriptorSet is valid by trying to build a list of Descriptors from it.
    try {
      ProtoReflection.buildDescriptors(listOf(request.eventGroupMetadataDescriptor.descriptorSet))
    } catch (e: DescriptorValidationException) {
      throw Status.INVALID_ARGUMENT.withCause(e)
        .withDescription("descriptor_set is invalid")
        .asRuntimeException()
    }

    val createRequest =
      request.eventGroupMetadataDescriptor.toInternal(
        parentKey.dataProviderId,
        idempotencyKey = request.requestId,
      )
    val internalEventGroupMetadataDescriptor =
      try {
        internalEventGroupMetadataDescriptorsStub.createEventGroupMetadataDescriptor(createRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalEventGroupMetadataDescriptor.toEventGroupMetadataDescriptor()
  }

  override suspend fun updateEventGroupMetadataDescriptor(
    request: UpdateEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    val eventGroupMetadataDescriptorKey =
      grpcRequireNotNull(
        EventGroupMetadataDescriptorKey.fromName(request.eventGroupMetadataDescriptor.name)
      ) {
        "EventGroupMetadataDescriptor name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (
          principal.resourceKey.dataProviderId != eventGroupMetadataDescriptorKey.dataProviderId
        ) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot update EventGroupMetadataDescriptors for another DataProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to update EventGroupMetadataDescriptors"
        }
      }
    }

    val updateRequest = updateEventGroupMetadataDescriptorRequest {
      eventGroupMetadataDescriptor =
        request.eventGroupMetadataDescriptor.toInternal(
          eventGroupMetadataDescriptorKey.dataProviderId,
          eventGroupMetadataDescriptorKey.eventGroupMetadataDescriptorId,
        )
    }
    val internalEventGroupMetadataDescriptor =
      try {
        internalEventGroupMetadataDescriptorsStub.updateEventGroupMetadataDescriptor(updateRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalEventGroupMetadataDescriptor.toEventGroupMetadataDescriptor()
  }

  override suspend fun batchGetEventGroupMetadataDescriptors(
    request: BatchGetEventGroupMetadataDescriptorsRequest
  ): BatchGetEventGroupMetadataDescriptorsResponse {
    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (principal.resourceKey.dataProviderId != parentKey.dataProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get EventGroupMetadataDescriptors belonging to other DataProviders"
          }
        }
      }
      is MeasurementConsumerPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to get EventGroupMetadataDescriptors"
        }
      }
    }

    val descriptorIds =
      request.namesList.map { name ->
        val descriptorKey = EventGroupMetadataDescriptorKey.fromName(name)
        if (descriptorKey != null) {
          apiIdToExternalId(descriptorKey.eventGroupMetadataDescriptorId)
        } else {
          failGrpc(Status.NOT_FOUND) { "Resource name is either unspecified or invalid" }
        }
      }

    val streamRequest = streamEventGroupMetadataDescriptorsRequest {
      filter = filter {
        if (parentKey.dataProviderId != ResourceKey.WILDCARD_ID) {
          externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
        }
        externalEventGroupMetadataDescriptorIds += descriptorIds
      }
    }

    val orderByDescriptorId = descriptorIds.withIndex().associate { it.value to it.index }
    val results: List<InternalEventGroupMetadataDescriptor> =
      internalEventGroupMetadataDescriptorsStub
        .streamEventGroupMetadataDescriptors(streamRequest)
        .toList()
        .sortedBy { descriptor ->
          if (orderByDescriptorId.containsKey(descriptor.externalEventGroupMetadataDescriptorId)) {
            orderByDescriptorId[descriptor.externalEventGroupMetadataDescriptorId]
          } else {
            failGrpc(Status.NOT_FOUND) { "Descriptor was not found" }
          }
        }

    if (results.isEmpty()) {
      return BatchGetEventGroupMetadataDescriptorsResponse.getDefaultInstance()
    }

    return batchGetEventGroupMetadataDescriptorsResponse {
      eventGroupMetadataDescriptors +=
        results.map(InternalEventGroupMetadataDescriptor::toEventGroupMetadataDescriptor)
    }
  }

  override suspend fun listEventGroupMetadataDescriptors(
    request: ListEventGroupMetadataDescriptorsRequest
  ): ListEventGroupMetadataDescriptorsResponse {
    val listEventGroupMetadataDescriptorsPageToken =
      request.toListEventGroupMetadataDescriptorsPageToken()

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (
          apiIdToExternalId(principal.resourceKey.dataProviderId) !=
            listEventGroupMetadataDescriptorsPageToken.externalDataProviderId
        ) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list EventGroup Metadata Descriptors belonging to other DataProviders"
          }
        }
      }
      is MeasurementConsumerPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to list EventGroup Metadata Descriptors"
        }
      }
    }

    val results: List<InternalEventGroupMetadataDescriptor> =
      try {
        internalEventGroupMetadataDescriptorsStub
          .streamEventGroupMetadataDescriptors(
            listEventGroupMetadataDescriptorsPageToken
              .toStreamEventGroupMetadataDescriptorsRequest()
          )
          .toList()
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    if (results.isEmpty()) {
      return ListEventGroupMetadataDescriptorsResponse.getDefaultInstance()
    }

    return listEventGroupMetadataDescriptorsResponse {
      eventGroupMetadataDescriptors +=
        results
          .subList(0, min(results.size, listEventGroupMetadataDescriptorsPageToken.pageSize))
          .map(InternalEventGroupMetadataDescriptor::toEventGroupMetadataDescriptor)
      if (results.size > listEventGroupMetadataDescriptorsPageToken.pageSize) {
        val pageToken =
          listEventGroupMetadataDescriptorsPageToken.copy {
            lastEventGroupMetadataDescriptor =
              ListEventGroupMetadataDescriptorsPageTokenKt.previousPageEnd {
                externalDataProviderId = results[results.lastIndex - 1].externalDataProviderId
                externalEventGroupMetadataDescriptorId =
                  results[results.lastIndex - 1].externalEventGroupMetadataDescriptorId
              }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
    }
  }
}

/**
 * Converts an internal [InternalEventGroupMetadataDescriptor] to a public
 * [EventGroupMetadataDescriptor].
 */
private fun InternalEventGroupMetadataDescriptor.toEventGroupMetadataDescriptor():
  EventGroupMetadataDescriptor {
  return eventGroupMetadataDescriptor {
    name =
      EventGroupMetadataDescriptorKey(
          externalIdToApiId(externalDataProviderId),
          externalIdToApiId(externalEventGroupMetadataDescriptorId),
        )
        .toName()
    descriptorSet = details.descriptorSet
  }
}

/**
 * Converts a public [EventGroupMetadataDescriptor] to an internal
 * [InternalEventGroupMetadataDescriptor].
 */
private fun EventGroupMetadataDescriptor.toInternal(
  dataProviderId: String,
  eventGroupMetadataDescriptorId: String = "",
  idempotencyKey: String = "",
): InternalEventGroupMetadataDescriptor {
  return internalEventGroupMetadataDescriptor {
    externalDataProviderId = apiIdToExternalId(dataProviderId)
    if (eventGroupMetadataDescriptorId.isNotEmpty()) {
      externalEventGroupMetadataDescriptorId = apiIdToExternalId(eventGroupMetadataDescriptorId)
    }
    if (idempotencyKey.isNotEmpty()) {
      this.idempotencyKey = idempotencyKey
    }

    details = eventGroupMetadataDescriptorDetails {
      apiVersion = API_VERSION.string
      descriptorSet = this@toInternal.descriptorSet
    }
  }
}

/**
 * Converts a public [ListEventGroupMetadataDescriptorsRequest] to an internal
 * [ListEventGroupMetadataDescriptorsPageToken].
 */
private fun ListEventGroupMetadataDescriptorsRequest.toListEventGroupMetadataDescriptorsPageToken():
  ListEventGroupMetadataDescriptorsPageToken {
  val source = this

  grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

  val parentKey: DataProviderKey =
    grpcRequireNotNull(DataProviderKey.fromName(source.parent)) {
      "Parent is either unspecified or invalid"
    }

  var externalDataProviderId = 0L
  if (parentKey.dataProviderId != WILDCARD) {
    externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
  }

  return if (source.pageToken.isNotBlank()) {
    ListEventGroupMetadataDescriptorsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
      grpcRequire(this.externalDataProviderId == externalDataProviderId) {
        "Arguments must be kept the same when using a page token"
      }

      if (
        source.pageSize != 0 && source.pageSize >= MIN_PAGE_SIZE && source.pageSize <= MAX_PAGE_SIZE
      ) {
        pageSize = source.pageSize
      }
    }
  } else {
    listEventGroupMetadataDescriptorsPageToken {
      pageSize =
        when {
          source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }

      this.externalDataProviderId = externalDataProviderId
    }
  }
}

/**
 * Converts an internal [ListEventGroupMetadataDescriptorsPageToken] to an internal
 * [StreamEventGroupMetadataDescriptorsRequest].
 */
private fun ListEventGroupMetadataDescriptorsPageToken
  .toStreamEventGroupMetadataDescriptorsRequest(): StreamEventGroupMetadataDescriptorsRequest {
  val source = this
  return streamEventGroupMetadataDescriptorsRequest {
    // get 1 more than the actual page size for deciding whether to set page token
    limit = source.pageSize + 1
    filter = filter {
      externalDataProviderId = source.externalDataProviderId
      keyAfter = eventGroupMetadataDescriptorKey {
        externalDataProviderId = source.lastEventGroupMetadataDescriptor.externalDataProviderId
        externalEventGroupMetadataDescriptorId =
          source.lastEventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId
      }
    }
  }
}
