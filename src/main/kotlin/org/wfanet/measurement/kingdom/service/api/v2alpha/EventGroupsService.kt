/*
 * Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.any
import com.google.protobuf.kotlin.unpack
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.BatchCreateEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.BatchCreateEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.BatchUpdateEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.BatchUpdateEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DeleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt.eventTemplate
import org.wfanet.measurement.api.v2alpha.EventGroupMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsPageToken
import org.wfanet.measurement.api.v2alpha.ListEventGroupsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerEventGroupKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.batchCreateEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsPageToken
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.orderByOrNull
import org.wfanet.measurement.api.v2alpha.packedValue
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.BatchCreateEventGroupsRequest as InternalBatchCreateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.CreateEventGroupRequest as InternalCreateEventGroupRequest
import org.wfanet.measurement.internal.kingdom.EventGroup as InternalEventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupDetails
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub as InternalEventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest as InternalGetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.MediaType as InternalMediaType
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt
import org.wfanet.measurement.internal.kingdom.UpdateEventGroupRequest as InternalUpdateEventGroupRequest
import org.wfanet.measurement.internal.kingdom.batchCreateEventGroupsRequest as internalBatchCreateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupsRequest as internalBatchUpdateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest as internalCreateEventGroupRequest
import org.wfanet.measurement.internal.kingdom.deleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup as internalEventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupDetails
import org.wfanet.measurement.internal.kingdom.eventGroupKey
import org.wfanet.measurement.internal.kingdom.eventTemplate as internalEventTemplate
import org.wfanet.measurement.internal.kingdom.getEventGroupRequest as internalGetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupRequest as internalUpdateEventGroupRequest

class EventGroupsService(
  private val internalEventGroupsStub: InternalEventGroupsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : EventGroupsCoroutineImplBase(coroutineContext) {

  private enum class Permission {
    GET,
    CREATE,
    UPDATE;

    fun deniedStatus(name: String): Status =
      Status.PERMISSION_DENIED.withDescription(
        "Permission $this denied on resource $name (or it might not exist)"
      )
  }

  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    fun permissionDeniedStatus() = Permission.GET.deniedStatus(request.name)

    val key: ChildResourceKey =
      grpcRequireNotNull(
        EventGroupKey.fromName(request.name)
          ?: MeasurementConsumerEventGroupKey.fromName(request.name)
      ) {
        "Resource name is either unspecified or invalid"
      }
    val principal = principalFromCurrentContext

    val internalRequest: InternalGetEventGroupRequest =
      when (key) {
        is EventGroupKey -> {
          val denied =
            when (principal) {
              is DataProviderPrincipal -> principal.resourceKey != key.parentKey
              is MeasurementConsumerPrincipal -> false
              else -> true
            }
          if (denied) throw permissionDeniedStatus().asRuntimeException()
          internalGetEventGroupRequest {
            externalDataProviderId = ApiId(key.dataProviderId).externalId.value
            externalEventGroupId = ApiId(key.eventGroupId).externalId.value
          }
        }
        is MeasurementConsumerEventGroupKey -> {
          if (key.parentKey != principal.resourceKey) {
            throw permissionDeniedStatus().asRuntimeException()
          }
          internalGetEventGroupRequest {
            externalMeasurementConsumerId = ApiId(key.measurementConsumerId).externalId.value
            externalEventGroupId = ApiId(key.eventGroupId).externalId.value
          }
        }
        else -> error("Unexpected resource key $key")
      }
    val internalResponse: InternalEventGroup =
      try {
        internalEventGroupsStub.getEventGroup(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND ->
            if (key.parentKey == principal.resourceKey) {
              Status.NOT_FOUND
            } else {
              permissionDeniedStatus()
            }
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    when (principal) {
      is DataProviderPrincipal -> {
        if (
          ExternalId(internalResponse.externalDataProviderId) !=
            ApiId(principal.resourceKey.dataProviderId).externalId
        ) {
          throw permissionDeniedStatus().asRuntimeException()
        }
      }
      is MeasurementConsumerPrincipal -> {
        if (
          ExternalId(internalResponse.externalMeasurementConsumerId) !=
            ApiId(principal.resourceKey.measurementConsumerId).externalId
        ) {
          throw permissionDeniedStatus().asRuntimeException()
        }
      }
      else -> throw permissionDeniedStatus().asRuntimeException()
    }

    return internalResponse.toEventGroup()
  }

  override suspend fun createEventGroup(request: CreateEventGroupRequest): EventGroup {
    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    if (authenticatedPrincipal.resourceKey != parentKey) {
      throw Permission.CREATE.deniedStatus("${request.parent}/eventGroups").asRuntimeException()
    }

    validateRequestEventGroup(request.eventGroup)

    val internalRequest: InternalCreateEventGroupRequest = internalCreateEventGroupRequest {
      eventGroup = request.eventGroup.toInternal(parentKey.dataProviderId)
      requestId = request.requestId
    }
    return try {
      internalEventGroupsStub.createEventGroup(internalRequest).toEventGroup()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun batchCreateEventGroups(
    request: BatchCreateEventGroupsRequest
  ): BatchCreateEventGroupsResponse {
    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    if (authenticatedPrincipal.resourceKey != parentKey) {
      throw Permission.CREATE.deniedStatus("${request.parent}/eventGroups").asRuntimeException()
    }

    val requestIdSet = mutableSetOf<String>()
    request.requestsList.forEach { subRequest ->
      if (subRequest.parent.isNotEmpty() && subRequest.parent != request.parent) {
        throw Status.INVALID_ARGUMENT.withDescription("Parent and child DataProvider is different")
          .asRuntimeException()
      }

      val requestId = subRequest.requestId
      if (requestId.isNotEmpty()) {
        if (!requestIdSet.add(requestId)) {
          throw Status.INVALID_ARGUMENT.withDescription(
              "request Id $requestId is duplicate in the batch of requests"
            )
            .asRuntimeException()
        }
      }

      if (!subRequest.hasEventGroup()) {
        throw Status.INVALID_ARGUMENT.withDescription("Child request event group is unspecified")
          .asRuntimeException()
      }

      validateRequestEventGroup(subRequest.eventGroup)
    }

    val internalRequest: InternalBatchCreateEventGroupsRequest =
      internalBatchCreateEventGroupsRequest {
        externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
        requests +=
          request.requestsList.map {
            internalCreateEventGroupRequest {
              eventGroup = it.eventGroup.toInternal(parentKey.dataProviderId)
              requestId = it.requestId
            }
          }
      }

    return try {
      batchCreateEventGroupsResponse {
        eventGroups +=
          internalEventGroupsStub.batchCreateEventGroups(internalRequest).eventGroupsList.map {
            it.toEventGroup()
          }
      }
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun updateEventGroup(request: UpdateEventGroupRequest): EventGroup {
    val eventGroupKey =
      grpcRequireNotNull(EventGroupKey.fromName(request.eventGroup.name)) {
        "EventGroup name is either unspecified or invalid"
      }

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    if (authenticatedPrincipal.resourceKey != eventGroupKey.parentKey) {
      throw Permission.UPDATE.deniedStatus(request.eventGroup.name).asRuntimeException()
    }

    validateRequestEventGroup(request.eventGroup)

    val updateRequest = internalUpdateEventGroupRequest {
      eventGroup =
        request.eventGroup.toInternal(eventGroupKey.dataProviderId, eventGroupKey.eventGroupId)
    }
    return try {
      internalEventGroupsStub.updateEventGroup(updateRequest).toEventGroup()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun batchUpdateEventGroups(
    request: BatchUpdateEventGroupsRequest
  ): BatchUpdateEventGroupsResponse {
    val parentKey: DataProviderKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val internalRequests = mutableListOf<InternalUpdateEventGroupRequest>()
    request.requestsList.forEach { childRequest ->
      grpcRequireNotNull(childRequest.eventGroup) { "Child request event group is unspecified" }

      val eventGroupKey =
        grpcRequireNotNull(EventGroupKey.fromName(childRequest.eventGroup.name)) {
          "Child request event group name is either unspecified invalid"
        }

      if (eventGroupKey.parentKey != parentKey) {
        throw Status.INVALID_ARGUMENT.withDescription(
            "parent DataProvider and child DataProviderKey do not match"
          )
          .asRuntimeException()
      }

      val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
      if (authenticatedPrincipal.resourceKey != eventGroupKey.parentKey) {
        throw Permission.UPDATE.deniedStatus(childRequest.eventGroup.name).asRuntimeException()
      }

      validateRequestEventGroup(childRequest.eventGroup)

      internalRequests += internalUpdateEventGroupRequest {
        eventGroup =
          childRequest.eventGroup.toInternal(
            eventGroupKey.dataProviderId,
            eventGroupKey.eventGroupId,
          )
      }
    }

    val internalBatchRequest = internalBatchUpdateEventGroupsRequest {
      externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
      requests += internalRequests
    }

    return try {
      batchUpdateEventGroupsResponse {
        eventGroups +=
          internalEventGroupsStub.batchUpdateEventGroup(internalBatchRequest).eventGroupsList.map {
            it.toEventGroup()
          }
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

  /**
   * Validates an `event_group` field from a request.
   *
   * @throws io.grpc.StatusRuntimeException
   */
  private fun validateRequestEventGroup(requestEventGroup: EventGroup) {
    if (requestEventGroup.hasEncryptedMetadata()) {
      grpcRequire(
        requestEventGroup.hasMeasurementConsumerPublicKey() ||
          // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop reading this
          // field.
          requestEventGroup.hasSignedMeasurementConsumerPublicKey()
      ) {
        "event_group.measurement_consumer_public_key must be specified if " +
          "event_group.encrypted_metadata is specified"
      }
    }
    if (requestEventGroup.hasMeasurementConsumerPublicKey()) {
      try {
        requestEventGroup.measurementConsumerPublicKey.unpack<EncryptionPublicKey>()
      } catch (e: InvalidProtocolBufferException) {
        throw Status.INVALID_ARGUMENT.withCause(e)
          .withDescription(
            "event_group.measurement_consumer_public_key.message is not a valid EncryptionPublicKey"
          )
          .asRuntimeException()
      }
    }

    if (requestEventGroup.hasDataAvailabilityInterval()) {
      grpcRequire(requestEventGroup.dataAvailabilityInterval.startTime.seconds > 0) {
        "start_time required in data_availability_interval"
      }

      if (requestEventGroup.dataAvailabilityInterval.hasEndTime()) {
        grpcRequire(
          Timestamps.compare(
            requestEventGroup.dataAvailabilityInterval.startTime,
            requestEventGroup.dataAvailabilityInterval.endTime,
          ) < 0
        ) {
          "data_availability_interval start_time must be before end_time"
        }
      }
    }
  }

  override suspend fun deleteEventGroup(request: DeleteEventGroupRequest): EventGroup {
    fun permissionDeniedStatus() =
      Status.PERMISSION_DENIED.withDescription(
        "Permission denied on resource ${request.name} (or it might not exist)"
      )

    val eventGroupKey =
      grpcRequireNotNull(EventGroupKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    if (principalFromCurrentContext.resourceKey != eventGroupKey.parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    val deleteRequest = deleteEventGroupRequest {
      externalDataProviderId = apiIdToExternalId(eventGroupKey.dataProviderId)
      externalEventGroupId = apiIdToExternalId(eventGroupKey.eventGroupId)
    }

    return try {
      internalEventGroupsStub.deleteEventGroup(deleteRequest).toEventGroup()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    fun permissionDeniedStatus() =
      Status.PERMISSION_DENIED.withDescription(
        "Permission ListEventGroups denied on resource ${request.parent} (or it might not exist)"
      )

    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }

    val parentKey: ResourceKey =
      DataProviderKey.fromName(request.parent)
        ?: MeasurementConsumerKey.fromName(request.parent)
        ?: throw Status.INVALID_ARGUMENT.withDescription("parent unspecified or invalid")
          .asRuntimeException()
    if (parentKey != principalFromCurrentContext.resourceKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    val pageToken: ListEventGroupsPageToken? =
      if (request.pageToken.isEmpty()) null
      else ListEventGroupsPageToken.parseFrom(request.pageToken.base64UrlDecode())
    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
    val internalRequest =
      buildInternalStreamEventGroupsRequest(
        request.filter,
        request.showDeleted,
        request.orderByOrNull,
        parentKey,
        pageSize,
        pageToken,
      )
    val internalEventGroups: List<InternalEventGroup> =
      try {
        internalEventGroupsStub.streamEventGroups(internalRequest).toList()
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    if (internalEventGroups.isEmpty()) {
      return ListEventGroupsResponse.getDefaultInstance()
    }

    return listEventGroupsResponse {
      eventGroups +=
        internalEventGroups
          .subList(0, min(internalEventGroups.size, pageSize))
          .map(InternalEventGroup::toEventGroup)
      if (internalEventGroups.size > pageSize) {
        nextPageToken =
          buildNextPageToken(internalRequest.filter, internalEventGroups)
            .toByteString()
            .base64UrlEncode()
      }
    }
  }

  private fun buildNextPageToken(
    internalFilter: StreamEventGroupsRequest.Filter,
    results: List<InternalEventGroup>,
  ): ListEventGroupsPageToken {
    val lastInternalEventGroup: InternalEventGroup = results[results.lastIndex - 1]
    return listEventGroupsPageToken {
      if (internalFilter.externalDataProviderId != 0L) {
        externalDataProviderId = internalFilter.externalDataProviderId
      }
      if (internalFilter.externalMeasurementConsumerId != 0L) {
        externalMeasurementConsumerId = internalFilter.externalMeasurementConsumerId
      }
      externalDataProviderIdIn += internalFilter.externalDataProviderIdInList
      externalMeasurementConsumerIdIn += internalFilter.externalMeasurementConsumerIdInList
      mediaTypesIntersect += internalFilter.mediaTypesIntersectList.map { it.toMediaType() }
      if (internalFilter.hasDataAvailabilityStartTimeOnOrAfter()) {
        dataAvailabilityStartTimeOnOrAfter = internalFilter.dataAvailabilityStartTimeOnOrAfter
      }
      if (internalFilter.hasDataAvailabilityEndTimeOnOrBefore()) {
        dataAvailabilityEndTimeOnOrBefore = internalFilter.dataAvailabilityEndTimeOnOrBefore
      }
      if (internalFilter.hasDataAvailabilityStartTimeOnOrBefore()) {
        dataAvailabilityStartTimeOnOrBefore = internalFilter.dataAvailabilityStartTimeOnOrBefore
      }
      if (internalFilter.hasDataAvailabilityEndTimeOnOrAfter()) {
        dataAvailabilityEndTimeOnOrAfter = internalFilter.dataAvailabilityEndTimeOnOrAfter
      }
      metadataSearchQuery = internalFilter.metadataSearchQuery
      lastEventGroup = previousPageEnd {
        externalDataProviderId = lastInternalEventGroup.externalDataProviderId
        externalEventGroupId = lastInternalEventGroup.externalEventGroupId
        if (lastInternalEventGroup.dataAvailabilityInterval.hasStartTime()) {
          dataAvailabilityStartTime = lastInternalEventGroup.dataAvailabilityInterval.startTime
        }
      }
    }
  }

  /**
   * Builds a [StreamEventGroupsRequest] for [listEventGroups].
   *
   * @throws io.grpc.StatusRuntimeException if the [ListEventGroupsRequest] is found to be invalid
   */
  private fun buildInternalStreamEventGroupsRequest(
    filter: ListEventGroupsRequest.Filter,
    showDeleted: Boolean,
    orderBy: ListEventGroupsRequest.OrderBy?,
    parentKey: ResourceKey,
    pageSize: Int,
    pageToken: ListEventGroupsPageToken?,
  ): StreamEventGroupsRequest {
    return streamEventGroupsRequest {
      allowStaleReads = true
      this.filter =
        StreamEventGroupsRequestKt.filter {
          if (parentKey is DataProviderKey) {
            externalDataProviderId = ApiId(parentKey.dataProviderId).externalId.value
          }
          if (parentKey is MeasurementConsumerKey) {
            externalMeasurementConsumerId = ApiId(parentKey.measurementConsumerId).externalId.value
          }
          if (filter.measurementConsumerInList.isNotEmpty()) {
            externalMeasurementConsumerIdIn +=
              filter.measurementConsumerInList.map {
                val measurementConsumerKey =
                  grpcRequireNotNull(MeasurementConsumerKey.fromName(it)) {
                    "Invalid resource name in filter.measurement_consumers"
                  }
                ApiId(measurementConsumerKey.measurementConsumerId).externalId.value
              }
          }
          if (filter.dataProviderInList.isNotEmpty()) {
            externalDataProviderIdIn +=
              filter.dataProviderInList.map {
                val dataProviderKey =
                  grpcRequireNotNull(DataProviderKey.fromName(it)) {
                    "Invalid resource name in filter.data_providers"
                  }
                ApiId(dataProviderKey.dataProviderId).externalId.value
              }
          }
          if (filter.mediaTypesIntersectList.isNotEmpty()) {
            mediaTypesIntersect += filter.mediaTypesIntersectList.map { it.toInternal() }
          }
          if (filter.hasDataAvailabilityStartTimeOnOrAfter()) {
            dataAvailabilityStartTimeOnOrAfter = filter.dataAvailabilityStartTimeOnOrAfter
          }
          if (filter.hasDataAvailabilityEndTimeOnOrBefore()) {
            dataAvailabilityEndTimeOnOrBefore = filter.dataAvailabilityEndTimeOnOrBefore
          }
          if (filter.hasDataAvailabilityStartTimeOnOrBefore()) {
            dataAvailabilityStartTimeOnOrBefore = filter.dataAvailabilityStartTimeOnOrBefore
          }
          if (filter.hasDataAvailabilityEndTimeOnOrAfter()) {
            dataAvailabilityEndTimeOnOrAfter = filter.dataAvailabilityEndTimeOnOrAfter
          }
          metadataSearchQuery = filter.metadataSearchQuery
          this.showDeleted = showDeleted
          if (pageToken != null) {
            if (
              pageToken.externalDataProviderId != externalDataProviderId ||
                pageToken.externalMeasurementConsumerId != externalMeasurementConsumerId ||
                pageToken.showDeleted != showDeleted ||
                pageToken.externalDataProviderIdInList != externalDataProviderIdIn ||
                pageToken.externalMeasurementConsumerIdInList != externalMeasurementConsumerIdIn ||
                pageToken.mediaTypesIntersectList != filter.mediaTypesIntersectList ||
                pageToken.dataAvailabilityStartTimeOnOrAfter !=
                  dataAvailabilityStartTimeOnOrAfter ||
                pageToken.dataAvailabilityEndTimeOnOrBefore != dataAvailabilityEndTimeOnOrBefore ||
                pageToken.dataAvailabilityStartTimeOnOrBefore !=
                  dataAvailabilityStartTimeOnOrBefore ||
                pageToken.dataAvailabilityEndTimeOnOrAfter != dataAvailabilityEndTimeOnOrAfter ||
                pageToken.metadataSearchQuery != metadataSearchQuery
            ) {
              throw Status.INVALID_ARGUMENT.withDescription(
                  "Arguments other than page_size must remain the same for subsequent page requests"
                )
                .asRuntimeException()
            }
            after =
              StreamEventGroupsRequestKt.FilterKt.after {
                eventGroupKey = eventGroupKey {
                  externalDataProviderId = pageToken.lastEventGroup.externalDataProviderId
                  externalEventGroupId = pageToken.lastEventGroup.externalEventGroupId
                }
                if (pageToken.lastEventGroup.hasDataAvailabilityStartTime()) {
                  dataAvailabilityStartTime = pageToken.lastEventGroup.dataAvailabilityStartTime
                }
              }
            // TODO(@SanjayVas): Stop writing the deprecated field once the replacement has been
            // available for at least one release.
            eventGroupKeyAfter = after.eventGroupKey
          }
        }
      if (orderBy != null) {
        this.orderBy =
          StreamEventGroupsRequestKt.orderBy {
            field = orderBy.field.toInternal()
            descending = orderBy.descending
          }
      }
      limit = pageSize + 1
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 500
  }
}

/** Converts an internal [InternalEventGroup] to a public [EventGroup]. */
private fun InternalEventGroup.toEventGroup(): EventGroup {
  val eventGroupKey =
    EventGroupKey(
      externalIdToApiId(externalDataProviderId),
      externalIdToApiId(externalEventGroupId),
    )
  val measurementConsumerKey =
    MeasurementConsumerKey(externalIdToApiId(externalMeasurementConsumerId))
  val source = this

  return eventGroup {
    name = eventGroupKey.toName()
    measurementConsumer = measurementConsumerKey.toName()
    eventGroupReferenceId = source.providedEventGroupId
    this.mediaTypes += source.mediaTypesList.map { it.toMediaType() }
    if (source.hasDataAvailabilityInterval()) {
      dataAvailabilityInterval = source.dataAvailabilityInterval
    }
    if (source.hasDetails()) {
      val apiVersion = Version.fromString(source.details.apiVersion)
      if (!source.details.measurementConsumerPublicKey.isEmpty) {
        measurementConsumerPublicKey = any {
          value = source.details.measurementConsumerPublicKey
          typeUrl =
            when (apiVersion) {
              Version.V2_ALPHA -> ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
            }
        }
        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
        // field.
        signedMeasurementConsumerPublicKey = signedMessage {
          message = this@eventGroup.measurementConsumerPublicKey
          data = message.value
          signature = source.details.measurementConsumerPublicKeySignature
          signatureAlgorithmOid = source.details.measurementConsumerPublicKeySignatureAlgorithmOid
        }
      }
      vidModelLines += source.details.vidModelLinesList
      eventTemplates.addAll(
        source.details.eventTemplatesList.map { event ->
          eventTemplate { type = event.fullyQualifiedType }
        }
      )
      if (source.details.hasMetadata()) {
        eventGroupMetadata = source.details.metadata.toEventGroupMetadata()
      }
      if (!source.details.encryptedMetadata.isEmpty) {
        encryptedMetadata = encryptedMessage {
          ciphertext = source.details.encryptedMetadata
          typeUrl =
            when (apiVersion) {
              Version.V2_ALPHA -> ProtoReflection.getTypeUrl(EventGroup.Metadata.getDescriptor())
            }
        }
        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
        // field.
        serializedEncryptedMetadata = source.details.encryptedMetadata
      }
    }
    state = source.state.toV2Alpha()
  }
}

private fun InternalMediaType.toMediaType(): MediaType {
  return when (this) {
    InternalMediaType.VIDEO -> MediaType.VIDEO
    InternalMediaType.DISPLAY -> MediaType.DISPLAY
    InternalMediaType.OTHER -> MediaType.OTHER
    InternalMediaType.MEDIA_TYPE_UNSPECIFIED -> MediaType.MEDIA_TYPE_UNSPECIFIED
    InternalMediaType.UNRECOGNIZED -> error("MediaType unrecognized")
  }
}

private fun EventGroupDetails.EventGroupMetadata.toEventGroupMetadata(): EventGroupMetadata {
  val source = this
  return eventGroupMetadata {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
    when (source.metadataCase) {
      EventGroupDetails.EventGroupMetadata.MetadataCase.AD_METADATA -> {
        adMetadata =
          EventGroupMetadataKt.adMetadata {
            campaignMetadata =
              EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                brandName = source.adMetadata.campaignMetadata.brandName
                campaignName = source.adMetadata.campaignMetadata.campaignName
              }
          }
      }
      EventGroupDetails.EventGroupMetadata.MetadataCase.METADATA_NOT_SET ->
        error("metadata not set")
    }
  }
}

/** Converts a public [EventGroup] to an internal [InternalEventGroup]. */
private fun EventGroup.toInternal(
  dataProviderId: String,
  eventGroupId: String = "",
): InternalEventGroup {
  val measurementConsumerKey =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(measurementConsumer)) {
      "Measurement consumer is either unspecified or invalid"
    }

  val source = this
  return internalEventGroup {
    externalDataProviderId = apiIdToExternalId(dataProviderId)
    if (eventGroupId.isNotEmpty()) {
      externalEventGroupId = apiIdToExternalId(eventGroupId)
    }
    externalMeasurementConsumerId = apiIdToExternalId(measurementConsumerKey.measurementConsumerId)

    providedEventGroupId = source.eventGroupReferenceId
    mediaTypes += source.mediaTypesList.map { it.toInternal() }
    if (source.hasDataAvailabilityInterval()) {
      dataAvailabilityInterval = source.dataAvailabilityInterval
    }
    details = eventGroupDetails {
      apiVersion = Version.V2_ALPHA.string
      // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop reading this
      // field.
      if (source.hasSignedMeasurementConsumerPublicKey()) {
        measurementConsumerPublicKeySignature = source.signedMeasurementConsumerPublicKey.signature
        measurementConsumerPublicKeySignatureAlgorithmOid =
          source.signedMeasurementConsumerPublicKey.signatureAlgorithmOid
      }
      if (source.hasMeasurementConsumerPublicKey()) {
        measurementConsumerPublicKey = source.measurementConsumerPublicKey.value
      } else if (source.hasSignedMeasurementConsumerPublicKey()) {
        measurementConsumerPublicKey = source.signedMeasurementConsumerPublicKey.packedValue
      }

      vidModelLines += source.vidModelLinesList
      eventTemplates.addAll(
        source.eventTemplatesList.map { event ->
          internalEventTemplate { fullyQualifiedType = event.type }
        }
      )
      if (source.hasEventGroupMetadata()) {
        metadata = source.eventGroupMetadata.toInternal()
      }
      if (source.hasEncryptedMetadata()) {
        encryptedMetadata = source.encryptedMetadata.ciphertext
      } else if (!source.serializedEncryptedMetadata.isEmpty) {
        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop reading this
        // field.
        encryptedMetadata = source.serializedEncryptedMetadata
      }
    }
  }
}

private fun MediaType.toInternal(): InternalMediaType {
  return when (this) {
    MediaType.VIDEO -> InternalMediaType.VIDEO
    MediaType.DISPLAY -> InternalMediaType.DISPLAY
    MediaType.OTHER -> InternalMediaType.OTHER
    MediaType.MEDIA_TYPE_UNSPECIFIED -> InternalMediaType.MEDIA_TYPE_UNSPECIFIED
    MediaType.UNRECOGNIZED -> error("Media type unrecognized")
  }
}

private fun EventGroupMetadata.toInternal(): EventGroupDetails.EventGroupMetadata {
  val source = this
  return EventGroupDetailsKt.eventGroupMetadata {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
    when (source.selectorCase) {
      EventGroupMetadata.SelectorCase.AD_METADATA -> {
        adMetadata =
          EventGroupDetailsKt.EventGroupMetadataKt.adMetadata {
            campaignMetadata =
              EventGroupDetailsKt.EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                brandName = source.adMetadata.campaignMetadata.brandName
                campaignName = source.adMetadata.campaignMetadata.campaignName
              }
          }
      }
      EventGroupMetadata.SelectorCase.SELECTOR_NOT_SET -> error("metadata not set")
    }
  }
}

private fun ListEventGroupsRequest.OrderBy.Field.toInternal():
  StreamEventGroupsRequest.OrderBy.Field {
  return when (this) {
    ListEventGroupsRequest.OrderBy.Field.FIELD_UNSPECIFIED ->
      StreamEventGroupsRequest.OrderBy.Field.FIELD_NOT_SPECIFIED
    ListEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME ->
      StreamEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
    ListEventGroupsRequest.OrderBy.Field.UNRECOGNIZED -> error("field unrecognized")
  }
}
