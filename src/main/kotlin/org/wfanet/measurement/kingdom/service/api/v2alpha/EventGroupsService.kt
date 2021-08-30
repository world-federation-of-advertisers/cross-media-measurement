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

import com.google.protobuf.Timestamp
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.EventGroup as InternalEventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.eventGroup as internalEventGroup
import org.wfanet.measurement.internal.kingdom.getEventGroupRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupsRequest

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 100
private const val WILDCARD = "-"

class EventGroupsService(private val internalEventGroupsStub: EventGroupsCoroutineStub) :
  EventGroupsCoroutineImplBase() {

  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    val key =
      grpcRequireNotNull(EventGroupKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val getRequest = getEventGroupRequest {
      externalDataProviderId = apiIdToExternalId(key.dataProviderId)
      externalEventGroupId = apiIdToExternalId(key.eventGroupId)
    }

    return internalEventGroupsStub.getEventGroup(getRequest).toEventGroup()
  }

  override suspend fun createEventGroup(request: CreateEventGroupRequest): EventGroup {
    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val measurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.eventGroup.measurementConsumer)) {
        "Measurement consumer is either unspecified or invalid"
      }

    return internalEventGroupsStub
      .createEventGroup(request.eventGroup.toInternal(parentKey, measurementConsumerKey))
      .toEventGroup()
  }

  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }

    val parentKey: DataProviderKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }
    // TODO(world-federation-of-advertisers/cross-media-measurement#119): MC caller can only specify
    // their own id, but EDP caller can list EventGroups for multiple MCs
    grpcRequire(
      (request.filter.measurementConsumersCount > 0 && parentKey.dataProviderId == WILDCARD) ||
        parentKey.dataProviderId != WILDCARD
    ) { "Either parent data provider or measurement consumers filter must be provided" }

    val pageSize =
      when {
        request.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
        request.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
        else -> request.pageSize
      }

    val streamRequest = streamEventGroupsRequest {
      limit = pageSize
      filter =
        filter {
          if (request.pageToken.isNotBlank()) {
            createdAfter = Timestamp.parseFrom(request.pageToken.base64UrlDecode())
          }

          if (parentKey.dataProviderId != WILDCARD) {
            externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
          }

          externalMeasurementConsumerIds +=
            request.filter.measurementConsumersList.map { measurementConsumerName ->
              grpcRequireNotNull(MeasurementConsumerKey.fromName(measurementConsumerName)) {
                "Measurement consumer name in filter invalid"
              }
                .let { key -> apiIdToExternalId(key.measurementConsumerId) }
            }
        }
    }

    val results: List<InternalEventGroup> =
      internalEventGroupsStub.streamEventGroups(streamRequest).toList()

    if (results.isEmpty()) {
      return ListEventGroupsResponse.getDefaultInstance()
    }

    return listEventGroupsResponse {
      eventGroups += results.map(InternalEventGroup::toEventGroup)
      if (results.size == pageSize) {
        nextPageToken = results.last().createTime.toByteArray().base64UrlEncode()
      }
    }
  }
}

/** Converts an internal [InternalEventGroup] to a public [EventGroup]. */
private fun InternalEventGroup.toEventGroup(): EventGroup {
  return eventGroup {
    name =
      EventGroupKey(
          externalIdToApiId(externalDataProviderId),
          externalIdToApiId(externalEventGroupId)
        )
        .toName()
    measurementConsumer =
      MeasurementConsumerKey(externalIdToApiId(externalMeasurementConsumerId)).toName()
    eventGroupReferenceId = providedEventGroupId
  }
}

/** Converts a public [EventGroup] to an internal [InternalEventGroup]. */
private fun EventGroup.toInternal(
  parentKey: DataProviderKey,
  measurementConsumerKey: MeasurementConsumerKey
): InternalEventGroup {
  return internalEventGroup {
    externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
    externalMeasurementConsumerId = apiIdToExternalId(measurementConsumerKey.measurementConsumerId)
    providedEventGroupId = eventGroupReferenceId
  }
}
