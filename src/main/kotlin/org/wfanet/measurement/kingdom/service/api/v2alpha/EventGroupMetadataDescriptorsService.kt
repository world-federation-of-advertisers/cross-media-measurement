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

import io.grpc.Status
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.BatchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.BatchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.CreateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor as InternalEventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorKt.details
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamEventGroupMetadataDescriptorsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptor as internalEventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.getEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupMetadataDescriptorRequest

private val API_VERSION = Version.V2_ALPHA

class EventGroupMetadataDescriptorsService(
  private val internalEventGroupMetadataDescriptorsStub: EventGroupMetadataDescriptorsCoroutineStub
) : EventGroupMetadataDescriptorsCoroutineImplBase() {

  override suspend fun getEventGroupMetadataDescriptor(
    request: GetEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    val key =
      grpcRequireNotNull(EventGroupMetadataDescriptorKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val principal = principalFromCurrentContext

    when (val resourceKey = principal.resourceKey) {
      is DataProviderKey -> {
        if (resourceKey.dataProviderId != key.dataProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get EventGroupMetadataDescriptors belonging to other DataProviders"
          }
        }
      }
      is MeasurementConsumerKey -> {}
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

    return internalEventGroupMetadataDescriptorsStub
      .getEventGroupMetadataDescriptor(getRequest)
      .toEventGroupMetadataDescriptor()
  }

  override suspend fun createEventGroupMetadataDescriptor(
    request: CreateEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val principal = principalFromCurrentContext

    when (val resourceKey = principal.resourceKey) {
      is DataProviderKey -> {
        if (resourceKey.toName() != request.parent) {
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

    return internalEventGroupMetadataDescriptorsStub
      .createEventGroupMetadataDescriptor(
        request.eventGroupMetadataDescriptor.toInternal(parentKey.dataProviderId)
      )
      .toEventGroupMetadataDescriptor()
  }

  override suspend fun updateEventGroupMetadataDescriptor(
    request: UpdateEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    val eventGroupMetadataDescriptorKey =
      grpcRequireNotNull(
        EventGroupMetadataDescriptorKey.fromName(request.eventGroupMetadataDescriptor.name)
      ) { "EventGroupMetadataDescriptor name is either unspecified or invalid" }

    val principal = principalFromCurrentContext

    when (val resourceKey = principal.resourceKey) {
      is DataProviderKey -> {
        if (resourceKey.dataProviderId != eventGroupMetadataDescriptorKey.dataProviderId) {
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

    return internalEventGroupMetadataDescriptorsStub
      .updateEventGroupMetadataDescriptor(
        updateEventGroupMetadataDescriptorRequest {
          eventGroupMetadataDescriptor =
            request.eventGroupMetadataDescriptor.toInternal(
              eventGroupMetadataDescriptorKey.dataProviderId,
              eventGroupMetadataDescriptorKey.eventGroupMetadataDescriptorId
            )
        }
      )
      .toEventGroupMetadataDescriptor()
  }

  override suspend fun batchGetEventGroupMetadataDescriptors(
    request: BatchGetEventGroupMetadataDescriptorsRequest
  ): BatchGetEventGroupMetadataDescriptorsResponse {
    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val principal = principalFromCurrentContext

    when (val resourceKey = principal.resourceKey) {
      is DataProviderKey -> {
        if (resourceKey.dataProviderId != parentKey.dataProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get EventGroupMetadataDescriptors belonging to other DataProviders"
          }
        }
      }
      is MeasurementConsumerKey -> {}
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
        externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
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
          externalIdToApiId(externalEventGroupMetadataDescriptorId)
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
  eventGroupMetadataDescriptorId: String = ""
): InternalEventGroupMetadataDescriptor {
  return internalEventGroupMetadataDescriptor {
    externalDataProviderId = apiIdToExternalId(dataProviderId)
    if (eventGroupMetadataDescriptorId != "")
      externalEventGroupMetadataDescriptorId = apiIdToExternalId(eventGroupMetadataDescriptorId)

    details = details {
      apiVersion = API_VERSION.string
      descriptorSet = this@toInternal.descriptorSet
    }
  }
}
