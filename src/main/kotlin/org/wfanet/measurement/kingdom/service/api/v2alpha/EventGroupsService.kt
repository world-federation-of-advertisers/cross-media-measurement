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
import io.grpc.StatusException
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2.alpha.ListEventGroupsPageToken
import org.wfanet.measurement.api.v2.alpha.ListEventGroupsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.copy
import org.wfanet.measurement.api.v2.alpha.listEventGroupsPageToken
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DeleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt.eventTemplate
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.EventGroup as InternalEventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupKt
import org.wfanet.measurement.internal.kingdom.EventGroupKt.details
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.deleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup as internalEventGroup
import org.wfanet.measurement.internal.kingdom.getEventGroupRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupRequest

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 100
private const val WILDCARD = ResourceKey.WILDCARD_ID
private val API_VERSION = Version.V2_ALPHA

class EventGroupsService(private val internalEventGroupsStub: EventGroupsCoroutineStub) :
  EventGroupsCoroutineImplBase() {

  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    val key =
      grpcRequireNotNull(EventGroupKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext

    when (principal) {
      is DataProviderPrincipal -> {
        if (principal.resourceKey.dataProviderId != key.dataProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get EventGroups belonging to other DataProviders"
          }
        }
      }
      is MeasurementConsumerPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to get EventGroups" }
      }
    }

    val getRequest = getEventGroupRequest {
      externalDataProviderId = apiIdToExternalId(key.dataProviderId)
      externalEventGroupId = apiIdToExternalId(key.eventGroupId)
    }

    val eventGroup =
      try {
        internalEventGroupsStub.getEventGroup(getRequest).toEventGroup()
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "EventGroup not found." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }

    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (eventGroup.measurementConsumer != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get EventGroups belonging to other MeasurementConsumers"
          }
        }
      }
      else -> {}
    }

    return eventGroup
  }

  override suspend fun createEventGroup(request: CreateEventGroupRequest): EventGroup {
    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (principal.resourceKey.toName() != request.parent) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create EventGroups for another DataProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to create EventGroups"
        }
      }
    }

    grpcRequire(
      request.eventGroup.encryptedMetadata.isEmpty ||
        request.eventGroup.hasMeasurementConsumerPublicKey()
    ) {
      "measurement_consumer_public_key must be specified if encrypted_metadata is specified"
    }
    grpcRequire(
      !request.eventGroup.hasMeasurementConsumerPublicKey() ||
        request.eventGroup.measurementConsumerCertificate.isNotBlank()
    ) {
      "measurement_consumer_certificate must be specified if measurement_consumer_public_key is " +
        "specified"
    }

    val createRequest = request.eventGroup.toInternal(parentKey.dataProviderId)
    return try {
      internalEventGroupsStub.createEventGroup(createRequest).toEventGroup()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.DEADLINE_EXCEEDED -> throw Status.DEADLINE_EXCEEDED.asRuntimeException()
        Status.Code.FAILED_PRECONDITION ->
          failGrpc(Status.FAILED_PRECONDITION, ex) { ex.message ?: "Failed precondition" }
        Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "DataProvider not found." }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
      }
    }
  }

  override suspend fun updateEventGroup(request: UpdateEventGroupRequest): EventGroup {
    val eventGroupKey =
      grpcRequireNotNull(EventGroupKey.fromName(request.eventGroup.name)) {
        "EventGroup name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (principal.resourceKey.dataProviderId != eventGroupKey.dataProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot update EventGroups for another DataProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to update EventGroups"
        }
      }
    }

    grpcRequire(
      request.eventGroup.encryptedMetadata.isEmpty ||
        request.eventGroup.hasMeasurementConsumerPublicKey()
    ) {
      "measurement_consumer_public_key must be specified if encrypted_metadata is specified"
    }
    grpcRequire(
      !request.eventGroup.hasMeasurementConsumerPublicKey() ||
        request.eventGroup.measurementConsumerCertificate.isNotBlank()
    ) {
      "measurement_consumer_certificate must be specified if measurement_consumer_public_key is " +
        "specified"
    }

    val updateRequest = updateEventGroupRequest {
      eventGroup =
        request.eventGroup.toInternal(eventGroupKey.dataProviderId, eventGroupKey.eventGroupId)
    }
    return try {
      internalEventGroupsStub.updateEventGroup(updateRequest).toEventGroup()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) { "Required field unspecified or invalid." }
        Status.Code.FAILED_PRECONDITION ->
          failGrpc(Status.FAILED_PRECONDITION, ex) { ex.message ?: "Failed precondition." }
        Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "EventGroup not found." }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
      }
    }
  }

  override suspend fun deleteEventGroup(request: DeleteEventGroupRequest): EventGroup {
    val eventGroupKey =
      grpcRequireNotNull(EventGroupKey.fromName(request.name)) {
        "EventGroup name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (principal.resourceKey.dataProviderId != eventGroupKey.dataProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot delete EventGroups for another DataProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Only a DataProvider can delete an EventGroup" }
      }
    }

    val deleteRequest = deleteEventGroupRequest {
      externalDataProviderId = apiIdToExternalId(eventGroupKey.dataProviderId)
      externalEventGroupId = apiIdToExternalId(eventGroupKey.eventGroupId)
    }

    return try {
      internalEventGroupsStub.deleteEventGroup(deleteRequest).toEventGroup()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) { "Required field unspecified or invalid." }
        Status.Code.FAILED_PRECONDITION ->
          failGrpc(Status.FAILED_PRECONDITION, ex) { ex.message ?: "Failed precondition." }
        Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "EventGroup not found." }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
      }
    }
  }

  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    val listEventGroupsPageToken = request.toListEventGroupPageToken()

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (
          apiIdToExternalId(principal.resourceKey.dataProviderId) !=
            listEventGroupsPageToken.externalDataProviderId
        ) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list EventGroups belonging to other DataProviders"
          }
        }
      }
      is MeasurementConsumerPrincipal -> {
        val externalMeasurementConsumerId =
          apiIdToExternalId(principal.resourceKey.measurementConsumerId)
        if (listEventGroupsPageToken.externalMeasurementConsumerIdsList.isEmpty()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list Event Groups belonging to other MeasurementConsumers"
          }
        }

        listEventGroupsPageToken.externalMeasurementConsumerIdsList.forEach {
          if (it != externalMeasurementConsumerId) {
            failGrpc(Status.PERMISSION_DENIED) {
              "Cannot list Event Groups belonging to other MeasurementConsumers"
            }
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to list EventGroups" }
      }
    }

    val results: List<InternalEventGroup> =
      try {
        internalEventGroupsStub
          .streamEventGroups(listEventGroupsPageToken.toStreamEventGroupsRequest())
          .toList()
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .asRuntimeException()
      }

    if (results.isEmpty()) {
      return ListEventGroupsResponse.getDefaultInstance()
    }

    return listEventGroupsResponse {
      eventGroups +=
        results
          .subList(0, min(results.size, listEventGroupsPageToken.pageSize))
          .map(InternalEventGroup::toEventGroup)
      if (results.size > listEventGroupsPageToken.pageSize) {
        val pageToken =
          listEventGroupsPageToken.copy {
            lastEventGroup = previousPageEnd {
              externalDataProviderId = results[results.lastIndex - 1].externalDataProviderId
              externalEventGroupId = results[results.lastIndex - 1].externalEventGroupId
            }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
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
    if (externalMeasurementConsumerCertificateId != 0L) {
      measurementConsumerCertificate =
        MeasurementConsumerCertificateKey(
            externalIdToApiId(externalMeasurementConsumerId),
            externalIdToApiId(externalMeasurementConsumerCertificateId)
          )
          .toName()
    }
    if (!details.measurementConsumerPublicKey.isEmpty) {
      measurementConsumerPublicKey = signedData {
        data = details.measurementConsumerPublicKey
        signature = details.measurementConsumerPublicKeySignature
      }
    }
    vidModelLines += details.vidModelLinesList
    eventTemplates.addAll(
      details.eventTemplatesList.map { event -> eventTemplate { type = event.fullyQualifiedType } }
    )
    encryptedMetadata = details.encryptedMetadata
    state = this@toEventGroup.state.toV2Alpha()
  }
}

/** Converts a public [EventGroup] to an internal [InternalEventGroup]. */
private fun EventGroup.toInternal(
  dataProviderId: String,
  eventGroupId: String = ""
): InternalEventGroup {
  val measurementConsumerKey =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(this.measurementConsumer)) {
      "Measurement consumer is either unspecified or invalid"
    }
  val measurementConsumerCertificateKey =
    MeasurementConsumerCertificateKey.fromName(this.measurementConsumerCertificate)

  return internalEventGroup {
    externalDataProviderId = apiIdToExternalId(dataProviderId)
    if (eventGroupId.isNotEmpty()) {
      externalEventGroupId = apiIdToExternalId(eventGroupId)
    }
    externalMeasurementConsumerId = apiIdToExternalId(measurementConsumerKey.measurementConsumerId)
    if (measurementConsumerCertificateKey != null) {
      externalMeasurementConsumerCertificateId =
        apiIdToExternalId(measurementConsumerCertificateKey.certificateId)
    }

    providedEventGroupId = eventGroupReferenceId
    details = details {
      apiVersion = API_VERSION.string
      measurementConsumerPublicKey = this@toInternal.measurementConsumerPublicKey.data
      measurementConsumerPublicKeySignature = this@toInternal.measurementConsumerPublicKey.signature
      vidModelLines += this@toInternal.vidModelLinesList
      eventTemplates.addAll(
        this@toInternal.eventTemplatesList.map { event ->
          EventGroupKt.eventTemplate { fullyQualifiedType = event.type }
        }
      )
      encryptedMetadata = this@toInternal.encryptedMetadata
    }
  }
}

/** Converts a public [ListEventGroupsRequest] to an internal [ListEventGroupsPageToken]. */
private fun ListEventGroupsRequest.toListEventGroupPageToken(): ListEventGroupsPageToken {
  val source = this

  grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

  val parentKey: DataProviderKey =
    grpcRequireNotNull(DataProviderKey.fromName(source.parent)) {
      "Parent is either unspecified or invalid"
    }

  grpcRequire(
    (source.filter.measurementConsumersCount > 0 && parentKey.dataProviderId == WILDCARD) ||
      parentKey.dataProviderId != WILDCARD
  ) {
    "Either parent data provider or measurement consumers filter must be provided"
  }

  var externalDataProviderId = 0L
  if (parentKey.dataProviderId != WILDCARD) {
    externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
  }

  val externalMeasurementConsumerIdsList =
    source.filter.measurementConsumersList.map { measurementConsumerName ->
      grpcRequireNotNull(MeasurementConsumerKey.fromName(measurementConsumerName)) {
          "Measurement consumer name in filter invalid"
        }
        .let { key -> apiIdToExternalId(key.measurementConsumerId) }
    }

  return if (source.pageToken.isNotBlank()) {
    ListEventGroupsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
      grpcRequire(this.externalDataProviderId == externalDataProviderId) {
        "Arguments must be kept the same when using a page token"
      }

      grpcRequire(
        externalMeasurementConsumerIdsList.containsAll(externalMeasurementConsumerIds) &&
          externalMeasurementConsumerIds.containsAll(externalMeasurementConsumerIdsList)
      ) {
        "Arguments must be kept the same when using a page token"
      }

      if (
        source.pageSize != 0 && source.pageSize >= MIN_PAGE_SIZE && source.pageSize <= MAX_PAGE_SIZE
      ) {
        pageSize = source.pageSize
      }
      this.showDeleted = source.showDeleted
    }
  } else {
    listEventGroupsPageToken {
      pageSize =
        when {
          source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }

      this.externalDataProviderId = externalDataProviderId
      externalMeasurementConsumerIds += externalMeasurementConsumerIdsList
      this.showDeleted = source.showDeleted
    }
  }
}

/** Converts an internal [ListEventGroupsPageToken] to an internal [StreamEventGroupsRequest]. */
private fun ListEventGroupsPageToken.toStreamEventGroupsRequest(): StreamEventGroupsRequest {
  val source = this
  return streamEventGroupsRequest {
    // get 1 more than the actual page size for deciding whether or not to set page token
    limit = source.pageSize + 1
    filter = filter {
      externalDataProviderId = source.externalDataProviderId
      externalMeasurementConsumerIds += source.externalMeasurementConsumerIdsList
      externalDataProviderIdAfter = source.lastEventGroup.externalDataProviderId
      externalEventGroupIdAfter = source.lastEventGroup.externalEventGroupId
      showDeleted = source.showDeleted
    }
  }
}
