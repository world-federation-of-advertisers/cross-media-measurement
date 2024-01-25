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
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DeleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt.eventTemplate
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
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listEventGroupsPageToken
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.CreateEventGroupRequest as InternalCreateEventGroupRequest
import org.wfanet.measurement.internal.kingdom.EventGroup as InternalEventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupKt.details
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub as InternalEventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest as InternalGetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt as InternalStreamEventGroupsRequests
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest as internalCreateEventGroupRequest
import org.wfanet.measurement.internal.kingdom.deleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup as internalEventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupKey
import org.wfanet.measurement.internal.kingdom.eventTemplate as internalEventTemplate
import org.wfanet.measurement.internal.kingdom.getEventGroupRequest as internalGetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupRequest

class EventGroupsService(private val internalEventGroupsStub: InternalEventGroupsCoroutineStub) :
  EventGroupsCoroutineImplBase() {

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
          }
          .withCause(e)
          .asRuntimeException()
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

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    if (authenticatedPrincipal.resourceKey != eventGroupKey.parentKey) {
      throw Permission.UPDATE.deniedStatus(request.eventGroup.name).asRuntimeException()
    }

    validateRequestEventGroup(request.eventGroup)

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

  /**
   * Validates an `event_group` field from a request.
   *
   * @throws io.grpc.StatusRuntimeException
   */
  private fun validateRequestEventGroup(requestEventGroup: EventGroup) {
    if (requestEventGroup.hasEncryptedMetadata()) {
      grpcRequire(requestEventGroup.hasMeasurementConsumerPublicKey()) {
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
          }
          .withCause(e)
          .asRuntimeException()
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
    return listEventGroupsPageToken {
      if (internalFilter.externalDataProviderId != 0L) {
        externalDataProviderId = internalFilter.externalDataProviderId
      }
      if (internalFilter.externalMeasurementConsumerId != 0L) {
        externalMeasurementConsumerId = internalFilter.externalMeasurementConsumerId
      }
      externalDataProviderIds += internalFilter.externalDataProviderIdsList
      externalMeasurementConsumerIds += internalFilter.externalMeasurementConsumerIdsList
      lastEventGroup = previousPageEnd {
        externalDataProviderId = results[results.lastIndex - 1].externalDataProviderId
        externalEventGroupId = results[results.lastIndex - 1].externalEventGroupId
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
    parentKey: ResourceKey,
    pageSize: Int,
    pageToken: ListEventGroupsPageToken?,
  ): StreamEventGroupsRequest {
    return streamEventGroupsRequest {
      this.filter =
        InternalStreamEventGroupsRequests.filter {
          if (parentKey is DataProviderKey) {
            externalDataProviderId = ApiId(parentKey.dataProviderId).externalId.value
          }
          if (parentKey is MeasurementConsumerKey) {
            externalMeasurementConsumerId = ApiId(parentKey.measurementConsumerId).externalId.value
          }
          if (filter.measurementConsumersList.isNotEmpty()) {
            externalMeasurementConsumerIds +=
              filter.measurementConsumersList.map {
                val measurementConsumerKey =
                  grpcRequireNotNull(MeasurementConsumerKey.fromName(it)) {
                    "Invalid resource name in filter.measurement_consumers"
                  }
                ApiId(measurementConsumerKey.measurementConsumerId).externalId.value
              }
          }
          if (filter.dataProvidersList.isNotEmpty()) {
            externalDataProviderIds +=
              filter.dataProvidersList.map {
                val dataProviderKey =
                  grpcRequireNotNull(DataProviderKey.fromName(it)) {
                    "Invalid resource name in filter.data_providers"
                  }
                ApiId(dataProviderKey.dataProviderId).externalId.value
              }
          }
          this.showDeleted = showDeleted
          if (pageToken != null) {
            if (
              pageToken.externalDataProviderId != externalDataProviderId ||
                pageToken.externalMeasurementConsumerId != externalMeasurementConsumerId ||
                pageToken.showDeleted != showDeleted ||
                pageToken.externalDataProviderIdsList != externalDataProviderIds ||
                pageToken.externalMeasurementConsumerIdsList != externalMeasurementConsumerIds
            ) {
              throw Status.INVALID_ARGUMENT.withDescription(
                  "Arguments other than page_size must remain the same for subsequent page requests"
                )
                .asRuntimeException()
            }
            after = eventGroupKey {
              externalDataProviderId = pageToken.lastEventGroup.externalDataProviderId
              externalEventGroupId = pageToken.lastEventGroup.externalEventGroupId
            }
          }
        }
      limit = pageSize + 1
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000
  }
}

/** Converts an internal [InternalEventGroup] to a public [EventGroup]. */
private fun InternalEventGroup.toEventGroup(): EventGroup {
  return eventGroup {
    name =
      EventGroupKey(
          externalIdToApiId(externalDataProviderId),
          externalIdToApiId(externalEventGroupId),
        )
        .toName()
    measurementConsumer =
      MeasurementConsumerKey(externalIdToApiId(externalMeasurementConsumerId)).toName()
    eventGroupReferenceId = providedEventGroupId
    if (hasDetails()) {
      val apiVersion = Version.fromString(details.apiVersion)
      if (!details.measurementConsumerPublicKey.isEmpty) {
        measurementConsumerPublicKey = any {
          value = details.measurementConsumerPublicKey
          typeUrl =
            when (apiVersion) {
              Version.V2_ALPHA -> ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
            }
        }
        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
        // field.
        signedMeasurementConsumerPublicKey = signedMessage {
          message = this@eventGroup.measurementConsumerPublicKey
          signature = details.measurementConsumerPublicKeySignature
          signatureAlgorithmOid = details.measurementConsumerPublicKeySignatureAlgorithmOid
        }
      }
      vidModelLines += details.vidModelLinesList
      eventTemplates.addAll(
        details.eventTemplatesList.map { event ->
          eventTemplate { type = event.fullyQualifiedType }
        }
      )
      if (!details.encryptedMetadata.isEmpty) {
        encryptedMetadata = encryptedMessage {
          ciphertext = details.encryptedMetadata
          typeUrl =
            when (apiVersion) {
              Version.V2_ALPHA -> ProtoReflection.getTypeUrl(EventGroup.Metadata.getDescriptor())
            }
        }
        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
        // field.
        serializedEncryptedMetadata = details.encryptedMetadata
      }
      if (details.hasDataAvailabilityInterval()) {
        dataAvailabilityInterval = details.dataAvailabilityInterval
      }
    }
    state = this@toEventGroup.state.toV2Alpha()
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
    details = details {
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
        measurementConsumerPublicKey = source.signedMeasurementConsumerPublicKey.unpack()
      }

      vidModelLines += source.vidModelLinesList
      eventTemplates.addAll(
        source.eventTemplatesList.map { event ->
          internalEventTemplate { fullyQualifiedType = event.type }
        }
      )
      if (source.hasEncryptedMetadata()) {
        encryptedMetadata = source.encryptedMetadata.ciphertext
      } else if (!source.serializedEncryptedMetadata.isEmpty) {
        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop reading this
        // field.
        encryptedMetadata = source.serializedEncryptedMetadata
      }
      if (source.hasDataAvailabilityInterval()) {
        dataAvailabilityInterval = source.dataAvailabilityInterval
      }
    }
  }
}
