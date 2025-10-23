/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.DynamicMessage
import com.google.protobuf.kotlin.unpack
import io.grpc.Context
import io.grpc.Deadline
import io.grpc.Deadline.Ticker
import io.grpc.Status
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transformWhile
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.ref.Val
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest as CmmsListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt as CmmsListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsResponse as CmmsListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.consent.client.measurementconsumer.decryptMetadata
import org.wfanet.measurement.reporting.service.api.CelEnvProvider
import org.wfanet.measurement.reporting.service.api.EncryptionKeyPairStore
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupKt
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.eventGroup
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsResponse

class EventGroupsService(
  private val cmmsEventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val authorization: Authorization,
  private val celEnvProvider: CelEnvProvider,
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  private val encryptionKeyPairStore: EncryptionKeyPairStore,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val ticker: Ticker = Deadline.getSystemTicker(),
) : EventGroupsCoroutineImplBase(coroutineContext) {
  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    authorization.check(request.parent, LIST_EVENT_GROUPS_PERMISSIONS)

    val deadline: Deadline =
      Context.current().deadline
        ?: Deadline.after(RPC_DEFAULT_DEADLINE_MILLIS, TimeUnit.MILLISECONDS, ticker)
    val measurementConsumerConfig =
      measurementConsumerConfigs.configsMap[request.parent]
        ?: throw Status.INTERNAL.withDescription(
            "MeasurementConsumerConfig not found for ${request.parent}"
          )
          .asRuntimeException()
    val apiAuthenticationKey: String = measurementConsumerConfig.apiKey

    grpcRequire(request.pageSize >= 0) { "page_size cannot be negative" }

    val limit =
      if (request.pageSize > 0) request.pageSize.coerceAtMost(MAX_PAGE_SIZE) else DEFAULT_PAGE_SIZE
    val parent = parentKey.toName()
    val eventGroupLists: Flow<ResourceList<EventGroup, String>> =
      cmmsEventGroupsStub.withAuthenticationKey(apiAuthenticationKey).listResources(
        limit,
        request.pageToken,
      ) { pageToken, remaining ->
        val response: CmmsListEventGroupsResponse =
          listEventGroups(
            cmmsListEventGroupsRequest {
              this.parent = parent
              this.pageSize = remaining
              this.pageToken = pageToken
              if (request.hasStructuredFilter()) {
                filter = request.structuredFilter.toCmmsFilter()
              }
              if (request.hasOrderBy()) {
                orderBy = request.orderBy.toCmmsOrderBy()
              }
            }
          )

        val eventGroups: List<EventGroup> =
          response.eventGroupsList
            .map {
              val cmmsMetadata: CmmsEventGroup.Metadata? =
                if (it.hasEncryptedMetadata()) {
                  decryptMetadata(it, parentKey.toName())
                } else {
                  null
                }

              it.toEventGroup(cmmsMetadata)
            }
            .let {
              // TODO(@SanjayVas): Stop reading deprecated field.
              if (request.hasFilter()) {
                filterEventGroups(it, request.filter)
              } else {
                it
              }
            }

        ResourceList(eventGroups, response.nextPageToken)
      }

    var hasResponse = false
    return listEventGroupsResponse {
      try {
        eventGroupLists
          .transformWhile {
            emit(it)
            deadline.timeRemaining(TimeUnit.MILLISECONDS) > RPC_DEADLINE_OVERHEAD_MILLIS
          }
          .collect { eventGroupList ->
            this.eventGroups += eventGroupList
            nextPageToken = eventGroupList.nextPageToken
            hasResponse = true
          }
      } catch (e: StatusException) {
        when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.CANCELLED -> {
            if (!hasResponse) {
              // Only throw an error if we don't have any response yet. Otherwise, just return what
              // we have so far.
              throw Status.DEADLINE_EXCEEDED.withDescription(
                  "Timed out listing EventGroups from backend"
                )
                .withCause(e)
                .asRuntimeException()
            }
          }
          else ->
            throw Status.INTERNAL.withDescription("Error listing EventGroups from backend")
              .withCause(e)
              .asRuntimeException()
        }
      }
    }
  }

  private suspend fun filterEventGroups(
    eventGroups: List<EventGroup>,
    filter: String,
  ): List<EventGroup> {
    if (filter.isEmpty()) {
      return eventGroups
    }

    val typeRegistryAndEnv = celEnvProvider.getTypeRegistryAndEnv()
    val env = typeRegistryAndEnv.env
    val typeRegistry = typeRegistryAndEnv.typeRegistry

    val astAndIssues =
      try {
        env.compile(filter)
      } catch (_: NullPointerException) {
        // NullPointerException is thrown when an operator in the filter is not a CEL operator.
        throw Status.INVALID_ARGUMENT.withDescription("filter is not a valid CEL expression")
          .asRuntimeException()
      }
    if (astAndIssues.hasIssues()) {
      throw Status.INVALID_ARGUMENT.withDescription(
          "filter is not a valid CEL expression: ${astAndIssues.issues}"
        )
        .asRuntimeException()
    }
    val program = env.program(astAndIssues.ast)

    eventGroups
      .filter { it.hasMetadata() }
      .distinctBy { it.metadata.metadata.typeUrl }
      .forEach {
        val typeUrl = it.metadata.metadata.typeUrl
        typeRegistry.getDescriptorForTypeUrl(typeUrl)
          ?: throw Status.FAILED_PRECONDITION.withDescription(
              "${it.metadata.eventGroupMetadataDescriptor} does not contain descriptor for $typeUrl"
            )
            .asRuntimeException()
      }

    return eventGroups.filter { eventGroup ->
      val variables: Map<String, Any> =
        mutableMapOf<String, Any>().apply {
          for (fieldDescriptor in eventGroup.descriptorForType.fields) {
            put(fieldDescriptor.name, eventGroup.getField(fieldDescriptor))
          }
          // TODO(projectnessie/cel-java#295): Remove when fixed.
          if (eventGroup.hasMetadata()) {
            val metadata: com.google.protobuf.Any = eventGroup.metadata.metadata
            put(
              METADATA_FIELD,
              DynamicMessage.parseFrom(
                typeRegistry.getDescriptorForTypeUrl(metadata.typeUrl),
                metadata.value,
              ),
            )
          }
        }
      val result: Val = program.eval(variables).`val`
      if (result is Err) {
        throw result.toRuntimeException()
      }

      if (result.value() !is Boolean) {
        throw Status.INVALID_ARGUMENT.withDescription("filter does not evaluate to boolean")
          .asRuntimeException()
      }

      result.booleanValue()
    }
  }

  private suspend fun decryptMetadata(
    cmmsEventGroup: CmmsEventGroup,
    principalName: String,
  ): CmmsEventGroup.Metadata {
    if (!cmmsEventGroup.hasMeasurementConsumerPublicKey()) {
      throw Status.FAILED_PRECONDITION.withDescription(
          "EventGroup ${cmmsEventGroup.name} has encrypted metadata but no encryption public key"
        )
        .asRuntimeException()
    }
    val encryptionKey: EncryptionPublicKey = cmmsEventGroup.measurementConsumerPublicKey.unpack()
    val decryptionKeyHandle: PrivateKeyHandle =
      encryptionKeyPairStore.getPrivateKeyHandle(principalName, encryptionKey.data)
        ?: throw Status.FAILED_PRECONDITION.withDescription(
            "Public key does not have corresponding private key"
          )
          .asRuntimeException()

    return try {
      decryptMetadata(cmmsEventGroup.encryptedMetadata, decryptionKeyHandle)
    } catch (e: GeneralSecurityException) {
      throw Status.FAILED_PRECONDITION.withCause(e)
        .withDescription("Metadata cannot be decrypted")
        .asRuntimeException()
    }
  }

  private fun CmmsEventGroup.toEventGroup(cmmsMetadata: CmmsEventGroup.Metadata?): EventGroup {
    val source = this
    val cmmsEventGroupKey = requireNotNull(CmmsEventGroupKey.fromName(name))
    val measurementConsumerKey =
      requireNotNull(MeasurementConsumerKey.fromName(measurementConsumer))
    return eventGroup {
      name =
        EventGroupKey(measurementConsumerKey.measurementConsumerId, cmmsEventGroupKey.eventGroupId)
          .toName()
      cmmsEventGroup = source.name
      cmmsDataProvider = DataProviderKey(cmmsEventGroupKey.dataProviderId).toName()
      eventGroupReferenceId = source.eventGroupReferenceId
      eventTemplates +=
        source.eventTemplatesList.map { EventGroupKt.eventTemplate { type = it.type } }
      mediaTypes += source.mediaTypesList.map { it.toMediaType() }
      if (source.hasDataAvailabilityInterval()) {
        dataAvailabilityInterval = source.dataAvailabilityInterval
      }
      if (source.hasEventGroupMetadata()) {
        eventGroupMetadata =
          EventGroupKt.eventGroupMetadata {
            @Suppress(
              "WHEN_ENUM_CAN_BE_NULL_IN_JAVA"
            ) // Protobuf enum accessors cannot return null.
            when (source.eventGroupMetadata.selectorCase) {
              EventGroupMetadata.SelectorCase.AD_METADATA -> {
                adMetadata =
                  EventGroupKt.EventGroupMetadataKt.adMetadata {
                    campaignMetadata =
                      EventGroupKt.EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                        brandName = source.eventGroupMetadata.adMetadata.campaignMetadata.brandName
                        campaignName =
                          source.eventGroupMetadata.adMetadata.campaignMetadata.campaignName
                      }
                  }
              }
              EventGroupMetadata.SelectorCase.SELECTOR_NOT_SET -> error("metadata not set")
            }
          }
      }
      if (cmmsMetadata != null) {
        metadata =
          EventGroupKt.metadata {
            eventGroupMetadataDescriptor = cmmsMetadata.eventGroupMetadataDescriptor
            metadata = cmmsMetadata.metadata
          }
      }
    }
  }

  companion object {
    private const val METADATA_FIELD = "metadata.metadata"

    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000

    /** Overhead to allow for RPC deadlines in milliseconds. */
    private const val RPC_DEADLINE_OVERHEAD_MILLIS = 100L

    /** Default RPC deadline in milliseconds. */
    private const val RPC_DEFAULT_DEADLINE_MILLIS = 30_000L

    val LIST_EVENT_GROUPS_PERMISSIONS = setOf("reporting.eventGroups.list")
  }
}

private fun CmmsMediaType.toMediaType(): MediaType {
  return when (this) {
    CmmsMediaType.VIDEO -> MediaType.VIDEO
    CmmsMediaType.DISPLAY -> MediaType.DISPLAY
    CmmsMediaType.OTHER -> MediaType.OTHER
    CmmsMediaType.MEDIA_TYPE_UNSPECIFIED -> MediaType.MEDIA_TYPE_UNSPECIFIED
    CmmsMediaType.UNRECOGNIZED -> error("MediaType unrecognized")
  }
}

private fun MediaType.toCmmsMediaType(): CmmsMediaType {
  return when (this) {
    MediaType.VIDEO -> CmmsMediaType.VIDEO
    MediaType.DISPLAY -> CmmsMediaType.DISPLAY
    MediaType.OTHER -> CmmsMediaType.OTHER
    MediaType.MEDIA_TYPE_UNSPECIFIED -> CmmsMediaType.MEDIA_TYPE_UNSPECIFIED
    MediaType.UNRECOGNIZED -> error("MediaType unrecognized")
  }
}

private fun ListEventGroupsRequest.OrderBy.toCmmsOrderBy(): CmmsListEventGroupsRequest.OrderBy {
  val source = this
  return CmmsListEventGroupsRequestKt.orderBy {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
    field =
      when (source.field) {
        ListEventGroupsRequest.OrderBy.Field.FIELD_UNSPECIFIED ->
          CmmsListEventGroupsRequest.OrderBy.Field.FIELD_UNSPECIFIED
        ListEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME ->
          CmmsListEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
        ListEventGroupsRequest.OrderBy.Field.UNRECOGNIZED -> error("Unrecognized OrderBy.Field")
      }
    descending = source.descending
  }
}

private fun ListEventGroupsRequest.Filter.toCmmsFilter(): CmmsListEventGroupsRequest.Filter {
  val source = this
  return CmmsListEventGroupsRequestKt.filter {
    dataProviderIn += source.cmmsDataProviderInList
    for (mediaType in source.mediaTypesIntersectList) {
      mediaTypesIntersect += mediaType.toCmmsMediaType()
    }
    if (source.hasDataAvailabilityEndTimeOnOrBefore()) {
      dataAvailabilityEndTimeOnOrBefore = source.dataAvailabilityEndTimeOnOrBefore
    }
    if (source.hasDataAvailabilityStartTimeOnOrAfter()) {
      dataAvailabilityStartTimeOnOrAfter = source.dataAvailabilityStartTimeOnOrAfter
    }
    if (source.hasDataAvailabilityStartTimeOnOrBefore()) {
      dataAvailabilityStartTimeOnOrBefore = source.dataAvailabilityStartTimeOnOrBefore
    }
    if (source.hasDataAvailabilityEndTimeOnOrAfter()) {
      dataAvailabilityEndTimeOnOrAfter = source.dataAvailabilityEndTimeOnOrAfter
    }
    metadataSearchQuery = source.metadataSearchQuery
  }
}
