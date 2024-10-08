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
import io.grpc.Status
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.util.concurrent.TimeUnit
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.ref.Val
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as CmmsEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.consent.client.measurementconsumer.decryptMetadata
import org.wfanet.measurement.reporting.service.api.CelEnvProvider
import org.wfanet.measurement.reporting.service.api.EncryptionKeyPairStore
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupKt
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.eventGroup
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsResponse

class EventGroupsService(
  private val cmmsEventGroupsStub: CmmsEventGroupsCoroutineStub,
  private val encryptionKeyPairStore: EncryptionKeyPairStore,
  private val celEnvProvider: CelEnvProvider,
) : EventGroupsCoroutineImplBase() {
  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          throw Status.PERMISSION_DENIED.withDescription(
              "Cannot list event groups for another MeasurementConsumer"
            )
            .asRuntimeException()
        }
      }
    }

    val apiAuthenticationKey: String = principal.config.apiKey

    grpcRequire(request.pageSize >= 0) { "page_size cannot be negative" }

    val pageSize =
      when {
        request.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
        request.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
        else -> request.pageSize
      }

    var nextPageToken = request.pageToken
    val deadline = Context.current().deadline ?: Deadline.after(30, TimeUnit.SECONDS)
    do {
      val cmmsListEventGroupResponse =
        try {
          cmmsEventGroupsStub
            .withAuthenticationKey(apiAuthenticationKey)
            .listEventGroups(
              listEventGroupsRequest {
                parent = parentKey.toName()
                this.pageSize = pageSize
                pageToken = nextPageToken
              }
            )
        } catch (e: StatusException) {
          throw when (e.status.code) {
              Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
              Status.Code.CANCELLED -> Status.CANCELLED
              else -> Status.UNKNOWN
            }
            .withCause(e)
            .asRuntimeException()
        }
      val cmmsEventGroups = cmmsListEventGroupResponse.eventGroupsList

      val eventGroups =
        cmmsEventGroups.map {
          val cmmsMetadata: CmmsEventGroup.Metadata? =
            if (it.hasEncryptedMetadata()) {
              decryptMetadata(it, principal.resourceKey.toName())
            } else {
              null
            }

          it.toEventGroup(cmmsMetadata)
        }

      val filteredEventGroups = filterEventGroups(eventGroups, request.filter)
      if (filteredEventGroups.size > 0) {
        return listEventGroupsResponse {
          this.eventGroups += filteredEventGroups
          this.nextPageToken = cmmsListEventGroupResponse.nextPageToken
        }
      } else {
        nextPageToken = cmmsListEventGroupResponse.nextPageToken
      }
    } while (deadline.timeRemaining(TimeUnit.SECONDS) > 5)

    return listEventGroupsResponse { this.nextPageToken = nextPageToken }
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

    private const val MIN_PAGE_SIZE = 1
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000
  }
}
