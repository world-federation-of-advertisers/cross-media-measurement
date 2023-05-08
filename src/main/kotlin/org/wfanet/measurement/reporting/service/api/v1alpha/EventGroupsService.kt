// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.protobuf.DynamicMessage
import io.grpc.Status
import io.grpc.StatusException
import java.security.GeneralSecurityException
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.ref.Val
import org.wfanet.measurement.api.v2alpha.DataProviderKey as CmmsDataProviderKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.consent.client.measurementconsumer.decryptMetadata
import org.wfanet.measurement.reporting.service.api.CelEnvProvider
import org.wfanet.measurement.reporting.service.api.EncryptionKeyPairStore
import org.wfanet.measurement.reporting.v1alpha.EventGroup
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.eventTemplate
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.metadata
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v1alpha.eventGroup
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsResponse

private const val METADATA_FIELD = "metadata.metadata"

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class EventGroupsService(
  private val cmmsEventGroupsStub: EventGroupsCoroutineStub,
  private val encryptionKeyPairStore: EncryptionKeyPairStore,
  private val celEnvProvider: CelEnvProvider,
) : EventGroupsCoroutineImplBase() {
  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    val principal: ReportingPrincipal = principalFromCurrentContext

    if (principal !is MeasurementConsumerPrincipal) {
      failGrpc(Status.PERMISSION_DENIED) {
        "Cannot list event groups with entities other than measurement consumer."
      }
    }

    val principalName = principal.resourceKey.toName()
    val apiAuthenticationKey: String = principal.config.apiKey
    val parentKey =
      EventGroupParentKey.fromName(request.parent)
        ?: failGrpc(Status.INVALID_ARGUMENT) { "parent malformed or unspecified" }
    val dataProviderKey = CmmsDataProviderKey(parentKey.dataProviderReferenceId)
    val dataProviderName = dataProviderKey.toName()
    val pageSize =
      when {
        request.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
        request.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
        else -> request.pageSize
      }

    val cmmsListEventGroupResponse =
      try {
        cmmsEventGroupsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .listEventGroups(
            cmmsListEventGroupsRequest {
              parent = dataProviderName
              this.pageSize = pageSize
              pageToken = request.pageToken
              filter = filter { measurementConsumers += principalName }
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
          if (it.encryptedMetadata.isEmpty) {
            null
          } else {
            decryptMetadata(it, principalName)
          }

        it.toEventGroup(cmmsMetadata)
      }

    val filter: String = request.filter
    if (filter.isEmpty()) {
      return listEventGroupsResponse {
        this.eventGroups += eventGroups
        nextPageToken = cmmsListEventGroupResponse.nextPageToken
      }
    }

    val filteredEventGroups = filterEventGroups(eventGroups, filter)
    return listEventGroupsResponse {
      this.eventGroups += filteredEventGroups
      nextPageToken = cmmsListEventGroupResponse.nextPageToken
    }
  }

  private suspend fun filterEventGroups(
    eventGroups: Iterable<EventGroup>,
    filter: String,
  ): List<EventGroup> {
    val typeRegistryAndEnv = celEnvProvider.getTypeRegistryAndEnv()
    val env = typeRegistryAndEnv.env
    val typeRegistry = typeRegistryAndEnv.typeRegistry

    val astAndIssues = env.compile(filter)
    if (astAndIssues.hasIssues()) {
      throw Status.INVALID_ARGUMENT.withDescription(
          "filter is not a valid CEL expression: ${astAndIssues.issues}"
        )
        .asRuntimeException()
    }
    val program = env.program(astAndIssues.ast)

    eventGroups
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
                metadata.value
              )
            )
          }
        }
      val result: Val = program.eval(variables).`val`
      if (result is Err) {
        throw result.toRuntimeException()
      }
      result.booleanValue()
    }
  }

  private suspend fun decryptMetadata(
    cmmsEventGroup: CmmsEventGroup,
    principalName: String,
  ): CmmsEventGroup.Metadata {
    if (!cmmsEventGroup.hasMeasurementConsumerPublicKey()) {
      failGrpc(Status.FAILED_PRECONDITION) {
        "EventGroup ${cmmsEventGroup.name} has encrypted metadata but no encryption public key"
      }
    }
    val encryptionKey =
      EncryptionPublicKey.parseFrom(cmmsEventGroup.measurementConsumerPublicKey.data)
    val decryptionKeyHandle: PrivateKeyHandle =
      encryptionKeyPairStore.getPrivateKeyHandle(principalName, encryptionKey.data)
        ?: failGrpc(Status.FAILED_PRECONDITION) {
          "Public key does not have corresponding private key"
        }
    return try {
      decryptMetadata(cmmsEventGroup.encryptedMetadata, decryptionKeyHandle)
    } catch (e: GeneralSecurityException) {
      throw Status.FAILED_PRECONDITION.withCause(e)
        .withDescription("Metadata cannot be decrypted")
        .asRuntimeException()
    }
  }
}

private fun CmmsEventGroup.toEventGroup(cmmsMetadata: CmmsEventGroup.Metadata?): EventGroup {
  val source = this
  val cmmsEventGroupKey = requireNotNull(CmmsEventGroupKey.fromName(name))
  val measurementConsumerKey = requireNotNull(MeasurementConsumerKey.fromName(measurementConsumer))
  return eventGroup {
    name =
      EventGroupKey(
          measurementConsumerKey.measurementConsumerId,
          cmmsEventGroupKey.dataProviderId,
          cmmsEventGroupKey.eventGroupId
        )
        .toName()
    dataProvider = CmmsDataProviderKey(cmmsEventGroupKey.dataProviderId).toName()
    eventGroupReferenceId = source.eventGroupReferenceId
    eventTemplates += source.eventTemplatesList.map { eventTemplate { type = it.type } }
    if (cmmsMetadata != null) {
      metadata = metadata {
        eventGroupMetadataDescriptor = cmmsMetadata.eventGroupMetadataDescriptor
        metadata = cmmsMetadata.metadata
      }
    }
  }
}
