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

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import io.grpc.Status
import io.grpc.StatusException
import java.security.GeneralSecurityException
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.pb.Checked
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
import org.projectnessie.cel.common.types.ref.Val
import org.wfanet.measurement.api.v2alpha.DataProviderKey as CmmsDataProviderKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.consent.client.measurementconsumer.decryptMetadata
import org.wfanet.measurement.reporting.v1alpha.EventGroup
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.eventTemplate
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.metadata
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v1alpha.eventGroup
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsResponse

private const val METADATA_FIELD = "metadata.metadata"

class EventGroupsService(
  private val cmmsEventGroupsStub: EventGroupsCoroutineStub,
  private val eventGroupsMetadataDescriptorsStub: EventGroupMetadataDescriptorsCoroutineStub,
  private val encryptionKeyPairStore: EncryptionKeyPairStore,
  /**
   * Registry of known (compiled-in) metadata message types.
   *
   * TODO(projectnessie/cel-java#295): Remove when fixed.
   */
  private val knownMetadataTypes: TypeRegistry = TypeRegistry.getEmptyTypeRegistry()
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

    val cmmsListEventGroupResponse =
      try {
        cmmsEventGroupsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .listEventGroups(
            cmmsListEventGroupsRequest {
              parent = dataProviderName
              pageSize = request.pageSize
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

    val filteredEventGroups =
      filterEventGroups(eventGroups, apiAuthenticationKey, dataProviderName, filter)
    return listEventGroupsResponse {
      this.eventGroups += filteredEventGroups
      nextPageToken = cmmsListEventGroupResponse.nextPageToken
    }
  }

  private suspend fun filterEventGroups(
    eventGroups: Iterable<EventGroup>,
    apiAuthenticationKey: String,
    dataProviderName: String,
    filter: String,
  ): List<EventGroup> {
    // Build TypeRegistry for filter evaluation.
    val eventGroupMetadataList = eventGroups.filter { it.hasMetadata() }.map { it.metadata }
    val descriptorResourceNames =
      eventGroupMetadataList.map { it.eventGroupMetadataDescriptor }.distinct()
    val descriptorResources: List<EventGroupMetadataDescriptor> =
      getEventGroupMetadataDescriptors(
        apiAuthenticationKey,
        dataProviderName,
        descriptorResourceNames
      )
    val fileDescriptorSets: List<DescriptorProtos.FileDescriptorSet> =
      descriptorResources.map { it.descriptorSet }
    val typeRegistry: TypeRegistry = buildTypeRegistry(fileDescriptorSets)
    val env = buildCelEnvironment(eventGroupMetadataList, typeRegistry)

    val astAndIssues = env.compile(filter)
    if (astAndIssues.hasIssues()) {
      throw Status.INVALID_ARGUMENT.withDescription(
          "filter is not a valid CEL expression: ${astAndIssues.issues}"
        )
        .asRuntimeException()
    }
    val program = env.program(astAndIssues.ast)

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

  private fun buildCelEnvironment(
    eventGroupMetadataList: List<EventGroup.Metadata>,
    metadataTypeRegistry: TypeRegistry,
  ): Env {
    // Build CEL ProtoTypeRegistry.
    val celTypeRegistry = ProtoTypeRegistry.newRegistry()
    eventGroupMetadataList
      .distinctBy { it.metadata.typeUrl }
      .forEach {
        val typeUrl = it.metadata.typeUrl
        val descriptor =
          metadataTypeRegistry.getDescriptorForTypeUrl(typeUrl)
            ?: throw Status.FAILED_PRECONDITION.withDescription(
                "${it.eventGroupMetadataDescriptor} does not contain descriptor for $typeUrl"
              )
              .asRuntimeException()
        celTypeRegistry.registerDescriptor(descriptor.file)
      }
    celTypeRegistry.registerMessage(EventGroup.getDefaultInstance())

    // Build CEL Env.
    val eventGroupDescriptor = EventGroup.getDescriptor()
    val env =
      Env.newEnv(
        EnvOption.container(eventGroupDescriptor.fullName),
        EnvOption.customTypeProvider(celTypeRegistry),
        EnvOption.customTypeAdapter(celTypeRegistry),
        EnvOption.declarations(
          eventGroupDescriptor.fields
            .map {
              Decls.newVar(
                it.name,
                celTypeRegistry.findFieldType(eventGroupDescriptor.fullName, it.name).type
              )
            }
            // TODO(projectnessie/cel-java#295): Remove when fixed.
            .plus(Decls.newVar(METADATA_FIELD, Checked.checkedAny))
        )
      )
    return env
  }

  private suspend fun getEventGroupMetadataDescriptors(
    apiAuthenticationKey: String,
    dataProviderName: String,
    descriptorResourceNames: Iterable<String>,
  ): List<EventGroupMetadataDescriptor> {
    return try {
      eventGroupsMetadataDescriptorsStub
        .withAuthenticationKey(apiAuthenticationKey)
        .batchGetEventGroupMetadataDescriptors(
          batchGetEventGroupMetadataDescriptorsRequest {
            parent = dataProviderName
            names += descriptorResourceNames
          }
        )
        .eventGroupMetadataDescriptorsList
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          Status.Code.CANCELLED -> Status.CANCELLED
          else -> Status.UNKNOWN
        }
        .withDescription("Error retrieving EventGroupMetadataDescriptors")
        .withCause(e)
        .asRuntimeException()
    }
  }

  private fun buildTypeRegistry(
    fileDescriptorSets: Iterable<DescriptorProtos.FileDescriptorSet>
  ): TypeRegistry {
    val fileDescriptors: List<Descriptors.Descriptor> =
      ProtoReflection.buildDescriptors(fileDescriptorSets)

    return TypeRegistry.newBuilder()
      .apply {
        for (descriptor in fileDescriptors) {
          add(knownMetadataTypes.find(descriptor.fullName) ?: descriptor)
        }
      }
      .build()
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
