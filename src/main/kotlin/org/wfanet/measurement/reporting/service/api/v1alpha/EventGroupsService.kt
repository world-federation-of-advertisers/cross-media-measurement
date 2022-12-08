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

import io.grpc.Status
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.DataProviderKey as CmmsDataProviderKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataParser
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import com.google.api.expr.v1alpha1.Decl
import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.Descriptors
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FileDescriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import com.google.protobuf.copy
import com.google.protobuf.fileDescriptorProto
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.consent.client.measurementconsumer.decryptMetadata
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.reporting.v1alpha.EventGroup
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.eventTemplate
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.metadata
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v1alpha.eventGroup
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsResponse

private const val EVENT_GROUP_METADATA_FIELD_DESCRIPTOR_NAME_PREFIX = "wfa.measurement.reporting.v1alpha.EventGroup.Metadata."

class EventGroupsService(
  private val cmmsEventGroupsStub: EventGroupsCoroutineStub,
  private val eventGroupsMetadataDescriptorsStub: EventGroupMetadataDescriptorsCoroutineStub,
  private val encryptionKeyPairStore: EncryptionKeyPairStore
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
    val dataProviderName =
      CmmsDataProviderKey(
          (EventGroupParentKey.fromName(request.parent)
              ?: failGrpc(Status.FAILED_PRECONDITION) { "Event group parent unable to be parsed" })
            .dataProviderReferenceId
        )
        .toName()

    val cmmsListEventGroupResponse =
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
    val cmmsEventGroups = cmmsListEventGroupResponse.eventGroupsList
    val parsedEventGroupMetadataMap: Map<String, CmmsEventGroup.Metadata> =
      cmmsEventGroups
        .filter { !it.encryptedMetadata.isEmpty }
        .associate {
          val measurementConsumerPrivateKey =
            encryptionKeyPairStore.getPrivateKeyHandle(
              principalName,
              EncryptionPublicKey.parseFrom(it.measurementConsumerPublicKey.data).data
            )
              ?: failGrpc(Status.FAILED_PRECONDITION) {
                "Public key does not have corresponding private key"
              }
          it.name to decryptMetadata(it.encryptedMetadata, measurementConsumerPrivateKey)
        }

    if (request.filter.isBlank())
      return listEventGroupsResponse {
        cmmsEventGroups.forEach {
          if (it.encryptedMetadata.isEmpty) {
            this.eventGroups += it.toEventGroup()
          } else {
            this.eventGroups += it.toEventGroup(parsedEventGroupMetadataMap)
          }
        }
        nextPageToken = cmmsListEventGroupResponse.nextPageToken
      }
    val eventGroupMetadataDescriptors: List<EventGroupMetadataDescriptor> =
      eventGroupsMetadataDescriptorsStub
        .withAuthenticationKey(apiAuthenticationKey)
        .batchGetEventGroupMetadataDescriptors(
          batchGetEventGroupMetadataDescriptorsRequest {
            parent = dataProviderName
            names +=
              parsedEventGroupMetadataMap.values.map { it.eventGroupMetadataDescriptor }.toSet()
          }
        )
        .eventGroupMetadataDescriptorsList
    val metadataParser = EventGroupMetadataParser(eventGroupMetadataDescriptors)
    val filteredEventGroups: MutableList<EventGroup> = mutableListOf()

    for (cmmsEventGroup in cmmsEventGroups) {
      val reportingEventGroup: EventGroup
      val program: Program
      try {
        if (cmmsEventGroup.encryptedMetadata.isEmpty) {
          reportingEventGroup = cmmsEventGroup.toEventGroup()
          program = compileProgram(request.filter)
        } else {
          val metadata = parsedEventGroupMetadataMap.getValue(cmmsEventGroup.name)
          var metadataMessage: DynamicMessage =
            metadataParser.convertToDynamicMessage(metadata)
              ?: failGrpc(Status.FAILED_PRECONDITION) {
                "Event group metadata message descriptor not found"
              }
          val descriptorProto = metadataMessage.descriptorForType.toProto()
          val modifiedDescriptorProto = descriptorProto.copy {
            val messageName = name
            //field.clear()
            //descriptorProto.fieldList.forEach { it ->
            //  field += it.copy {
            //    typeName = "$EVENT_GROUP_METADATA_FIELD_DESCRIPTOR_NAME_PREFIX$messageName." + it.name.replaceFirstChar { it.titlecaseChar() }
            //  }
            //}
            nestedType.clear()
            descriptorProto.nestedTypeList.forEach {
              nestedType += nestedType {

              }
            }
          }
          val fileDescriptorProto = fileDescriptorProto {
            name = "temp"
            messageType += modifiedDescriptorProto
          }
          val fileDescriptor = FileDescriptor.buildFrom(fileDescriptorProto, arrayOf(), true)
          val modifiedDescriptor = fileDescriptor.findMessageTypeByName(modifiedDescriptorProto.name)
          metadataMessage = DynamicMessage.parseFrom(modifiedDescriptor, metadata.metadata.value)
          reportingEventGroup = cmmsEventGroup.toEventGroup(parsedEventGroupMetadataMap)
          program = compileProgram(request.filter, metadataMessage)
        }
      } catch (e: IllegalArgumentException) {
        failGrpc(Status.INVALID_ARGUMENT) { "Filter is invalid" }
      }

      if (EventFilters.matches(reportingEventGroup, program)) {
        filteredEventGroups.add(reportingEventGroup)
      }
    }

    return listEventGroupsResponse {
      this.eventGroups += filteredEventGroups
      nextPageToken = cmmsListEventGroupResponse.nextPageToken
    }
  }

  companion object {
    private val eventGroupDeclarations: List<Decl> =
      with(EventGroup.getDefaultInstance()) {
        this.descriptorForType.fields
          .filter { fieldDescriptor ->
            fieldDescriptor.type == FieldDescriptor.Type.MESSAGE
          }
          .map { fieldDescriptor ->
            val typeName = fieldDescriptor.messageType.fullName
            Decls.newVar(
              fieldDescriptor.name,
              Decls.newObjectType(typeName),
            )
          }
      }

    private fun compileProgram(celExpr: String): Program {
      return compileProgram(celExpr, null)
    }

    /**
     * @throws IllegalArgumentException if celExpr is invalid
     */
    private fun compileProgram(celExpr: String, eventMessage: Message?): Program {
      val typeRegistry: ProtoTypeRegistry = ProtoTypeRegistry.newRegistry(EventGroup.getDefaultInstance())
      val declarations: List<Decl> =
        eventMessage?.descriptorForType?.fields?.map { fieldDescriptor ->
          val typeName = fieldDescriptor.messageType.fullName
          val defaultValue = eventMessage.getField(fieldDescriptor) as? Message
          typeRegistry.registerMessage(defaultValue)
          Decls.newVar(
            fieldDescriptor.name,
            Decls.newObjectType(typeName),
          )
        }
          ?: emptyList()
      val env = Env.newEnv(
        EnvOption.customTypeAdapter(typeRegistry),
        EnvOption.customTypeProvider(typeRegistry),
        EnvOption.declarations(eventGroupDeclarations)
        //EnvOption.declarations(eventGroupDeclarations.plus(declarations)),
      )

      val astIssuesTuple = env.compile(celExpr)
      if (astIssuesTuple.hasIssues()) {
        throw IllegalArgumentException()
      }

      return env.program(astIssuesTuple.ast)
    }
  }
}

private fun CmmsEventGroup.toEventGroup(): EventGroup {
  return this.toEventGroup(null)
}

private fun CmmsEventGroup.toEventGroup(
  parsedEventGroupMetadataMap: Map<String, CmmsEventGroup.Metadata>?,
): EventGroup {
  val source = this
  val cmmsEventGroupKey =
    grpcRequireNotNull(CmmsEventGroupKey.fromName(name)) { "Event group name is missing" }
  val measurementConsumerKey =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(measurementConsumer)) {
      "Event group measurement consumer key is missing"
    }
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
    if (parsedEventGroupMetadataMap != null) {
      val cmmsMetadata = parsedEventGroupMetadataMap.getValue(source.name)
      metadata = metadata {
        eventGroupMetadataDescriptor = cmmsMetadata.eventGroupMetadataDescriptor
        metadata = cmmsMetadata.metadata
      }
    }
  }
}
