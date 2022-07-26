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
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataParser
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.Principal
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.reporting.v1alpha.EventGroup
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.eventTemplate
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.metadata
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v1alpha.eventGroup
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsResponse

class EventGroupsService(
  private val cmmsEventGroupsStub: EventGroupsCoroutineStub,
  private val eventGroupsMetadataDescriptorsStub: EventGroupMetadataDescriptorsCoroutineStub,
  private val encryptionKeyPairStore: EncryptionKeyPairStore
) : EventGroupsCoroutineImplBase() {
  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    val principal: Principal<*> = principalFromCurrentContext
    val cmmsListEventGroupResponse =
      cmmsEventGroupsStub.listEventGroups(
        cmmsListEventGroupsRequest {
          parent = request.parent
          pageSize = request.pageSize
          pageToken = request.pageToken
          filter = filter {
            measurementConsumers +=
              (principal.resourceKey as MeasurementConsumerKey).measurementConsumerId
          }
        }
      )
    val cmmsEventGroups = cmmsListEventGroupResponse.eventGroupsList
    val parsedEventGroupMetadataMap: Map<String, CmmsEventGroup.Metadata> =
      cmmsEventGroups.associate {
        it.name to
          CmmsEventGroup.Metadata.parseFrom(
            decryptResult(
                it.encryptedMetadata,
                encryptionKeyPairStore.getPrivateKeyHandle(it.measurementConsumerPublicKey.data)
                  ?: failGrpc(Status.FAILED_PRECONDITION) {
                    "Public key does not have corresponding private key"
                  }
              )
              .data
          )
      }

    if (request.filter.isEmpty())
      return listEventGroupsResponse {
        this.eventGroups += cmmsEventGroups.map { it.toEventGroup(parsedEventGroupMetadataMap) }
        nextPageToken = cmmsListEventGroupResponse.nextPageToken
      }
    val eventGroupMetadataDescriptors: List<EventGroupMetadataDescriptor> =
      eventGroupsMetadataDescriptorsStub
        .batchGetEventGroupMetadataDescriptors(
          batchGetEventGroupMetadataDescriptorsRequest {
            parent = request.parent
            names +=
              parsedEventGroupMetadataMap.values.map { it.eventGroupMetadataDescriptor }.toSet()
          }
        )
        .eventGroupMetadataDescriptorsList
    val metadataParser = EventGroupMetadataParser(eventGroupMetadataDescriptors)
    val filteredEventGroups: MutableList<CmmsEventGroup> = mutableListOf()

    for (cmmsEventGroup in cmmsEventGroups) {
      val metadata = parsedEventGroupMetadataMap.getValue(cmmsEventGroup.name)
      val metadataMessage =
        metadataParser.convertToDynamicMessage(metadata)
          ?: failGrpc(Status.FAILED_PRECONDITION) {
            "Event group metadata message descriptor not found"
          }
      val program: Program =
        EventFilters.compileProgram(request.filter, metadataMessage.defaultInstanceForType)
      if (EventFilters.matches(metadataMessage, program)) {
        filteredEventGroups.add(cmmsEventGroup)
      }
    }

    return listEventGroupsResponse {
      this.eventGroups += filteredEventGroups.map { it.toEventGroup(parsedEventGroupMetadataMap) }
      nextPageToken = cmmsListEventGroupResponse.nextPageToken
    }
  }
}

private fun CmmsEventGroup.toEventGroup(
  parsedEventGroupMetadataMap: Map<String, CmmsEventGroup.Metadata>
): EventGroup {
  val source = this
  val cmmsMetadata = parsedEventGroupMetadataMap.getValue(name)
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
    dataProvider = DataProviderKey(cmmsEventGroupKey.dataProviderId).toName()
    eventGroupReferenceId = source.eventGroupReferenceId
    eventTemplates += source.eventTemplatesList.map { eventTemplate { type = it.type } }
    metadata = metadata {
      eventGroupMetadataDescriptor = cmmsMetadata.eventGroupMetadataDescriptor
      metadata = cmmsMetadata.metadata
    }
  }
}
