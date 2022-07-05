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

import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataParser
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.reporting.service.api.v1alpha.crypto.decryptResult
import org.wfanet.measurement.reporting.v1alpha.EventGroup as ReportingEventGroup
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.eventTemplate as reportingEventTemplate
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.metadata as reportingMetadata
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v1alpha.eventGroup as reportingEventGroup
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsResponse

class EventGroupsService(
  private val cmmsEventGroupsStub: EventGroupsCoroutineStub,
  private val eventGroupsMetadataDescriptorsStub: EventGroupMetadataDescriptorsCoroutineStub,
  // TODO(@chipingyeh): Call key retrieval service once implemented
  private val encryptionPrivateKey: TinkPrivateKeyHandle
) : EventGroupsCoroutineImplBase() {
  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    val principal = principalFromCurrentContext
    val eventGroups =
      cmmsEventGroupsStub
        .listEventGroups(
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
        .eventGroupsList
    if (request.filter.isEmpty())
      return listEventGroupsResponse {
        this.eventGroups += eventGroups.map { it.toReportingServer() }
      }
    val eventGroupMetadataDescriptors =
      eventGroupsMetadataDescriptorsStub
        .batchGetEventGroupMetadataDescriptors(
          batchGetEventGroupMetadataDescriptorsRequest {
            parent = request.parent
            names += eventGroups.map { it.name }
          }
        )
        .eventGroupMetadataDescriptorsList
    val metadataParser = EventGroupMetadataParser(eventGroupMetadataDescriptors)
    val filteredEventGroups: MutableList<CmmsEventGroup> = mutableListOf()

    for (eventGroup in eventGroups) {
      val metadata =
        CmmsEventGroup.Metadata.parseFrom(
          decryptResult(eventGroup.encryptedMetadata, encryptionPrivateKey).data
        )
      val metadataMessage = metadataParser.convertToDynamicMessage(metadata)
      val program =
        EventFilters.compileProgram(request.filter, metadataMessage!!.defaultInstanceForType)
      if (EventFilters.matches(metadataMessage, program)) {
        filteredEventGroups.add(eventGroup)
      }
    }

    return listEventGroupsResponse {
      this.eventGroups += filteredEventGroups.map { it.toReportingServer() }
    }
  }

  private fun CmmsEventGroup.toReportingServer(): ReportingEventGroup {
    val cmmsMetadata =
      CmmsEventGroup.Metadata.parseFrom(
        decryptResult(this@toReportingServer.encryptedMetadata, encryptionPrivateKey).data
      )
    return reportingEventGroup {
      name = this@toReportingServer.name
      // dataProvider =
      eventGroupReferenceId = this@toReportingServer.eventGroupReferenceId
      eventTemplates +=
        this@toReportingServer.eventTemplatesList.map { reportingEventTemplate { type = it.type } }
      metadata = reportingMetadata {
        eventGroupMetadataDescriptor = cmmsMetadata.eventGroupMetadataDescriptor
        metadata = cmmsMetadata.metadata
      }
    }
  }
}
