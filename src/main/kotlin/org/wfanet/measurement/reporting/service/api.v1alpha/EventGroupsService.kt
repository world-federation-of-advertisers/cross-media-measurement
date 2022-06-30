package org.wfanet.measurement.reporting.service.api.v1alpha

import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataParser
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.service.api.v1alpha.crypto.decryptResult
import org.wfanet.measurement.reporting.service.api.v1alpha.crypto.loadPrivateKey
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.ListEventGroupsResponse
import java.nio.file.Path
import java.nio.file.Paths

class EventGroupsService(private val cmmsEventGroupsStub: EventGroupsCoroutineStub,
                         private val eventGroupsMetadataDescriptorsStub: EventGroupMetadataDescriptorsCoroutineStub,
                         private val encryptionPrivateKey: TinkPrivateKeyHandle) :
  EventGroupsCoroutineImplBase() {
    override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
      val principal = principalFromCurrentContext
      val eventGroups = cmmsEventGroupsStub.listEventGroups(cmmsListEventGroupsRequest {
        parent = request.parent
        pageSize = request.pageSize
        pageToken = request.pageToken
        filter = filter{measurementConsumers += (principal.resourceKey as MeasurementConsumerKey).measurementConsumerId}
      }).eventGroupsList
      val eventGroupMetadataDescriptors = eventGroupsMetadataDescriptorsStub.batchGetEventGroupMetadataDescriptors(
        batchGetEventGroupMetadataDescriptorsRequest {
          parent = request.parent
          names += eventGroups.map{it.name}
        }
      ).eventGroupMetadataDescriptorsList
      val metadataParser = EventGroupMetadataParser(eventGroupMetadataDescriptors)
      println("encrypted: " + eventGroups.get(0).encryptedMetadata)
      println("decrypted: " + decryptResult(eventGroups.get(0).encryptedMetadata, encryptionPrivateKey))
      val eventGroupMetadataMessages = eventGroups.map{metadataParser.convertToDynamicMessage(
        EventGroup.Metadata.parseFrom(decryptResult(it.encryptedMetadata, encryptionPrivateKey).data))
      }
      println("metadataMessages: $eventGroupMetadataMessages")
      //val filteredEventGroups = mutableListOf()

      /*for(event in eventGroups){

      }*/

      return ListEventGroupsResponse.getDefaultInstance()
    }
}