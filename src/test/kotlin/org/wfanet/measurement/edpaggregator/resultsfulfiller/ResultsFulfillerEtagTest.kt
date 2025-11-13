/*
 * Copyright 2025 The Cross-Media Measurement Authors
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.rpc.ErrorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.protobuf.StatusProto
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.getRequisitionRequest
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.requisitionEntry
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.getRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.edpaggregator.service.Errors as EdpErrors

class ResultsFulfillerEtagTest {

  @Test
  fun syncFulfilled_withEtagMismatch_treatedAsSuccess() {
    runBlocking {
    val dataProvider = "dataProviders/DP"
    val requisitionName = "$dataProvider/requisitions/R1"
    val rmName = "$dataProvider/requisitionMetadata/1"

    val requisition: Requisition = Requisition.newBuilder().setName(requisitionName).build()

    val grouped: GroupedRequisitions =
      GroupedRequisitions.newBuilder()
        .setModelLine("modelLines/ML")
        .setGroupId("group-1")
        .addRequisitions(requisitionEntry { this.requisition = Any.pack(requisition) })
        .build()

    val requisitionMetadataStub = mock<RequisitionMetadataServiceCoroutineStub>()
    val requisitionsStub = mock<RequisitionsCoroutineStub>()

    // list -> returns one metadata pointing to the CMMS requisition
    val listedMetadata = requisitionMetadata {
      name = rmName
      cmmsRequisition = requisitionName
      state = RequisitionMetadata.State.STORED
      etag = "v1"
    }
    whenever(
        requisitionMetadataStub.listRequisitionMetadata(any(), any())
      )
      .thenReturn(
        listRequisitionMetadataResponse { requisitionMetadata += listedMetadata }
      )

    // CMMS says already fulfilled, triggering the "sync to fulfilled" path
    whenever(requisitionsStub.getRequisition(any(), any()))
      .thenReturn(Requisition.newBuilder().setName(requisitionName).setState(Requisition.State.FULFILLED).build())

    // Refresh before signaling: latest metadata (still not fulfilled) with fresh etag v2
    val latestBeforeFulfill = requisitionMetadata {
      name = rmName
      cmmsRequisition = requisitionName
      state = RequisitionMetadata.State.PROCESSING
      etag = "v2"
    }
    val fulfilledCurrent = requisitionMetadata {
      name = rmName
      cmmsRequisition = requisitionName
      state = RequisitionMetadata.State.FULFILLED
      etag = "v3"
    }
    whenever(
        requisitionMetadataStub.getRequisitionMetadata(any(), any())
      )
      .thenReturn(latestBeforeFulfill)
      .thenReturn(fulfilledCurrent)

    // First fulfill attempt with v2 -> simulate concurrent update etag mismatch
    whenever(requisitionMetadataStub.fulfillRequisitionMetadata(any(), any()))
      .thenAnswer { throw makeEtagMismatch("v2", "v3") }

    val fulfiller =
      ResultsFulfiller(
        dataProvider = dataProvider,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        privateEncryptionKey = mock(),
        groupedRequisitions = grouped,
        modelLineInfoMap = emptyMap(),
        pipelineConfiguration = PipelineConfiguration(batchSize = 1, channelCapacity = 1, threadPoolSize = 1, workers = 1),
        impressionDataSourceProvider = mock(),
        kmsClient = null,
        impressionsStorageConfig = StorageConfig(),
        fulfillerSelector = mock(),
      )

    // Should not throw
    fulfiller.fulfillRequisitions()

    // Verify we refreshed and treated fulfilled as success
    verify(requisitionMetadataStub, atLeast(1)).getRequisitionMetadata(any(), any())
    verify(requisitionMetadataStub, atLeast(1)).fulfillRequisitionMetadata(any(), any())
    }
  }

  @Test
  fun syncRefused_withEtagMismatch_treatedAsSuccess() {
    runBlocking {
    val dataProvider = "dataProviders/DP"
    val requisitionName = "$dataProvider/requisitions/R2"
    val rmName = "$dataProvider/requisitionMetadata/2"

    val requisition: Requisition = Requisition.newBuilder().setName(requisitionName).build()
    val grouped: GroupedRequisitions =
      GroupedRequisitions.newBuilder()
        .setModelLine("modelLines/ML")
        .setGroupId("group-2")
        .addRequisitions(requisitionEntry { this.requisition = Any.pack(requisition) })
        .build()

    val requisitionMetadataStub = mock<RequisitionMetadataServiceCoroutineStub>()
    val requisitionsStub = mock<RequisitionsCoroutineStub>()

    val listedMetadata = requisitionMetadata {
      name = rmName
      cmmsRequisition = requisitionName
      state = RequisitionMetadata.State.STORED
      etag = "v1"
    }
    whenever(
        requisitionMetadataStub.listRequisitionMetadata(any(), any())
      )
      .thenReturn(
        listRequisitionMetadataResponse { requisitionMetadata += listedMetadata }
      )

    // CMMS says invalid state (not UNFULFILLED, not FULFILLED) to trigger refusal path
    whenever(requisitionsStub.getRequisition(any(), any()))
      .thenReturn(
        Requisition.newBuilder()
          .setName(requisitionName)
          .setState(Requisition.State.STATE_UNSPECIFIED)
          .build()
      )

    val latestBeforeRefuse = requisitionMetadata {
      name = rmName
      cmmsRequisition = requisitionName
      state = RequisitionMetadata.State.PROCESSING
      etag = "v2"
    }
    val refusedCurrent = requisitionMetadata {
      name = rmName
      cmmsRequisition = requisitionName
      state = RequisitionMetadata.State.REFUSED
      etag = "v3"
    }
    whenever(
        requisitionMetadataStub.getRequisitionMetadata(any(), any())
      )
      .thenReturn(latestBeforeRefuse)
      .thenReturn(refusedCurrent)

    whenever(requisitionMetadataStub.refuseRequisitionMetadata(any(), any()))
      .thenAnswer { throw makeEtagMismatch("v2", "v3") }

    val fulfiller =
      ResultsFulfiller(
        dataProvider = dataProvider,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        privateEncryptionKey = mock(),
        groupedRequisitions = grouped,
        modelLineInfoMap = emptyMap(),
        pipelineConfiguration = PipelineConfiguration(batchSize = 1, channelCapacity = 1, threadPoolSize = 1, workers = 1),
        impressionDataSourceProvider = mock(),
        kmsClient = null,
        impressionsStorageConfig = StorageConfig(),
        fulfillerSelector = mock(),
      )

    fulfiller.fulfillRequisitions()

    verify(requisitionMetadataStub, atLeast(1)).getRequisitionMetadata(any(), any())
    verify(requisitionMetadataStub, atLeast(1)).refuseRequisitionMetadata(any(), any())
    }
  }

  private fun makeEtagMismatch(requestEtag: String, actualEtag: String): StatusException {
    val errorInfo =
      ErrorInfo.newBuilder()
        .setReason(EdpErrors.Reason.ETAG_MISMATCH.name)
        .setDomain(EdpErrors.DOMAIN)
        .putMetadata(EdpErrors.Metadata.REQUEST_ETAG.key, requestEtag)
        .putMetadata(EdpErrors.Metadata.ETAG.key, actualEtag)
        .build()

    val status =
      com.google.rpc.Status.newBuilder()
        .setCode(Status.Code.FAILED_PRECONDITION.value())
        .setMessage(
          "Request etag $requestEtag does not match actual etag $actualEtag"
        )
        .addDetails(Any.pack(errorInfo))
        .build()

    return StatusProto.toStatusException(status)
  }
}
