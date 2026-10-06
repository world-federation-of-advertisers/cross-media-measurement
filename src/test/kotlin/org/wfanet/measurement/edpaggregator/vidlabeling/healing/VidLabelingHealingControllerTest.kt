/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeling.healing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.type.date
import io.grpc.Status
import java.security.MessageDigest
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.acquireRawImpressionUploadEvictionFenceResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.labeledOutputManifest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItemPublisher
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcherHttpException

@RunWith(JUnit4::class)
class VidLabelingHealingControllerTest {
  private val candidatesService =
    mockService<
      RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase
    >()
  private val operationsService =
    mockService<
      UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase
    >()
  private val uploadsService =
    mockService<RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase>()
  private val filesService =
    mockService<
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
    >()
  private val modelLinesService =
    mockService<
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
    >()
  private val rankIndexBlobsService =
    mockService<RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase>()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(candidatesService)
    addService(operationsService)
    addService(uploadsService)
    addService(filesService)
    addService(modelLinesService)
    addService(rankIndexBlobsService)
  }

  @Test
  fun `run purges expired candidates after processing a DataProvider`() = runBlocking {
    whenever(candidatesService.listRawImpressionUploadCorrectionCandidates(any()))
      .thenReturn(ListRawImpressionUploadCorrectionCandidatesResponse.getDefaultInstance())
    whenever(operationsService.listUploadHealingOperations(any()))
      .thenReturn(ListUploadHealingOperationsResponse.getDefaultInstance())
    val purgedDataProviders = mutableListOf<String>()

    val result =
      newController(candidateCleaner = CorrectionCandidateCleaner { purgedDataProviders += it })
        .run()

    assertThat(result.processedDataProviders).isEqualTo(1)
    assertThat(result.failedDataProviders).isEqualTo(0)
    assertThat(purgedDataProviders).containsExactly(DATA_PROVIDER)
    Unit
  }

  @Test
  fun `run combines pending candidates into one draft plan`() = runBlocking {
    whenever(candidatesService.listRawImpressionUploadCorrectionCandidates(any()))
      .thenReturn(
        listRawImpressionUploadCorrectionCandidatesResponse {
          rawImpressionUploadCorrectionCandidates += CANDIDATE
          nextPageToken = "next"
        },
        ListRawImpressionUploadCorrectionCandidatesResponse.getDefaultInstance(),
      )
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE)
    whenever(operationsService.listUploadHealingOperations(any()))
      .thenReturn(ListUploadHealingOperationsResponse.getDefaultInstance())
    whenever(uploadsService.getRawImpressionUpload(any())).thenAnswer { invocation ->
      when (
        invocation
          .getArgument<org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest>(
            0
          )
          .name
      ) {
        CANDIDATE_UPLOAD_NAME -> CANDIDATE_UPLOAD
        SOURCE_UPLOAD_NAME -> SOURCE_UPLOAD
        else -> error("unexpected upload")
      }
    }
    whenever(uploadsService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads += listOf(SOURCE_UPLOAD, CANDIDATE_UPLOAD)
        }
      )
    var reconciled = UploadHealingOperation.getDefaultInstance()
    whenever(operationsService.reconcileUploadHealingOperation(any())).thenAnswer { invocation ->
      reconciled =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ReconcileUploadHealingOperationRequest
          >(
            0
          )
          .uploadHealingOperation
      reconciled
    }

    newController().run()

    assertThat(reconciled.rawImpressionUploadCorrectionCandidatesList)
      .containsExactly(CANDIDATE_NAME)
    assertThat(reconciled.badRawImpressionUploadsList).containsExactly(SOURCE_UPLOAD_NAME)
    assertThat(reconciled.state).isEqualTo(UploadHealingOperation.State.STATE_UNSPECIFIED)
    Unit
  }

  @Test
  fun `non-transient planning failure persists an inspectable plan`() = runBlocking {
    whenever(candidatesService.listRawImpressionUploadCorrectionCandidates(any()))
      .thenReturn(
        listRawImpressionUploadCorrectionCandidatesResponse {
          rawImpressionUploadCorrectionCandidates += CANDIDATE
        }
      )
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenThrow(Status.NOT_FOUND.asRuntimeException())
    whenever(operationsService.listUploadHealingOperations(any()))
      .thenReturn(ListUploadHealingOperationsResponse.getDefaultInstance())
    var plan = UploadHealingOperation.getDefaultInstance()
    whenever(operationsService.reconcileUploadHealingOperation(any())).thenAnswer { invocation ->
      plan =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ReconcileUploadHealingOperationRequest
          >(
            0
          )
          .uploadHealingOperation
      plan.copy { state = UploadHealingOperation.State.NEEDS_ATTENTION }
    }
    val events = mutableListOf<VidLabelingHealingControllerEventSink.Event>()

    newController(eventSink = VidLabelingHealingControllerEventSink(events::add)).run()

    assertThat(plan.stepsList).isEmpty()
    assertThat(plan.rawImpressionUploadCorrectionCandidatesList).containsExactly(CANDIDATE_NAME)
    assertThat(events.map { it.type })
      .contains(VidLabelingHealingControllerEventSink.Event.Type.NEEDS_ATTENTION)
  }

  @Test
  fun `run waits in draining while an availability lease is active`() = runBlocking {
    var operation = APPROVED_OPERATION
    whenever(candidatesService.listRawImpressionUploadCorrectionCandidates(any()))
      .thenReturn(ListRawImpressionUploadCorrectionCandidatesResponse.getDefaultInstance())
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val requestedStates =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in requestedStates) uploadHealingOperations += operation
      }
    }
    whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer { invocation ->
      operation =
        operation.copy {
          state =
            invocation
              .getArgument<
                org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
              >(
                0
              )
              .state
          etag = "etag-${state.number}"
        }
      operation
    }
    whenever(uploadsService.acquireRawImpressionUploadEvictionFence(any()))
      .thenReturn(
        acquireRawImpressionUploadEvictionFenceResponse {
          newlyAcquired = false
          state =
            org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingEvictionFenceState
              .VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
        }
      )
    var advanceCalls = 0
    whenever(uploadsService.advanceRawImpressionUploadEvictionFence(any())).thenAnswer {
      advanceCalls++
      if (advanceCalls == 2) {
        throw Status.FAILED_PRECONDITION.withDescription("active synchronization lease")
          .asRuntimeException()
      }
      org.wfanet.measurement.edpaggregator.v1alpha.AdvanceRawImpressionUploadEvictionFenceResponse
        .getDefaultInstance()
    }
    val evictionExecutor = org.mockito.kotlin.mock<EvictionExecutor>()

    newController(evictionExecutor = evictionExecutor).run()

    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.DRAINING)
    verify(evictionExecutor, never()).evict(any(), any(), any())
    Unit
  }

  @Test
  fun `run resumes after fence reaches evicting before manifest checkpoint`() = runBlocking {
    val operation = APPROVED_OPERATION.copy { state = UploadHealingOperation.State.DRAINING }
    whenever(candidatesService.listRawImpressionUploadCorrectionCandidates(any()))
      .thenReturn(ListRawImpressionUploadCorrectionCandidatesResponse.getDefaultInstance())
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED })
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(uploadsService.acquireRawImpressionUploadEvictionFence(any()))
      .thenReturn(
        acquireRawImpressionUploadEvictionFenceResponse {
          state =
            org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingEvictionFenceState
              .VID_LABELING_EVICTION_FENCE_STATE_EVICTING
          etag = "evicting-fence-etag"
        }
      )
    val evictionExecutor = org.mockito.kotlin.mock<EvictionExecutor>()
    whenever(evictionExecutor.finalizeOutputManifests(any())).thenAnswer { invocation ->
      val plan = invocation.getArgument<EvictUploader.EvictionPlan>(0)
      plan.copy(
        cascade =
          plan.cascade.map { entry ->
            entry.copy(
              labeledOutputManifest =
                labeledOutputManifest {
                  blobs +=
                    LabeledOutputManifestKt.blobVersion {
                      blobUri = entry.labeledOutputManifest.blobsList.single().blobUri
                      generation = 7L
                    }
                }
            )
          },
        outputManifestsFinalized = true,
      )
    }
    whenever(operationsService.finalizeUploadHealingOperation(any()))
      .thenThrow(Status.UNAVAILABLE.asRuntimeException())

    newController(evictionExecutor = evictionExecutor).run()

    verify(operationsService).finalizeUploadHealingOperation(any())
    verify(evictionExecutor, never()).evict(any(), any(), any())
    Unit
  }

  @Test
  fun `later healing waits before fencing a scheduled availability handoff`() = runBlocking {
    val operation = APPROVED_OPERATION.copy { state = UploadHealingOperation.State.DRAINING }
    val completedStep =
      EVICTING_OPERATION.stepsList.single().copy {
        state = UploadHealingStep.State.COMPLETE
        replacementRawImpressionUpload = CANDIDATE_UPLOAD_NAME
      }
    val completedOperation =
      EVICTING_OPERATION.copy {
        name = "$DATA_PROVIDER/uploadHealingOperations/completed"
        state = UploadHealingOperation.State.COMPLETE
        steps[0] = completedStep
      }
    whenever(candidatesService.listRawImpressionUploadCorrectionCandidates(any()))
      .thenReturn(ListRawImpressionUploadCorrectionCandidatesResponse.getDefaultInstance())
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED })
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val requestedStates =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in requestedStates) uploadHealingOperations += operation
        if (completedOperation.state in requestedStates) {
          uploadHealingOperations += completedOperation
        }
      }
    }
    whenever(modelLinesService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            state = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING
            pendingAvailabilityDates += date {
              year = 2026
              month = 11
              day = 1
            }
          }
        }
      )

    newController().run()

    verify(uploadsService, never()).acquireRawImpressionUploadEvictionFence(any())
    Unit
  }

  @Test
  fun `manifest mismatch moves operation to needs attention before eviction`() = runBlocking {
    var operation = EVICTING_OPERATION
    whenever(candidatesService.listRawImpressionUploadCorrectionCandidates(any()))
      .thenReturn(ListRawImpressionUploadCorrectionCandidatesResponse.getDefaultInstance())
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE)
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val requestedStates =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in requestedStates) uploadHealingOperations += operation
      }
    }
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer { invocation ->
      operation =
        operation.copy {
          state =
            invocation
              .getArgument<
                org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
              >(
                0
              )
              .state
          etag = "attention-etag"
        }
      operation
    }
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(CANDIDATE_UPLOAD)
    whenever(filesService.listRawImpressionUploadFiles(any()))
      .thenReturn(listRawImpressionUploadFilesResponse {})
    val evictionExecutor = org.mockito.kotlin.mock<EvictionExecutor>()
    val events = mutableListOf<VidLabelingHealingControllerEventSink.Event>()

    newController(
        evictionExecutor = evictionExecutor,
        manifestReader =
          CorrectionManifestReader { _, _ ->
            listOf(RawImpressionUploadManifestClassifier.File("gs://raw/changed", 9L))
          },
        eventSink = VidLabelingHealingControllerEventSink(events::add),
      )
      .run()

    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.NEEDS_ATTENTION)
    assertThat(events.map { it.type })
      .contains(VidLabelingHealingControllerEventSink.Event.Type.MANIFEST_MISMATCH)
    verify(evictionExecutor, never()).evict(any(), any(), any())
    Unit
  }

  @Test
  fun `run evicts then replays the approved exact candidate generation`() = runBlocking {
    var operation = EVICTING_OPERATION
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val requestedStates =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in requestedStates) uploadHealingOperations += operation
      }
    }
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer { invocation ->
      operation =
        operation.copy {
          state =
            invocation
              .getArgument<
                org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
              >(
                0
              )
              .state
          etag = "operation-${state.number}"
        }
      operation
    }
    whenever(operationsService.advanceUploadHealingStep(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
        >(
          0
        )
      val state =
        when (request.action) {
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest.Action
            .CONFIRM_EVICTION -> UploadHealingStep.State.WAITING_FOR_REPLACEMENT
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest.Action
            .RECORD_RECOVERY -> UploadHealingStep.State.RECOVERY_STARTED
          else -> error("unexpected action")
        }
      val updatedStep =
        operation.stepsList.single().copy {
          this.state = state
          recoveryDoneBlobGeneration = request.recoveryDoneBlobGeneration
          etag = "step-${state.number}"
        }
      operation =
        operation.copy {
          steps[0] = updatedStep
          etag = "operation-${state.number}"
        }
      updatedStep
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(
        CANDIDATE.copy {
          state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED
          decision = RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
        }
      )
    whenever(uploadsService.getRawImpressionUpload(any())).thenAnswer { invocation ->
      when (
        invocation
          .getArgument<org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest>(
            0
          )
          .name
      ) {
        CANDIDATE_UPLOAD_NAME -> CANDIDATE_UPLOAD
        SOURCE_UPLOAD_NAME -> SOURCE_UPLOAD
        else -> error("unexpected upload")
      }
    }
    whenever(filesService.listRawImpressionUploadFiles(any()))
      .thenReturn(listRawImpressionUploadFilesResponse {})
    whenever(uploadsService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += SOURCE_UPLOAD })
    val evictionExecutor = testEvictionExecutor { plan, _, checkpoint ->
      plan.cascade.forEach { checkpoint(it) }
      EvictUploader.EvictionResult(emptyList(), 0, 0, 0)
    }
    var replay: DoneBlobReplayer.Request? = null

    newController(
        evictionExecutor = evictionExecutor,
        doneBlobReplayer = DoneBlobReplayer { replay = it },
      )
      .run()

    assertThat(replay!!.doneBlobUri).isEqualTo(CANDIDATE_UPLOAD.doneBlobUri)
    assertThat(replay!!.doneBlobGeneration).isEqualTo(CANDIDATE_UPLOAD.doneBlobGeneration)
    assertThat(replay!!.sourceRawImpressionUpload).isEqualTo(SOURCE_UPLOAD_NAME)
    assertThat(operation.stepsList.single().state)
      .isEqualTo(UploadHealingStep.State.RECOVERY_STARTED)
    Unit
  }

  @Test
  fun `completed recovery wins a done-object timestamp tie with its candidate`() = runBlocking {
    val sharedDoneTime = timestamp { seconds = 150L }
    val candidateUpload =
      CANDIDATE_UPLOAD.copy {
        doneBlobCreateTime = sharedDoneTime
        createTime = timestamp { seconds = 200L }
        registrationComplete = true
      }
    val recoveryName = "$DATA_PROVIDER/rawImpressionUploads/recovery"
    val recoveryUpload =
      candidateUpload.copy {
        name = recoveryName
        createTime = timestamp { seconds = 201L }
        state = RawImpressionUpload.State.COMPLETED
        uploadHealingOperation = OPERATION_NAME
      }
    val recoveryStartedStep =
      EVICTING_OPERATION.stepsList.single().copy {
        state = UploadHealingStep.State.RECOVERY_STARTED
        recoveryDoneBlobGeneration = recoveryUpload.doneBlobGeneration
      }
    var operation =
      EVICTING_OPERATION.copy {
        state = UploadHealingOperation.State.REPLAYING
        steps[0] = recoveryStartedStep
      }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    whenever(operationsService.advanceUploadHealingStep(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
        >(
          0
        )
      val completedStep =
        operation.stepsList.single().copy {
          state = UploadHealingStep.State.COMPLETE
          replacementRawImpressionUpload = request.replacementRawImpressionUpload
        }
      operation =
        operation.copy {
          state = UploadHealingOperation.State.COMPLETE
          steps[0] = completedStep
          etag = "complete"
        }
      completedStep
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED })
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(SOURCE_UPLOAD)
    whenever(uploadsService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads += listOf(candidateUpload, recoveryUpload)
        }
      )
    whenever(modelLinesService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = "$recoveryName/rawImpressionUploadModelLines/ml"
            cmmsModelLine = CMMS_MODEL_LINE
            state = RawImpressionUploadModelLine.State.COMPLETED
          }
        }
      )

    newController().run()

    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(operation.stepsList.single().replacementRawImpressionUpload).isEqualTo(recoveryName)
  }

  @Test
  fun `no-replacement completes eviction without replay`() = runBlocking {
    var operation =
      EVICTING_OPERATION.copy {
        steps.clear()
        steps +=
          EVICTING_OPERATION.stepsList.single().copy {
            recoveryAction =
              RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT
            recoveryTarget = false
          }
      }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(
        CANDIDATE.copy {
          state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED
          decision =
            RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
        }
      )
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(CANDIDATE_UPLOAD)
    whenever(filesService.listRawImpressionUploadFiles(any()))
      .thenReturn(listRawImpressionUploadFilesResponse {})
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    whenever(operationsService.advanceUploadHealingStep(any())).thenAnswer {
      val completedStep =
        operation.stepsList.single().copy { state = UploadHealingStep.State.COMPLETE }
      operation =
        operation.copy {
          state = UploadHealingOperation.State.COMPLETE
          steps[0] = completedStep
          etag = "complete"
        }
      completedStep
    }
    val evictionExecutor = testEvictionExecutor { plan, _, checkpoint ->
      plan.cascade.forEach { checkpoint(it) }
      EvictUploader.EvictionResult(emptyList(), 0, 0, 0)
    }
    val replayer = org.mockito.kotlin.mock<DoneBlobReplayer>()

    newController(evictionExecutor = evictionExecutor, doneBlobReplayer = replayer).run()

    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
    verify(replayer, never()).replay(any())
    Unit
  }

  @Test
  fun `run reopens a draining plan whose candidate was superseded`() = runBlocking {
    var operation = APPROVED_OPERATION.copy { state = UploadHealingOperation.State.DRAINING }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(
        CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.SUPERSEDED }
      )
    whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer { invocation ->
      operation =
        operation.copy {
          state =
            invocation
              .getArgument<
                org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
              >(
                0
              )
              .state
          etag = "reopened"
        }
      operation
    }
    val evictionExecutor = org.mockito.kotlin.mock<EvictionExecutor>()

    newController(evictionExecutor = evictionExecutor).run()

    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.APPROVAL_REQUIRED)
    verify(evictionExecutor, never()).evict(any(), any(), any())
    Unit
  }

  @Test
  fun `run leaves transient failures resumable`() = runBlocking {
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (EVICTING_OPERATION.state in states) {
          uploadHealingOperations += EVICTING_OPERATION
        }
      }
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenThrow(Status.UNAVAILABLE.asRuntimeException())

    val result = newController().run()

    assertThat(result.failedDataProviders).isEqualTo(0)
    verify(operationsService, never()).advanceUploadHealingOperation(any())
    Unit
  }

  @Test
  fun `run moves a permanent HTTP failure to needs attention`() = runBlocking {
    var operation = EVICTING_OPERATION
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenThrow(DataWatcherHttpException(400, "bad dispatcher request"))
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer { invocation ->
      val previousState = operation.state
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
        >(
          0
        )
      operation =
        operation.copy {
          state = request.state
          resumeState = previousState
        }
      operation
    }

    newController().run()

    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.NEEDS_ATTENTION)
    assertThat(operation.resumeState).isEqualTo(UploadHealingOperation.State.EVICTING)
  }

  @Test
  fun `run reports a stalled operation only after its timeout`() = runBlocking {
    val operation =
      APPROVED_OPERATION.copy {
        state = UploadHealingOperation.State.RECOVERING
        steps.clear()
        etag = "stable"
        updateTime = timestamp { seconds = 1L }
      }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED })
    val events = mutableListOf<VidLabelingHealingControllerEventSink.Event>()

    newController(
        eventSink = VidLabelingHealingControllerEventSink(events::add),
        stallTimeout = Duration.ofSeconds(1),
      )
      .run()

    assertThat(events.map { it.type })
      .contains(VidLabelingHealingControllerEventSink.Event.Type.STALLED)
  }

  @Test
  fun `operator recovery checkpoints a fresh done generation before replay`() = runBlocking {
    val recoveryStep =
      APPROVED_OPERATION.stepsList.single().copy {
        recoveryAction =
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        state = UploadHealingStep.State.WAITING_FOR_REPLACEMENT
        rawImpressionUploadCorrectionCandidate = ""
      }
    var operation =
      APPROVED_OPERATION.copy {
        state = UploadHealingOperation.State.RECOVERING
        rawImpressionUploadCorrectionCandidates.clear()
        steps[0] = recoveryStep
        etag = "recovering-etag"
      }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    val failedSource = SOURCE_UPLOAD.copy { state = RawImpressionUpload.State.FAILED }
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(failedSource)
    whenever(uploadsService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += failedSource })
    whenever(filesService.listRawImpressionUploadFiles(any()))
      .thenReturn(listRawImpressionUploadFilesResponse {})
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    val replayed = mutableListOf<DoneBlobReplayer.Request>()
    whenever(operationsService.advanceUploadHealingStep(any())).thenAnswer { invocation ->
      check(replayed.isEmpty())
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
        >(
          0
        )
      val updated =
        operation.stepsList.single().copy {
          state = UploadHealingStep.State.RECOVERY_STARTED
          recoveryDoneBlobGeneration = request.recoveryDoneBlobGeneration
          etag = "recovery-started-etag"
        }
      operation =
        operation.copy {
          steps[0] = updated
          etag = "recovery-started-operation-etag"
        }
      updated
    }
    var recoveredSource = ""
    var recoveredModelLines = emptyList<String>()
    val freshGeneration = SOURCE_UPLOAD.doneBlobGeneration + 1L
    val recoveryExecutor = RecoveryExecutor { source, modelLines ->
      recoveredSource = source
      recoveredModelLines = modelLines
      RecoverUploader.Result(source, SOURCE_UPLOAD.doneBlobUri, freshGeneration, modelLines)
    }
    val replayer = DoneBlobReplayer { replayed += it }
    var verifiedGeneration = 0L
    val manifestReader = CorrectionManifestReader { _, generation ->
      verifiedGeneration = generation
      emptyList()
    }

    newController(
        manifestReader = manifestReader,
        doneBlobReplayer = replayer,
        recoveryExecutor = recoveryExecutor,
      )
      .run()

    assertThat(recoveredSource).isEqualTo(SOURCE_UPLOAD_NAME)
    assertThat(recoveredModelLines).containsExactly(CMMS_MODEL_LINE)
    assertThat(verifiedGeneration).isEqualTo(freshGeneration)
    assertThat(operation.stepsList.single().recoveryDoneBlobGeneration).isEqualTo(freshGeneration)
    assertThat(operation.stepsList.single().recoveryDoneBlobGeneration)
      .isNotEqualTo(SOURCE_UPLOAD.doneBlobGeneration)
    assertThat(replayed).isNotEmpty()
    assertThat(replayed.map { it.doneBlobUri }.distinct())
      .containsExactly(SOURCE_UPLOAD.doneBlobUri)
    assertThat(replayed.map { it.doneBlobGeneration }.distinct()).containsExactly(freshGeneration)
    assertThat(replayed.map { it.sourceRawImpressionUpload }.distinct())
      .containsExactly(SOURCE_UPLOAD_NAME)
    assertThat(replayed.flatMap { it.cmmsModelLines }.distinct()).containsExactly(CMMS_MODEL_LINE)
    assertThat(replayed.map { it.uploadHealingOperation }.distinct())
      .containsExactly(OPERATION_NAME)
    Unit
  }

  @Test
  fun `operator recovery retries HTTP 429 from its persisted generation`() = runBlocking {
    val recoveryStep =
      APPROVED_OPERATION.stepsList.single().copy {
        recoveryAction =
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        state = UploadHealingStep.State.WAITING_FOR_REPLACEMENT
        rawImpressionUploadCorrectionCandidate = ""
      }
    var operation =
      APPROVED_OPERATION.copy {
        state = UploadHealingOperation.State.RECOVERING
        rawImpressionUploadCorrectionCandidates.clear()
        steps[0] = recoveryStep
        etag = "recovering-etag"
      }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    val failedSource = SOURCE_UPLOAD.copy { state = RawImpressionUpload.State.FAILED }
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(failedSource)
    whenever(uploadsService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += failedSource })
    whenever(filesService.listRawImpressionUploadFiles(any()))
      .thenReturn(listRawImpressionUploadFilesResponse {})
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    var checkpoints = 0
    whenever(operationsService.advanceUploadHealingStep(any())).thenAnswer { invocation ->
      checkpoints++
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
        >(
          0
        )
      val updated =
        operation.stepsList.single().copy {
          state = UploadHealingStep.State.RECOVERY_STARTED
          recoveryDoneBlobGeneration = request.recoveryDoneBlobGeneration
          etag = "recovery-started-etag"
        }
      operation =
        operation.copy {
          steps[0] = updated
          etag = "recovery-started-operation-etag"
        }
      updated
    }
    val freshGeneration = SOURCE_UPLOAD.doneBlobGeneration + 1L
    var recoveryAttempts = 0
    val recoveryExecutor = RecoveryExecutor { source, modelLines ->
      recoveryAttempts++
      RecoverUploader.Result(source, SOURCE_UPLOAD.doneBlobUri, freshGeneration, modelLines)
    }
    var replayAttempts = 0
    val replayer = DoneBlobReplayer {
      replayAttempts++
      if (replayAttempts == 1) throw DataWatcherHttpException(429, "dispatcher throttled")
    }
    val controller = newController(doneBlobReplayer = replayer, recoveryExecutor = recoveryExecutor)

    controller.run()

    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.RECOVERING)
    assertThat(operation.stepsList.single().state)
      .isEqualTo(UploadHealingStep.State.RECOVERY_STARTED)
    assertThat(operation.stepsList.single().recoveryDoneBlobGeneration).isEqualTo(freshGeneration)
    assertThat(checkpoints).isEqualTo(1)

    controller.run()

    assertThat(recoveryAttempts).isEqualTo(1)
    assertThat(replayAttempts).isAtLeast(2)
    assertThat(checkpoints).isEqualTo(1)
    assertThat(operation.stepsList.single().state)
      .isEqualTo(UploadHealingStep.State.RECOVERY_STARTED)
    assertThat(operation.stepsList.single().recoveryDoneBlobGeneration).isEqualTo(freshGeneration)
  }

  @Test
  fun `operator recovery retry replays its persisted generation after HTTP 500`() = runBlocking {
    val recoveryStep =
      APPROVED_OPERATION.stepsList.single().copy {
        recoveryAction =
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        state = UploadHealingStep.State.WAITING_FOR_REPLACEMENT
        rawImpressionUploadCorrectionCandidate = ""
      }
    var operation =
      APPROVED_OPERATION.copy {
        state = UploadHealingOperation.State.RECOVERING
        rawImpressionUploadCorrectionCandidates.clear()
        steps[0] = recoveryStep
        etag = "recovering-etag"
      }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    val failedSource = SOURCE_UPLOAD.copy { state = RawImpressionUpload.State.FAILED }
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(failedSource)
    whenever(uploadsService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += failedSource })
    whenever(filesService.listRawImpressionUploadFiles(any()))
      .thenReturn(listRawImpressionUploadFilesResponse {})
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    var checkpoints = 0
    whenever(operationsService.advanceUploadHealingStep(any())).thenAnswer { invocation ->
      checkpoints++
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
        >(
          0
        )
      val updated =
        operation.stepsList.single().copy {
          state = UploadHealingStep.State.RECOVERY_STARTED
          recoveryDoneBlobGeneration = request.recoveryDoneBlobGeneration
          etag = "recovery-started-etag"
        }
      operation =
        operation.copy {
          steps[0] = updated
          etag = "recovery-started-operation-etag"
        }
      updated
    }
    whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer { invocation ->
      val previousState = operation.state
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
        >(
          0
        )
      operation =
        operation.copy {
          state = request.state
          resumeState = previousState
          etag = "needs-attention-etag"
        }
      operation
    }
    val freshGeneration = SOURCE_UPLOAD.doneBlobGeneration + 1L
    var recoveryAttempts = 0
    val recoveryExecutor = RecoveryExecutor { source, modelLines ->
      recoveryAttempts++
      RecoverUploader.Result(source, SOURCE_UPLOAD.doneBlobUri, freshGeneration, modelLines)
    }
    val replayedGenerations = mutableListOf<Long>()
    val replayer = DoneBlobReplayer { request ->
      replayedGenerations += request.doneBlobGeneration
      if (replayedGenerations.size == 1) {
        throw DataWatcherHttpException(500, "dispatcher exhausted heap")
      }
    }
    val controller = newController(doneBlobReplayer = replayer, recoveryExecutor = recoveryExecutor)

    controller.run()

    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.NEEDS_ATTENTION)
    assertThat(operation.resumeState).isEqualTo(UploadHealingOperation.State.RECOVERING)
    assertThat(operation.stepsList.single().state)
      .isEqualTo(UploadHealingStep.State.RECOVERY_STARTED)
    assertThat(operation.stepsList.single().recoveryDoneBlobGeneration).isEqualTo(freshGeneration)
    operation =
      operation.copy {
        state = UploadHealingOperation.State.RECOVERING
        resumeState = UploadHealingOperation.State.STATE_UNSPECIFIED
        etag = "retried-etag"
      }

    controller.run()

    assertThat(recoveryAttempts).isEqualTo(1)
    assertThat(checkpoints).isEqualTo(1)
    assertThat(replayedGenerations).containsExactly(freshGeneration, freshGeneration).inOrder()
    Unit
  }

  @Test
  fun `restart completes a partially recorded recovery checkpoint`() = runBlocking {
    val freshGeneration = SOURCE_UPLOAD.doneBlobGeneration + 1L
    val firstStep =
      APPROVED_OPERATION.stepsList.single().copy {
        recoveryAction =
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        state = UploadHealingStep.State.RECOVERY_STARTED
        recoveryDoneBlobGeneration = freshGeneration
      }
    val secondStep =
      firstStep.copy {
        name = "$OPERATION_NAME/uploadHealingSteps/2"
        rawImpressionUploadModelLine = "$SOURCE_UPLOAD_NAME/rawImpressionUploadModelLines/second"
        state = UploadHealingStep.State.WAITING_FOR_REPLACEMENT
        recoveryDoneBlobGeneration = 0L
        etag = "second-etag"
      }
    var operation =
      APPROVED_OPERATION.copy {
        state = UploadHealingOperation.State.RECOVERING
        steps.clear()
        steps += listOf(firstStep, secondStep)
        etag = "recovering-etag"
      }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED })
    val failedSource = SOURCE_UPLOAD.copy { state = RawImpressionUpload.State.FAILED }
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(failedSource)
    whenever(uploadsService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += failedSource })
    whenever(modelLinesService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines +=
            listOf(firstStep, secondStep).map { step ->
              rawImpressionUploadModelLine {
                name = step.rawImpressionUploadModelLine
                cmmsModelLine = step.cmmsModelLine
                state = RawImpressionUploadModelLine.State.FAILED
                failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
              }
            }
        }
      )
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    val replayed = mutableListOf<DoneBlobReplayer.Request>()
    whenever(operationsService.advanceUploadHealingStep(any())).thenAnswer { invocation ->
      check(replayed.isEmpty())
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
        >(
          0
        )
      val updated =
        operation.stepsList
          .single { it.name == request.name }
          .copy {
            state = UploadHealingStep.State.RECOVERY_STARTED
            recoveryDoneBlobGeneration = request.recoveryDoneBlobGeneration
          }
      operation =
        operation.copy {
          steps.clear()
          steps += operation.stepsList.map { if (it.name == updated.name) updated else it }
          etag = "checkpointed"
        }
      updated
    }
    val replayer = DoneBlobReplayer { replayed += it }

    newController(doneBlobReplayer = replayer).run()

    assertThat(operation.stepsList.map { it.state }.distinct())
      .containsExactly(UploadHealingStep.State.RECOVERY_STARTED)
    assertThat(operation.stepsList.map { it.recoveryDoneBlobGeneration }.distinct())
      .containsExactly(freshGeneration)
    assertThat(replayed).isNotEmpty()
    assertThat(replayed.map { it.doneBlobUri }.distinct())
      .containsExactly(SOURCE_UPLOAD.doneBlobUri)
    assertThat(replayed.map { it.doneBlobGeneration }.distinct()).containsExactly(freshGeneration)
    assertThat(replayed.map { it.sourceRawImpressionUpload }.distinct())
      .containsExactly(SOURCE_UPLOAD_NAME)
    Unit
  }

  @Test
  fun `availability-syncing replacement schedules handoff before healing completes`() =
    runBlocking {
      val recoveryStep =
        EVICTING_OPERATION.stepsList.single().copy {
          state = UploadHealingStep.State.RECOVERY_STARTED
          recoveryDoneBlobGeneration = CANDIDATE_UPLOAD.doneBlobGeneration
        }
      var operation =
        EVICTING_OPERATION.copy {
          state = UploadHealingOperation.State.REPLAYING
          steps[0] = recoveryStep
        }
      val replacement =
        CANDIDATE_UPLOAD.copy {
          registrationComplete = true
          state = RawImpressionUpload.State.ACTIVE
          uploadHealingOperation = OPERATION_NAME
        }
      val createRequests = mutableListOf<CreateWorkItemRequest>()
      val publisher =
        DataAvailabilitySyncWorkItemPublisher(
          createWorkItem = { _, request ->
            createRequests += request
            request.workItem
              .toBuilder()
              .setName("workItems/${request.workItemId}")
              .setState(WorkItem.State.QUEUED)
              .setGeneration(1)
              .build()
          },
          getWorkItem = { _, _ -> error("unexpected get") },
        )
      whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
        val states =
          invocation
            .getArgument<
              org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
            >(
              0
            )
            .filter
            .stateInList
        listUploadHealingOperationsResponse {
          if (operation.state in states) uploadHealingOperations += operation
        }
      }
      whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
      whenever(operationsService.advanceUploadHealingStep(any())).thenAnswer { invocation ->
        val request =
          invocation.getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
          >(
            0
          )
        check(
          request.action ==
            org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest.Action
              .CONFIRM_REPLACEMENT
        )
        val completed =
          operation.stepsList.single().copy {
            state = UploadHealingStep.State.COMPLETE
            replacementRawImpressionUpload = request.replacementRawImpressionUpload
          }
        operation =
          operation.copy {
            state = UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING
            steps[0] = completed
            etag = "handoff-pending"
          }
        completed
      }
      whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer { invocation ->
        val request =
          invocation.getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
          >(
            0
          )
        check(request.state == UploadHealingOperation.State.COMPLETE)
        operation = operation.copy { state = UploadHealingOperation.State.COMPLETE }
        operation
      }
      whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
        .thenReturn(
          CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED }
        )
      whenever(uploadsService.getRawImpressionUpload(any())).thenAnswer { invocation ->
        when (
          invocation
            .getArgument<
              org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
            >(
              0
            )
            .name
        ) {
          SOURCE_UPLOAD_NAME -> SOURCE_UPLOAD
          CANDIDATE_UPLOAD_NAME -> replacement
          else -> error("unexpected upload")
        }
      }
      whenever(uploadsService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            rawImpressionUploads += listOf(SOURCE_UPLOAD, replacement)
          }
        )
      whenever(modelLinesService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = "$CANDIDATE_UPLOAD_NAME/rawImpressionUploadModelLines/ml"
              cmmsModelLine = CMMS_MODEL_LINE
              state = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING
              pendingAvailabilityDates += date {
                year = 2026
                month = 11
                day = 1
              }
            }
          }
        )

      newController(
          dataAvailabilitySyncWorkItemPublisher = publisher,
          doneObjectMetadataReader =
            DoneObjectMetadataReader {
              DoneObjectMetadata(123L, Instant.parse("2026-11-01T00:00:00Z"))
            },
        )
        .run()

      assertThat(operation.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
      assertThat(operation.stepsList.single().state).isEqualTo(UploadHealingStep.State.COMPLETE)
      assertThat(operation.stepsList.single().replacementRawImpressionUpload)
        .isEqualTo(CANDIDATE_UPLOAD_NAME)
      assertThat(createRequests).hasSize(1)
      assertThat(createRequests.single().workItem.workItemParams.value).isNotEmpty()
      Unit
    }

  @Test
  fun `pending handoff publishes every replacement model line with exact object identity`() =
    runBlocking {
      val modelLineNames =
        listOf(
          "modelProviders/mp/modelSuites/ms/modelLines/a",
          "modelProviders/mp/modelSuites/ms/modelLines/b",
          "modelProviders/mp/modelSuites/ms/modelLines/c",
        )
      val baseStep = EVICTING_OPERATION.stepsList.single()
      val completedSteps =
        modelLineNames.mapIndexed { index, modelLineName ->
          baseStep.copy {
            name = "$OPERATION_NAME/uploadHealingSteps/${index + 1}"
            cmmsModelLine = modelLineName
            state = UploadHealingStep.State.COMPLETE
            replacementRawImpressionUpload = CANDIDATE_UPLOAD_NAME
          }
        }
      var operation =
        EVICTING_OPERATION.copy {
          state = UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING
          steps.clear()
          steps += completedSteps
        }
      val replacement =
        CANDIDATE_UPLOAD.copy {
          registrationComplete = true
          state = RawImpressionUpload.State.ACTIVE
          uploadHealingOperation = OPERATION_NAME
        }
      val createRequests = mutableListOf<CreateWorkItemRequest>()
      val publisher =
        DataAvailabilitySyncWorkItemPublisher(
          createWorkItem = { _, request ->
            createRequests += request
            request.workItem
              .toBuilder()
              .setName("workItems/${request.workItemId}")
              .setState(WorkItem.State.QUEUED)
              .setGeneration(1)
              .build()
          },
          getWorkItem = { _, _ -> error("unexpected get") },
        )
      whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
        val states =
          invocation
            .getArgument<
              org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
            >(
              0
            )
            .filter
            .stateInList
        listUploadHealingOperationsResponse {
          if (operation.state in states) uploadHealingOperations += operation
        }
      }
      whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
      whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer {
        operation = operation.copy { state = UploadHealingOperation.State.COMPLETE }
        operation
      }
      whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
        .thenReturn(
          CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED }
        )
      whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(replacement)
      whenever(modelLinesService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              modelLineNames.mapIndexed { index, modelLineName ->
                rawImpressionUploadModelLine {
                  name = "$CANDIDATE_UPLOAD_NAME/rawImpressionUploadModelLines/line-$index"
                  cmmsModelLine = modelLineName
                  state = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING
                  pendingAvailabilityDates += date {
                    year = 2026
                    month = 11
                    day = 1
                  }
                }
              }
          }
        )
      val generations =
        modelLineNames
          .mapIndexed { index, modelLineName -> modelLineName to (101L + index) }
          .toMap()
      val createTime = Instant.parse("2026-11-01T00:00:00Z")
      newController(
          dataAvailabilitySyncWorkItemPublisher = publisher,
          doneObjectMetadataReader =
            DoneObjectMetadataReader { uri ->
              val modelLineId = uri.substringAfter("/model-line/").substringBefore('/')
              val modelLineName = modelLineNames.single { it.endsWith("/$modelLineId") }
              DoneObjectMetadata(generations.getValue(modelLineName), createTime)
            },
        )
        .run()

      assertThat(operation.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
      assertThat(createRequests).hasSize(3)
      val requestsByModelLine =
        createRequests.associateBy { request ->
          request.workItem.workItemParams
            .unpack(WorkItem.WorkItemParams::class.java)
            .appParams
            .unpack(DataAvailabilitySyncParams::class.java)
            .modelLine
        }
      assertThat(requestsByModelLine.keys).containsExactlyElementsIn(modelLineNames)
      for ((index, modelLineName) in modelLineNames.withIndex()) {
        val request = requestsByModelLine.getValue(modelLineName)
        val params = request.workItem.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
        val appParams = params.appParams.unpack(DataAvailabilitySyncParams::class.java)
        val eventDate = java.time.LocalDate.of(2026, 11, 1)
        assertThat(appParams.triggeringRawImpressionUpload).isEqualTo(CANDIDATE_UPLOAD_NAME)
        assertThat(appParams.rawImpressionUploadModelLine)
          .isEqualTo("$CANDIDATE_UPLOAD_NAME/rawImpressionUploadModelLines/line-$index")
        assertThat(params.dataPathParams.dataPath)
          .isEqualTo(
            LabeledImpressionsBlobKeys.forDoneUri(
              operation.labeledImpressionsBlobPrefix,
              CANDIDATE_UPLOAD_NAME,
              modelLineName,
              eventDate,
            )
          )
        assertThat(params.dataPathParams.generation).isEqualTo(generations.getValue(modelLineName))
        assertThat(params.dataPathParams.createTime.toInstant()).isEqualTo(createTime)
      }
    }

  @Test
  fun `pending handoff retries after transient publication failure`() = runBlocking {
    val completedStep =
      EVICTING_OPERATION.stepsList.single().copy {
        state = UploadHealingStep.State.COMPLETE
        replacementRawImpressionUpload = CANDIDATE_UPLOAD_NAME
      }
    var operation =
      EVICTING_OPERATION.copy {
        state = UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING
        steps[0] = completedStep
      }
    val replacement =
      CANDIDATE_UPLOAD.copy {
        registrationComplete = true
        state = RawImpressionUpload.State.ACTIVE
        uploadHealingOperation = OPERATION_NAME
      }
    val workItems = linkedMapOf<String, WorkItem>()
    var createAttempts = 0
    val publisher =
      DataAvailabilitySyncWorkItemPublisher(
        createWorkItem = { _, request ->
          createAttempts++
          if (createAttempts == 1) throw Status.UNAVAILABLE.asException()
          val name = "workItems/${request.workItemId}"
          if (workItems.containsKey(name)) throw Status.ALREADY_EXISTS.asException()
          val workItem =
            request.workItem
              .toBuilder()
              .setName(name)
              .setState(WorkItem.State.QUEUED)
              .setGeneration(1)
              .build()
          workItems[name] = workItem
          workItem
        },
        getWorkItem = { _, name -> workItems.getValue(name) },
      )
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    var completionAttempts = 0
    whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer {
      completionAttempts++
      if (completionAttempts == 1) throw Status.UNAVAILABLE.asException()
      operation = operation.copy { state = UploadHealingOperation.State.COMPLETE }
      operation
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED })
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(replacement)
    whenever(modelLinesService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = "$CANDIDATE_UPLOAD_NAME/rawImpressionUploadModelLines/ml"
            cmmsModelLine = CMMS_MODEL_LINE
            state = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING
            pendingAvailabilityDates += date {
              year = 2026
              month = 11
              day = 1
            }
          }
        }
      )
    var metadataAttempts = 0
    val controller =
      newController(
        dataAvailabilitySyncWorkItemPublisher = publisher,
        doneObjectMetadataReader =
          DoneObjectMetadataReader {
            metadataAttempts++
            if (metadataAttempts == 1) throw Status.UNAVAILABLE.asException()
            DoneObjectMetadata(123L, Instant.parse("2026-11-01T00:00:00Z"))
          },
      )

    controller.run()
    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING)
    assertThat(workItems).isEmpty()
    assertThat(createAttempts).isEqualTo(0)

    controller.run()
    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING)
    assertThat(workItems).isEmpty()

    controller.run()
    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING)
    assertThat(workItems).hasSize(1)

    controller.run()
    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(workItems).hasSize(1)
    assertThat(createAttempts).isEqualTo(3)
    assertThat(completionAttempts).isEqualTo(2)
    assertThat(metadataAttempts).isEqualTo(4)
  }

  @Test
  fun `pending handoff resumes after only the first date was published`() = runBlocking {
    val completedStep =
      EVICTING_OPERATION.stepsList.single().copy {
        state = UploadHealingStep.State.COMPLETE
        replacementRawImpressionUpload = CANDIDATE_UPLOAD_NAME
      }
    var operation =
      EVICTING_OPERATION.copy {
        state = UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING
        steps[0] = completedStep
      }
    val replacement =
      CANDIDATE_UPLOAD.copy {
        registrationComplete = true
        state = RawImpressionUpload.State.ACTIVE
        uploadHealingOperation = OPERATION_NAME
      }
    val workItems = linkedMapOf<String, WorkItem>()
    var failSecondDate = true
    var createAttempts = 0
    val publisher =
      DataAvailabilitySyncWorkItemPublisher(
        createWorkItem = { _, request ->
          createAttempts++
          val name = "workItems/${request.workItemId}"
          if (workItems.containsKey(name)) throw Status.ALREADY_EXISTS.asException()
          if (workItems.size == 1 && failSecondDate) {
            failSecondDate = false
            throw Status.UNAVAILABLE.asException()
          }
          val workItem =
            request.workItem
              .toBuilder()
              .setName(name)
              .setState(WorkItem.State.QUEUED)
              .setGeneration(1)
              .build()
          workItems[name] = workItem
          workItem
        },
        getWorkItem = { _, name -> workItems.getValue(name) },
      )
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer {
      operation = operation.copy { state = UploadHealingOperation.State.COMPLETE }
      operation
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED })
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(replacement)
    whenever(modelLinesService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = "$CANDIDATE_UPLOAD_NAME/rawImpressionUploadModelLines/ml"
            cmmsModelLine = CMMS_MODEL_LINE
            state = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING
            pendingAvailabilityDates +=
              listOf(
                date {
                  year = 2026
                  month = 11
                  day = 1
                },
                date {
                  year = 2026
                  month = 11
                  day = 2
                },
              )
          }
        }
      )
    val controller =
      newController(
        dataAvailabilitySyncWorkItemPublisher = publisher,
        doneObjectMetadataReader =
          DoneObjectMetadataReader { uri ->
            DoneObjectMetadata(
              if ("2026-11-01" in uri) 101L else 102L,
              Instant.parse("2026-11-01T00:00:00Z"),
            )
          },
      )

    controller.run()
    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING)
    assertThat(workItems).hasSize(1)

    controller.run()
    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(workItems).hasSize(2)
    assertThat(createAttempts).isEqualTo(4)
  }

  @Test
  fun `availability-syncing replacement without pending dates needs attention`() = runBlocking {
    val completedStep =
      EVICTING_OPERATION.stepsList.single().copy {
        state = UploadHealingStep.State.COMPLETE
        replacementRawImpressionUpload = CANDIDATE_UPLOAD_NAME
      }
    var operation =
      EVICTING_OPERATION.copy {
        state = UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING
        steps[0] = completedStep
      }
    val replacement =
      CANDIDATE_UPLOAD.copy {
        registrationComplete = true
        state = RawImpressionUpload.State.ACTIVE
        uploadHealingOperation = OPERATION_NAME
      }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
        >(
          0
        )
      check(request.state == UploadHealingOperation.State.NEEDS_ATTENTION)
      operation =
        operation.copy {
          state = UploadHealingOperation.State.NEEDS_ATTENTION
          resumeState = UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING
        }
      operation
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED })
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(replacement)
    whenever(modelLinesService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = "$CANDIDATE_UPLOAD_NAME/rawImpressionUploadModelLines/ml"
            cmmsModelLine = CMMS_MODEL_LINE
            state = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING
          }
        }
      )

    newController().run()

    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.NEEDS_ATTENTION)
    assertThat(operation.resumeState)
      .isEqualTo(UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING)
  }

  @Test
  fun `failed replacement moves the operation to needs attention`() = runBlocking {
    val recoveryStartedStep =
      EVICTING_OPERATION.stepsList.single().copy {
        state = UploadHealingStep.State.RECOVERY_STARTED
        recoveryDoneBlobGeneration = CANDIDATE_UPLOAD.doneBlobGeneration
      }
    var operation =
      EVICTING_OPERATION.copy {
        state = UploadHealingOperation.State.REPLAYING
        steps[0] = recoveryStartedStep
      }
    whenever(operationsService.listUploadHealingOperations(any())).thenAnswer { invocation ->
      val states =
        invocation
          .getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
          >(
            0
          )
          .filter
          .stateInList
      listUploadHealingOperationsResponse {
        if (operation.state in states) uploadHealingOperations += operation
      }
    }
    whenever(operationsService.getUploadHealingOperation(any())).thenAnswer { operation }
    whenever(operationsService.advanceUploadHealingOperation(any())).thenAnswer { invocation ->
      operation =
        operation.copy {
          state =
            invocation
              .getArgument<
                org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
              >(
                0
              )
              .state
          etag = "attention"
        }
      operation
    }
    whenever(candidatesService.getRawImpressionUploadCorrectionCandidate(any()))
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.ASSIGNED })
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(SOURCE_UPLOAD)
    whenever(uploadsService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads +=
            listOf(
              SOURCE_UPLOAD,
              CANDIDATE_UPLOAD.copy { state = RawImpressionUpload.State.FAILED },
            )
        }
      )
    whenever(modelLinesService.listRawImpressionUploadModelLines(any()))
      .thenReturn(listRawImpressionUploadModelLinesResponse {})

    newController().run()

    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.NEEDS_ATTENTION)
  }

  private fun testEvictionExecutor(
    finalize: suspend (EvictUploader.EvictionPlan) -> EvictUploader.EvictionPlan = {
      it.copy(outputManifestsFinalized = true)
    },
    evict:
      suspend (
        EvictUploader.EvictionPlan, String, suspend (EvictUploader.CascadeEntry) -> Unit,
      ) -> EvictUploader.EvictionResult,
  ): EvictionExecutor =
    object : EvictionExecutor {
      override suspend fun finalizeOutputManifests(
        plan: EvictUploader.EvictionPlan
      ): EvictUploader.EvictionPlan = finalize(plan)

      override suspend fun evict(
        plan: EvictUploader.EvictionPlan,
        reason: String,
        onEntryEvicted: suspend (EvictUploader.CascadeEntry) -> Unit,
      ): EvictUploader.EvictionResult = evict(plan, reason, onEntryEvicted)
    }

  private fun newController(
    evictionExecutor: EvictionExecutor = testEvictionExecutor { _, _, _ ->
      EvictUploader.EvictionResult(emptyList(), 0, 0, 0)
    },
    manifestReader: CorrectionManifestReader = CorrectionManifestReader { _, _ -> emptyList() },
    doneBlobReplayer: DoneBlobReplayer = DoneBlobReplayer { _ -> },
    recoveryExecutor: RecoveryExecutor = RecoveryExecutor { _, _ -> error("unexpected recovery") },
    dataAvailabilitySyncWorkItemPublisher: DataAvailabilitySyncWorkItemPublisher =
      DataAvailabilitySyncWorkItemPublisher(
        createWorkItem = { _, _ -> error("unexpected availability handoff") },
        getWorkItem = { _, _ -> error("unexpected availability lookup") },
      ),
    doneObjectMetadataReader: DoneObjectMetadataReader = DoneObjectMetadataReader { null },
    eventSink: VidLabelingHealingControllerEventSink = VidLabelingHealingControllerEventSink {},
    candidateCleaner: CorrectionCandidateCleaner = CorrectionCandidateCleaner { _ -> },
    stallTimeout: Duration = Duration.ofHours(1),
  ): VidLabelingHealingController {
    val planner =
      RawImpressionUploadCorrectionPlanner(
        planCorrection = { owners, cutoff, operationId ->
          EvictUploader.EvictionPlan(
            cascade =
              listOf(
                EvictUploader.CascadeEntry(
                  SOURCE_UPLOAD_NAME,
                  MODEL_LINE_ROW,
                  CMMS_MODEL_LINE,
                  memoized = false,
                  RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION,
                  recoveryPredecessorUploadName = "",
                )
              ),
            extraUploads = emptyList(),
            memoizedModelLines = emptySet(),
            nonMemoizedModelLines = setOf(CMMS_MODEL_LINE),
            badUploads = owners,
            noReplacementUploads = emptySet(),
            cutoffTime = cutoff,
            evictionOperationId = operationId,
            recoveryTargets = emptyList(),
            replacementTargets =
              listOf(EvictUploader.RecoveryTarget(SOURCE_UPLOAD_NAME, listOf(CMMS_MODEL_LINE))),
          )
        },
        operationIdGenerator = { OPERATION_ID },
      )
    return VidLabelingHealingController(
      listOf(
        VidLabelingHealingController.DataProviderConfig(
          DATA_PROVIDER,
          "gs://output/vid",
          Duration.ofDays(30),
          stallTimeout,
        )
      ),
      RawImpressionUploadCorrectionCandidateServiceGrpcKt
        .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(grpcTestServerRule.channel),
      candidateCleaner,
      UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
        grpcTestServerRule.channel
      ),
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(
        grpcTestServerRule.channel
      ),
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
        grpcTestServerRule.channel
      ),
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        grpcTestServerRule.channel
      ),
      RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(grpcTestServerRule.channel),
      plannerFactory = { planner },
      evictionExecutorFactory = { evictionExecutor },
      manifestReader = manifestReader,
      doneBlobReplayerFactory = { doneBlobReplayer },
      recoveryExecutorFactory = { recoveryExecutor },
      dataAvailabilitySyncWorkItemPublisher = dataAvailabilitySyncWorkItemPublisher,
      doneObjectMetadataReader = doneObjectMetadataReader,
      eventSink = eventSink,
      clock = Clock.fixed(Instant.ofEpochSecond(1_000L), ZoneOffset.UTC),
    )
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp"
    private const val SOURCE_UPLOAD_NAME = "$DATA_PROVIDER/rawImpressionUploads/source"
    private const val CANDIDATE_UPLOAD_NAME = "$DATA_PROVIDER/rawImpressionUploads/candidate"
    private const val CANDIDATE_NAME =
      "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private const val OPERATION_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    private const val OPERATION_NAME = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID"
    private const val CMMS_MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val MODEL_LINE_ROW = "$SOURCE_UPLOAD_NAME/rawImpressionUploadModelLines/ml"
    private val EMPTY_DIGEST =
      ByteString.copyFrom(MessageDigest.getInstance("SHA-256").digest(byteArrayOf()))
    private val SOURCE_UPLOAD = rawImpressionUpload {
      name = SOURCE_UPLOAD_NAME
      doneBlobUri = "gs://raw/day/done"
      doneBlobGeneration = 1L
      createTime = timestamp { seconds = 100L }
      registrationComplete = true
      state = RawImpressionUpload.State.COMPLETED
    }
    private val CANDIDATE_UPLOAD = rawImpressionUpload {
      name = CANDIDATE_UPLOAD_NAME
      doneBlobUri = "gs://raw/day/done"
      doneBlobGeneration = 2L
      replacesRawImpressionUpload = SOURCE_UPLOAD_NAME
      createTime = timestamp { seconds = 200L }
      state = RawImpressionUpload.State.CORRECTION_REQUIRED
    }
    private val CANDIDATE = rawImpressionUploadCorrectionCandidate {
      name = CANDIDATE_NAME
      rawImpressionUpload = CANDIDATE_UPLOAD_NAME
      classification = RawImpressionUploadCorrectionCandidate.Classification.EDITED
      priorManifestDigest = EMPTY_DIGEST
      currentManifestDigest = EMPTY_DIGEST
      state = RawImpressionUploadCorrectionCandidate.State.PENDING
      createTime = timestamp { seconds = 300L }
      manifestDifferences +=
        RawImpressionUploadCorrectionCandidateKt.manifestDifference {
          type = RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.EDITED
          blobUri = "gs://raw/day/file"
          priorBlobGeneration = 1L
          currentBlobGeneration = 2L
          priorOutputSourceRawImpressionUpload = SOURCE_UPLOAD_NAME
        }
    }
    private val APPROVED_OPERATION = uploadHealingOperation {
      name = OPERATION_NAME
      state = UploadHealingOperation.State.APPROVED
      planFormatVersion = RawImpressionUploadCorrectionPlanner.PLAN_FORMAT_VERSION
      outputManifestState =
        UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_PROVISIONAL
      etag = "approved-etag"
      reason = "correction"
      labeledImpressionsBlobPrefix = "gs://output/vid"
      badRawImpressionUploads += SOURCE_UPLOAD_NAME
      rawImpressionUploadCorrectionCandidates += CANDIDATE_NAME
      cutoffTime = timestamp { seconds = 1L }
      steps += uploadHealingStep {
        name = "$OPERATION_NAME/uploadHealingSteps/1"
        sourceRawImpressionUpload = SOURCE_UPLOAD_NAME
        rawImpressionUploadModelLine = MODEL_LINE_ROW
        cmmsModelLine = CMMS_MODEL_LINE
        recoveryAction = RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
        rawImpressionUploadCorrectionCandidate = CANDIDATE_NAME
        labeledOutputManifest = labeledOutputManifest {
          blobs += LabeledOutputManifestKt.blobVersion { blobUri = "gs://output/vid/file" }
        }
        recoveryTarget = true
        state =
          org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep.State.PENDING_EVICTION
        etag = "step-etag"
      }
    }
    private val EVICTING_OPERATION =
      APPROVED_OPERATION.copy {
        state = UploadHealingOperation.State.EVICTING
        outputManifestState =
          UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_FINALIZED
        steps[0] =
          steps[0].copy {
            labeledOutputManifest = labeledOutputManifest {
              blobs +=
                LabeledOutputManifestKt.blobVersion {
                  blobUri = "gs://output/vid/file"
                  generation = 1L
                }
            }
          }
        etag = "evicting-etag"
      }
  }
}
