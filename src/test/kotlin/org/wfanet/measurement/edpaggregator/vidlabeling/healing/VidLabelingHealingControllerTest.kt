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
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier

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
          state = RawImpressionUploadCorrectionCandidate.State.HEALING
          decision = RawImpressionUploadCorrectionCandidate.Decision.DECISION_CORRECT
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
    val evictionExecutor = EvictionExecutor { plan, _, checkpoint ->
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
          state = RawImpressionUploadCorrectionCandidate.State.HEALING
          decision = RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT
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
    val evictionExecutor = EvictionExecutor { plan, _, checkpoint ->
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
  fun `run reopens an active plan whose candidate was superseded`() = runBlocking {
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
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.HEALING })
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
  fun `restart completes a partially recorded recovery checkpoint`() = runBlocking {
    val firstStep =
      APPROVED_OPERATION.stepsList.single().copy {
        recoveryAction =
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        state = UploadHealingStep.State.RECOVERY_STARTED
        recoveryDoneBlobGeneration = SOURCE_UPLOAD.doneBlobGeneration
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
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.HEALING })
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
    whenever(operationsService.advanceUploadHealingStep(any())).thenAnswer { invocation ->
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
    val replayer = org.mockito.kotlin.mock<DoneBlobReplayer>()

    newController(doneBlobReplayer = replayer).run()

    assertThat(operation.stepsList.map { it.state }.distinct())
      .containsExactly(UploadHealingStep.State.RECOVERY_STARTED)
    assertThat(operation.stepsList.map { it.recoveryDoneBlobGeneration }.distinct())
      .containsExactly(SOURCE_UPLOAD.doneBlobGeneration)
    verify(replayer, never()).replay(any())
    Unit
  }

  @Test
  fun `failed replacement moves the operation to needs attention`() = runBlocking {
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
      .thenReturn(CANDIDATE.copy { state = RawImpressionUploadCorrectionCandidate.State.HEALING })
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

  private fun newController(
    evictionExecutor: EvictionExecutor = EvictionExecutor { _, _, _ ->
      EvictUploader.EvictionResult(emptyList(), 0, 0, 0)
    },
    manifestReader: CorrectionManifestReader = CorrectionManifestReader { _, _ -> emptyList() },
    doneBlobReplayer: DoneBlobReplayer = DoneBlobReplayer { _ -> },
    eventSink: VidLabelingHealingControllerEventSink = VidLabelingHealingControllerEventSink {},
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
          historicalOwnerRawImpressionUpload = SOURCE_UPLOAD_NAME
        }
    }
    private val APPROVED_OPERATION = uploadHealingOperation {
      name = OPERATION_NAME
      state = UploadHealingOperation.State.APPROVED
      etag = "approved-etag"
      reason = "correction"
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
        recoveryTarget = true
        state =
          org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep.State.PENDING_EVICTION
        etag = "step-etag"
      }
    }
    private val EVICTING_OPERATION =
      APPROVED_OPERATION.copy {
        state = UploadHealingOperation.State.EVICTING
        etag = "evicting-etag"
      }
  }
}
