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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat as assertThatProto
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Rule
import org.junit.Test
import org.mockito.Mockito.mockingDetails
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FinalizeUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.ReconcileUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.getUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.labeledOutputManifest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.reconcileUploadHealingOperationResponse
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep

class HealingOperationStoreTest {
  private val recording = VidLabelingRpcThrottlersTestHelper.recording()
  private val service =
    mockService<
      UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase
    >()
  @get:Rule val server = GrpcTestServerRule { addService(service) }
  private val store by lazy {
    GrpcHealingOperationStore(
      UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
        server.channel
      ),
      recording.throttlers,
    )
  }

  @After
  fun verifyPacing() {
    val calls = mockingDetails(service).invocations.filter { it.method.parameterCount == 2 }
    val reads =
      calls.count { it.method.name.startsWith("get") || it.method.name.startsWith("list") }
    assertThat(recording.metadataRead.invocationCount).isEqualTo(reads)
    assertThat(recording.metadataWrite.invocationCount).isEqualTo(calls.size - reads)
  }

  @Test
  fun `reconcile sends the complete controller plan through the public API`() =
    runBlocking<Unit> {
      whenever(service.reconcileUploadHealingOperation(any()))
        .thenReturn(
          reconcileUploadHealingOperationResponse { uploadHealingOperation = OPERATION_RESOURCE }
        )
      val result =
        store.reconcileUploadHealingOperation(
          ReconcileUploadHealingOperationCommand(
            PARENT,
            DRAFT_RESOURCE,
            OPERATION_ID,
            "draft-etag",
            REQUEST_ID,
          )
        )
      val requests = argumentCaptor<ReconcileUploadHealingOperationRequest>()
      verify(service).reconcileUploadHealingOperation(requests.capture())
      val request = requests.firstValue
      assertThat(request.parent).isEqualTo(PARENT)
      assertThat(request.uploadHealingOperationId).isEqualTo(OPERATION_ID)
      assertThat(request.etag).isEqualTo("draft-etag")
      assertThat(request.requestId).isEqualTo(REQUEST_ID)
      assertThat(request.plan.reason).isEqualTo(OPERATION_RESOURCE.reason)
      assertThat(request.plan.rawImpressionUploadCorrectionCandidatesList)
        .containsExactly(CANDIDATE)
      val step = request.plan.stepsList.single()
      assertThat(step.sourceRawImpressionUpload).isEqualTo(SOURCE)
      assertThat(step.rawImpressionUploadModelLine).isEqualTo(MODEL_LINE_ROW)
      assertThat(step.cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
      assertThat(step.recoveryPredecessorRawImpressionUpload).isEqualTo(PREVIOUS)
      assertThat(step.rawImpressionUploadCorrectionCandidate).isEqualTo(CANDIDATE)
      assertThat(step.recoveryAction).isEqualTo(STEP_RESOURCE.recoveryAction)
      assertThat(step.recoveryTarget).isTrue()
      assertThatProto(step.labeledOutputManifest)
        .isEqualTo(DRAFT_RESOURCE.stepsList.single().labeledOutputManifest)
      assertThatProto(result).isEqualTo(OPERATION_RESOURCE)
    }

  @Test
  fun `advance operation preserves state etag and request identity`() =
    runBlocking<Unit> {
      whenever(service.advanceUploadHealingOperation(any())).thenReturn(OPERATION_RESOURCE)
      val result =
        store.advanceUploadHealingOperation(
          AdvanceUploadHealingOperationCommand(
            OPERATION,
            "etag",
            UploadHealingOperation.State.EVICTING,
            REQUEST_ID,
          )
        )
      val requests = argumentCaptor<AdvanceUploadHealingOperationRequest>()
      verify(service).advanceUploadHealingOperation(requests.capture())
      assertThat(requests.firstValue.name).isEqualTo(OPERATION)
      assertThat(requests.firstValue.state).isEqualTo(UploadHealingOperation.State.EVICTING)
      assertThat(requests.firstValue.etag).isEqualTo("etag")
      assertThat(requests.firstValue.requestId).isEqualTo(REQUEST_ID)
      assertThatProto(result).isEqualTo(OPERATION_RESOURCE)
    }

  @Test
  fun `finalize preserves exact generations and absent generation markers`() =
    runBlocking<Unit> {
      whenever(service.finalizeUploadHealingOperation(any())).thenReturn(OPERATION_RESOURCE)
      val result =
        store.finalizeUploadHealingOperation(
          FinalizeUploadHealingOperationCommand(
            OPERATION,
            listOf(UploadHealingStepManifest(STEP, STEP_RESOURCE.labeledOutputManifest)),
            "etag",
            REQUEST_ID,
          )
        )
      val requests = argumentCaptor<FinalizeUploadHealingOperationRequest>()
      verify(service).finalizeUploadHealingOperation(requests.capture())
      assertThat(requests.firstValue.name).isEqualTo(OPERATION)
      assertThat(requests.firstValue.etag).isEqualTo("etag")
      assertThat(requests.firstValue.requestId).isEqualTo(REQUEST_ID)
      assertThat(requests.firstValue.stepManifestsList.single().uploadHealingStep).isEqualTo(STEP)
      assertThatProto(requests.firstValue.stepManifestsList.single().labeledOutputManifest)
        .isEqualTo(STEP_RESOURCE.labeledOutputManifest)
      assertThat(
          requests.firstValue.stepManifestsList
            .single()
            .labeledOutputManifest
            .blobsList[1]
            .hasGeneration()
        )
        .isFalse()
      assertThatProto(result).isEqualTo(OPERATION_RESOURCE)
    }

  @Test
  fun `step checkpoints preserve all actions and evidence`() =
    runBlocking<Unit> {
      whenever(service.advanceUploadHealingStep(any())).thenReturn(STEP_RESOURCE)
      for (action in UploadHealingStepAction.entries) {
        val result =
          store.advanceUploadHealingStep(
            AdvanceUploadHealingStepCommand(
              STEP,
              "step-etag",
              action,
              if (action == UploadHealingStepAction.CONFIRM_REPLACEMENT) REPLACEMENT else "",
              if (action == UploadHealingStepAction.RECORD_RECOVERY) 73L else 0L,
              REQUEST_ID,
            )
          )
        assertThatProto(result).isEqualTo(STEP_RESOURCE)
      }
      val requests = argumentCaptor<AdvanceUploadHealingStepRequest>()
      verify(service, org.mockito.kotlin.times(3)).advanceUploadHealingStep(requests.capture())
      assertThat(requests.allValues.map { it.action })
        .containsExactly(
          AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION,
          AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY,
          AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT,
        )
        .inOrder()
      for (request in requests.allValues) {
        assertThat(request.name).isEqualTo(STEP)
        assertThat(request.etag).isEqualTo("step-etag")
        assertThat(request.requestId).isEqualTo(REQUEST_ID)
        assertThat(request.replacementRawImpressionUpload)
          .isEqualTo(
            if (request.action == AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT)
              REPLACEMENT
            else ""
          )
        assertThat(request.recoveryDoneBlobGeneration)
          .isEqualTo(
            if (request.action == AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY) 73L
            else 0L
          )
      }
    }

  @Test
  fun `get and successive list pages use the read budget`() =
    runBlocking<Unit> {
      whenever(service.getUploadHealingOperation(any())).thenReturn(OPERATION_RESOURCE)
      whenever(service.listUploadHealingOperations(any()))
        .thenReturn(
          listUploadHealingOperationsResponse {
            uploadHealingOperations += OPERATION_RESOURCE
            nextPageToken = "next"
          },
          listUploadHealingOperationsResponse {},
        )
      assertThatProto(
          store.getUploadHealingOperation(getUploadHealingOperationRequest { name = OPERATION })
        )
        .isEqualTo(OPERATION_RESOURCE)
      val first =
        store.listUploadHealingOperations(listUploadHealingOperationsRequest { parent = PARENT })
      val last =
        store.listUploadHealingOperations(
          listUploadHealingOperationsRequest {
            parent = PARENT
            pageToken = first.nextPageToken
          }
        )
      assertThat(first.uploadHealingOperationsList).containsExactly(OPERATION_RESOURCE)
      assertThat(last.uploadHealingOperationsList).isEmpty()
      assertThat(recording.metadataRead.invocationCount).isEqualTo(3)
    }

  @Test
  fun `cancellation while waiting for a read permit sends no RPC`() =
    runBlocking<Unit> {
      val entered = CompletableDeferred<Unit>()
      val release = CompletableDeferred<Unit>()
      val gate =
        object : Throttler {
          override suspend fun <T> onReady(block: suspend () -> T): T {
            entered.complete(Unit)
            release.await()
            return recording.metadataRead.onReady(block)
          }
        }
      val gated =
        GrpcHealingOperationStore(
          UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
            server.channel
          ),
          recording.throttlers.copy(metadataRead = gate),
        )
      val request = async {
        gated.getUploadHealingOperation(getUploadHealingOperationRequest { name = OPERATION })
      }
      entered.await()
      request.cancel()
      assertFailsWith<CancellationException> { request.await() }
      verify(service, never()).getUploadHealingOperation(any())
    }

  companion object {
    private const val PARENT = "dataProviders/dp"
    private const val OPERATION_ID = "11111111-1111-4111-8111-111111111111"
    private const val REQUEST_ID = "22222222-2222-4222-8222-222222222222"
    private const val OPERATION = "$PARENT/uploadHealingOperations/$OPERATION_ID"
    private const val STEP = "$OPERATION/uploadHealingSteps/1"
    private const val SOURCE = "$PARENT/rawImpressionUploads/source"
    private const val PREVIOUS = "$PARENT/rawImpressionUploads/previous"
    private const val REPLACEMENT = "$PARENT/rawImpressionUploads/replacement"
    private const val MODEL_LINE_ROW = "$SOURCE/rawImpressionUploadModelLines/row"
    private const val CMMS_MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val CANDIDATE = "$PARENT/rawImpressionUploadCorrectionCandidates/candidate"
    private val STEP_RESOURCE = uploadHealingStep {
      name = STEP
      sourceRawImpressionUpload = SOURCE
      rawImpressionUploadModelLine = MODEL_LINE_ROW
      cmmsModelLine = CMMS_MODEL_LINE
      recoveryAction = RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
      recoveryPredecessorRawImpressionUpload = PREVIOUS
      recoveryTarget = true
      rawImpressionUploadCorrectionCandidate = CANDIDATE
      labeledOutputManifest = labeledOutputManifest {
        blobs +=
          LabeledOutputManifestKt.blobVersion {
            blobUri = "gs://output/file.avro"
            generation = 42L
          }
        blobs += LabeledOutputManifestKt.blobVersion { blobUri = "gs://output/absent.avro" }
      }
    }
    private val OPERATION_RESOURCE = uploadHealingOperation {
      name = OPERATION
      state = UploadHealingOperation.State.EVICTING
      reason = "correction"
      planFormatVersion = 2
      outputManifestState =
        UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_FINALIZED
      rawImpressionUploadCorrectionCandidates += CANDIDATE
      steps += STEP_RESOURCE
    }
    private val DRAFT_RESOURCE =
      OPERATION_RESOURCE.copy {
        outputManifestState =
          UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_PROVISIONAL
        steps.clear()
        steps +=
          STEP_RESOURCE.copy {
            labeledOutputManifest =
              STEP_RESOURCE.labeledOutputManifest.copy {
                blobs.clear()
                blobs +=
                  STEP_RESOURCE.labeledOutputManifest.blobsList.map {
                    it.copy { clearGeneration() }
                  }
              }
          }
      }
  }
}
