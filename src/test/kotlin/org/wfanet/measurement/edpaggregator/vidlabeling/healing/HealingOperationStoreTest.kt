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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
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
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt as PublicServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.getUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.labeledOutputManifest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingOperationRequest as InternalAdvanceOperationRequest
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest as InternalAdvanceStepRequest
import org.wfanet.measurement.internal.edpaggregator.FinalizeUploadHealingOperationRequest as InternalFinalizeOperationRequest
import org.wfanet.measurement.internal.edpaggregator.ReconcileUploadHealingOperationRequest as InternalReconcileRequest
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt as InternalServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation as internalOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep as internalStep

@RunWith(JUnit4::class)
class HealingOperationStoreTest {
  private val recordingThrottlers = VidLabelingRpcThrottlersTestHelper.recording()

  @After
  fun verifyRpcPacing() {
    val calls =
      listOf(readService, mutationService)
        .flatMap { mockingDetails(it).invocations }
        .filter { it.method.parameterCount == 2 }
        .filter {
          it.method.name.startsWith("get") ||
            it.method.name.startsWith("list") ||
            it.method.name.startsWith("acquire") ||
            it.method.name.startsWith("advance") ||
            it.method.name.startsWith("release") ||
            it.method.name.startsWith("mark") ||
            it.method.name.startsWith("delete") ||
            it.method.name.startsWith("batchDelete") ||
            it.method.name.startsWith("reconcile") ||
            it.method.name.startsWith("finalize")
        }
    val reads =
      calls.count { it.method.name.startsWith("get") || it.method.name.startsWith("list") }
    assertThat(recordingThrottlers.metadataRead.invocationCount).isEqualTo(reads)
    assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(calls.size - reads)
    assertThat(recordingThrottlers.kingdom.invocationCount).isEqualTo(0)
    assertThat(recordingThrottlers.controlPlane.invocationCount).isEqualTo(0)
  }

  private val readService =
    mockService<PublicServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase>()
  private val mutationService =
    mockService<InternalServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase>()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(readService)
    addService(mutationService)
  }

  private val store by lazy {
    GrpcHealingOperationStore(
      PublicServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(grpcTestServerRule.channel),
      InternalServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(grpcTestServerRule.channel),
      rpcThrottlers = recordingThrottlers.throttlers,
    )
  }

  @Test
  fun `reconcile translates candidate graph and reads persisted operation`() =
    runBlocking<Unit> {
      whenever(mutationService.reconcileUploadHealingOperation(any()))
        .thenReturn(internalOperation {})
      whenever(readService.getUploadHealingOperation(any())).thenReturn(PUBLIC_OPERATION)

      val result =
        store.reconcileUploadHealingOperation(
          ReconcileUploadHealingOperationCommand(
            parent = DATA_PROVIDER,
            uploadHealingOperationId = OPERATION_ID,
            requestId = REQUEST_ID,
            uploadHealingOperation =
              uploadHealingOperation {
                planFormatVersion = 2
                outputManifestState =
                  UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_FINALIZED
                reason = "correction"
                rawImpressionUploadCorrectionCandidates += CANDIDATE
                steps += PUBLIC_STEP
              },
          )
        )

      val request = argumentCaptor<InternalReconcileRequest>()
      verify(mutationService).reconcileUploadHealingOperation(request.capture())
      assertThat(request.firstValue.dataProviderResourceId).isEqualTo("dp")
      assertThat(request.firstValue.uploadHealingOperation.state)
        .isEqualTo(
          org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation.State
            .UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
        )
      assertThat(request.firstValue.uploadHealingOperation.planFormatVersion).isEqualTo(2)
      assertThat(request.firstValue.uploadHealingOperation.outputManifestState)
        .isEqualTo(
          org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation.OutputManifestState
            .UPLOAD_HEALING_OUTPUT_MANIFEST_STATE_FINALIZED
        )
      assertThat(
          request.firstValue.uploadHealingOperation.rawImpressionUploadCorrectionCandidateIdsList
        )
        .containsExactly("candidate")
      val step = request.firstValue.uploadHealingOperation.stepsList.single()
      assertThat(step.sourceRawImpressionUploadResourceId).isEqualTo("source")
      assertThat(step.rawImpressionUploadModelLineResourceId).isEqualTo("row")
      assertThat(step.recoveryPredecessorRawImpressionUploadResourceId).isEqualTo("previous")
      assertThat(step.rawImpressionUploadCorrectionCandidateId).isEqualTo("candidate")
      assertThat(step.labeledOutputManifest.blobsList)
        .containsExactly(
          org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifest.BlobVersion
            .newBuilder()
            .setBlobUri("gs://output/file.avro")
            .setGeneration(42L)
            .build()
        )
      assertThat(result).isEqualTo(PUBLIC_OPERATION)
    }

  @Test
  fun `advance operation maps state and reads persisted operation`() =
    runBlocking<Unit> {
      whenever(mutationService.advanceUploadHealingOperation(any()))
        .thenReturn(internalOperation {})
      whenever(readService.getUploadHealingOperation(any())).thenReturn(PUBLIC_OPERATION)

      store.advanceUploadHealingOperation(
        AdvanceUploadHealingOperationCommand(
          name = OPERATION,
          etag = "etag",
          state = UploadHealingOperation.State.EVICTING,
          requestId = REQUEST_ID,
        )
      )

      val request = argumentCaptor<InternalAdvanceOperationRequest>()
      verify(mutationService).advanceUploadHealingOperation(request.capture())
      assertThat(request.firstValue.dataProviderResourceId).isEqualTo("dp")
      assertThat(request.firstValue.uploadHealingOperationId).isEqualTo(OPERATION_ID)
      assertThat(request.firstValue.requestId).isEqualTo(REQUEST_ID)
      assertThat(request.firstValue.state)
        .isEqualTo(
          org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation.State
            .UPLOAD_HEALING_OPERATION_STATE_EVICTING
        )
    }

  @Test
  fun `finalize maps exact step manifests and reads persisted operation`() =
    runBlocking<Unit> {
      whenever(mutationService.finalizeUploadHealingOperation(any()))
        .thenReturn(internalOperation {})
      whenever(readService.getUploadHealingOperation(any())).thenReturn(PUBLIC_OPERATION)

      val result =
        store.finalizeUploadHealingOperation(
          FinalizeUploadHealingOperationCommand(
            name = OPERATION,
            stepManifests =
              listOf(UploadHealingStepManifest(STEP, PUBLIC_STEP.labeledOutputManifest)),
            etag = "etag",
            requestId = REQUEST_ID,
          )
        )

      val request = argumentCaptor<InternalFinalizeOperationRequest>()
      verify(mutationService).finalizeUploadHealingOperation(request.capture())
      assertThat(request.firstValue.uploadHealingOperationId).isEqualTo(OPERATION_ID)
      assertThat(request.firstValue.stepManifestsList.single().uploadHealingStepId).isEqualTo(1L)
      assertThat(request.firstValue.stepManifestsList.single().labeledOutputManifest)
        .isEqualTo(
          org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifest.newBuilder()
            .addBlobs(
              org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifest.BlobVersion
                .newBuilder()
                .setBlobUri("gs://output/file.avro")
                .setGeneration(42L)
            )
            .build()
        )
      assertThat(result).isEqualTo(PUBLIC_OPERATION)
    }

  @Test
  fun `advance step maps action resources and reads persisted step`() =
    runBlocking<Unit> {
      whenever(mutationService.advanceUploadHealingStep(any())).thenReturn(internalStep {})
      whenever(readService.getUploadHealingOperation(any())).thenReturn(PUBLIC_OPERATION)

      val result =
        store.advanceUploadHealingStep(
          AdvanceUploadHealingStepCommand(
            name = STEP,
            etag = "step-etag",
            action = UploadHealingStepAction.CONFIRM_REPLACEMENT,
            replacementRawImpressionUpload = REPLACEMENT,
            requestId = REQUEST_ID,
          )
        )

      val request = argumentCaptor<InternalAdvanceStepRequest>()
      verify(mutationService).advanceUploadHealingStep(request.capture())
      assertThat(request.firstValue.uploadHealingStepId).isEqualTo(1L)
      assertThat(request.firstValue.action)
        .isEqualTo(InternalAdvanceStepRequest.Action.CONFIRM_REPLACEMENT)
      assertThat(request.firstValue.replacementRawImpressionUploadResourceId)
        .isEqualTo("replacement")
      assertThat(result.name).isEqualTo(STEP)
    }

  @Test
  fun `public get and successive list pages use the read budget`() =
    runBlocking<Unit> {
      whenever(readService.getUploadHealingOperation(any())).thenReturn(PUBLIC_OPERATION)
      whenever(readService.listUploadHealingOperations(any()))
        .thenReturn(
          listUploadHealingOperationsResponse {
            uploadHealingOperations += PUBLIC_OPERATION
            nextPageToken = "next"
          },
          listUploadHealingOperationsResponse {},
        )

      assertThat(
          store.getUploadHealingOperation(getUploadHealingOperationRequest { name = OPERATION })
        )
        .isEqualTo(PUBLIC_OPERATION)
      val first =
        store.listUploadHealingOperations(
          listUploadHealingOperationsRequest { parent = DATA_PROVIDER }
        )
      val last =
        store.listUploadHealingOperations(
          listUploadHealingOperationsRequest {
            parent = DATA_PROVIDER
            pageToken = first.nextPageToken
          }
        )

      assertThat(first.uploadHealingOperationsList).containsExactly(PUBLIC_OPERATION)
      assertThat(last.uploadHealingOperationsList).isEmpty()
      assertThat(recordingThrottlers.metadataRead.invocationCount).isEqualTo(3)
      assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(0)
    }

  @Test
  fun `cancelling a read waiting for a permit does not issue the RPC`() =
    runBlocking<Unit> {
      val entered = CompletableDeferred<Unit>()
      val release = CompletableDeferred<Unit>()
      val gate =
        object : Throttler {
          override suspend fun <T> onReady(block: suspend () -> T): T {
            entered.complete(Unit)
            release.await()
            return recordingThrottlers.metadataRead.onReady(block)
          }
        }
      val gatedStore =
        GrpcHealingOperationStore(
          PublicServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
            grpcTestServerRule.channel
          ),
          InternalServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
            grpcTestServerRule.channel
          ),
          recordingThrottlers.throttlers.copy(metadataRead = gate),
        )
      val request = async {
        gatedStore.getUploadHealingOperation(getUploadHealingOperationRequest { name = OPERATION })
      }
      entered.await()
      verify(readService, never()).getUploadHealingOperation(any())
      request.cancel()
      assertFailsWith<CancellationException> { request.await() }
      verify(readService, never()).getUploadHealingOperation(any())
    }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp"
    private const val OPERATION_ID = "11111111-1111-4111-8111-111111111111"
    private const val OPERATION = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID"
    private const val STEP = "$OPERATION/uploadHealingSteps/1"
    private const val SOURCE = "$DATA_PROVIDER/rawImpressionUploads/source"
    private const val PREVIOUS = "$DATA_PROVIDER/rawImpressionUploads/previous"
    private const val REPLACEMENT = "$DATA_PROVIDER/rawImpressionUploads/replacement"
    private const val MODEL_LINE_ROW = "$SOURCE/rawImpressionUploadModelLines/row"
    private const val CMMS_MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val CANDIDATE = "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/candidate"
    private const val REQUEST_ID = "22222222-2222-4222-8222-222222222222"
    private val PUBLIC_STEP = uploadHealingStep {
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
      }
    }
    private val PUBLIC_OPERATION = uploadHealingOperation {
      name = OPERATION
      state = UploadHealingOperation.State.EVICTING
      planFormatVersion = 2
      outputManifestState =
        UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_FINALIZED
      reason = "correction"
      rawImpressionUploadCorrectionCandidates += CANDIDATE
      steps += PUBLIC_STEP
    }
  }
}
