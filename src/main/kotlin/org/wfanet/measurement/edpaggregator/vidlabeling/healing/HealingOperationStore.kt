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

import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FinalizeUploadHealingOperationRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.GetUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifest
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ReconcileUploadHealingOperationRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.advanceUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.advanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.finalizeUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.reconcileUploadHealingOperationRequest

data class ReconcileUploadHealingOperationCommand(
  val parent: String,
  val uploadHealingOperation: UploadHealingOperation,
  val uploadHealingOperationId: String,
  val etag: String = "",
  val requestId: String,
)

data class AdvanceUploadHealingOperationCommand(
  val name: String,
  val etag: String,
  val state: UploadHealingOperation.State,
  val requestId: String,
)

data class UploadHealingStepManifest(
  val uploadHealingStep: String,
  val labeledOutputManifest: LabeledOutputManifest,
)

data class FinalizeUploadHealingOperationCommand(
  val name: String,
  val stepManifests: List<UploadHealingStepManifest>,
  val etag: String,
  val requestId: String,
)

enum class UploadHealingStepAction {
  CONFIRM_EVICTION,
  RECORD_RECOVERY,
  CONFIRM_REPLACEMENT,
}

data class AdvanceUploadHealingStepCommand(
  val name: String,
  val etag: String,
  val action: UploadHealingStepAction,
  val replacementRawImpressionUpload: String = "",
  val recoveryDoneBlobGeneration: Long = 0L,
  val requestId: String,
)

/** Controller access to durable upload-healing operations. */
interface HealingOperationStore {
  suspend fun reconcileUploadHealingOperation(
    request: ReconcileUploadHealingOperationCommand
  ): UploadHealingOperation

  suspend fun getUploadHealingOperation(
    request: GetUploadHealingOperationRequest
  ): UploadHealingOperation

  suspend fun listUploadHealingOperations(
    request: ListUploadHealingOperationsRequest
  ): ListUploadHealingOperationsResponse

  suspend fun advanceUploadHealingOperation(
    request: AdvanceUploadHealingOperationCommand
  ): UploadHealingOperation

  suspend fun finalizeUploadHealingOperation(
    request: FinalizeUploadHealingOperationCommand
  ): UploadHealingOperation

  suspend fun advanceUploadHealingStep(request: AdvanceUploadHealingStepCommand): UploadHealingStep
}

/** Controller access through the public upload-healing API. */
class GrpcHealingOperationStore(
  private val stub: UploadHealingOperationServiceCoroutineStub,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
) : HealingOperationStore {
  override suspend fun reconcileUploadHealingOperation(
    request: ReconcileUploadHealingOperationCommand
  ): UploadHealingOperation =
    rpcThrottlers.metadataWrite.onReady {
      stub
        .reconcileUploadHealingOperation(
          reconcileUploadHealingOperationRequest {
            parent = request.parent
            uploadHealingOperationId = request.uploadHealingOperationId
            plan =
              ReconcileUploadHealingOperationRequestKt.plan {
                val source = request.uploadHealingOperation
                reason = source.reason
                labeledImpressionsBlobPrefix = source.labeledImpressionsBlobPrefix
                rawImpressionUploadCorrectionCandidates +=
                  source.rawImpressionUploadCorrectionCandidatesList
                planFormatVersion = source.planFormatVersion
                outputManifestState = source.outputManifestState
                steps +=
                  source.stepsList.map { step ->
                    ReconcileUploadHealingOperationRequestKt.step {
                      sequenceNumber = step.sequenceNumber
                      sourceRawImpressionUpload = step.sourceRawImpressionUpload
                      rawImpressionUploadModelLine = step.rawImpressionUploadModelLine
                      cmmsModelLine = step.cmmsModelLine
                      memoized = step.memoized
                      recoveryAction = step.recoveryAction
                      recoveryPredecessorRawImpressionUpload =
                        step.recoveryPredecessorRawImpressionUpload
                      recoveryTarget = step.recoveryTarget
                      labeledOutputManifest = step.labeledOutputManifest
                      rawImpressionUploadCorrectionCandidate =
                        step.rawImpressionUploadCorrectionCandidate
                    }
                  }
              }
            etag = request.etag
            requestId = request.requestId
          }
        )
        .uploadHealingOperation
    }

  override suspend fun getUploadHealingOperation(
    request: GetUploadHealingOperationRequest
  ): UploadHealingOperation =
    rpcThrottlers.metadataRead.onReady { stub.getUploadHealingOperation(request) }

  override suspend fun listUploadHealingOperations(
    request: ListUploadHealingOperationsRequest
  ): ListUploadHealingOperationsResponse =
    rpcThrottlers.metadataRead.onReady { stub.listUploadHealingOperations(request) }

  override suspend fun advanceUploadHealingOperation(
    request: AdvanceUploadHealingOperationCommand
  ): UploadHealingOperation =
    rpcThrottlers.metadataWrite.onReady {
      stub.advanceUploadHealingOperation(
        advanceUploadHealingOperationRequest {
          name = request.name
          state = request.state
          etag = request.etag
          requestId = request.requestId
        }
      )
    }

  override suspend fun finalizeUploadHealingOperation(
    request: FinalizeUploadHealingOperationCommand
  ): UploadHealingOperation =
    rpcThrottlers.metadataWrite.onReady {
      stub.finalizeUploadHealingOperation(
        finalizeUploadHealingOperationRequest {
          name = request.name
          stepManifests +=
            request.stepManifests.map { manifest ->
              FinalizeUploadHealingOperationRequestKt.stepManifest {
                uploadHealingStep = manifest.uploadHealingStep
                labeledOutputManifest = manifest.labeledOutputManifest
              }
            }
          etag = request.etag
          requestId = request.requestId
        }
      )
    }

  override suspend fun advanceUploadHealingStep(
    request: AdvanceUploadHealingStepCommand
  ): UploadHealingStep =
    rpcThrottlers.metadataWrite.onReady {
      stub.advanceUploadHealingStep(
        advanceUploadHealingStepRequest {
          name = request.name
          action =
            when (request.action) {
              UploadHealingStepAction.CONFIRM_EVICTION ->
                AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
              UploadHealingStepAction.RECORD_RECOVERY ->
                AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY
              UploadHealingStepAction.CONFIRM_REPLACEMENT ->
                AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT
            }
          replacementRawImpressionUpload = request.replacementRawImpressionUpload
          recoveryDoneBlobGeneration = request.recoveryDoneBlobGeneration
          etag = request.etag
          requestId = request.requestId
        }
      )
    }
}
