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

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadCorrectionCandidateKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingStepKey
import org.wfanet.measurement.edpaggregator.v1alpha.GetUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.getUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifestKt as InternalLabeledOutputManifestKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction as InternalRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation as InternalOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub as InternalOperationStub
import org.wfanet.measurement.internal.edpaggregator.advanceUploadHealingOperationRequest as internalAdvanceOperationRequest
import org.wfanet.measurement.internal.edpaggregator.advanceUploadHealingStepRequest as internalAdvanceStepRequest
import org.wfanet.measurement.internal.edpaggregator.labeledOutputManifest as internalLabeledOutputManifest
import org.wfanet.measurement.internal.edpaggregator.reconcileUploadHealingOperationRequest as internalReconcileOperationRequest
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation as internalOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep as internalStep

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

  suspend fun advanceUploadHealingStep(request: AdvanceUploadHealingStepCommand): UploadHealingStep
}

/** Routes controller mutations to the internal API and reads through the public API. */
class GrpcHealingOperationStore(
  private val readStub: UploadHealingOperationServiceCoroutineStub,
  private val mutationStub: InternalOperationStub,
) : HealingOperationStore {
  override suspend fun reconcileUploadHealingOperation(
    request: ReconcileUploadHealingOperationCommand
  ): UploadHealingOperation {
    val parent = requireNotNull(DataProviderKey.fromName(request.parent))
    mutationStub.reconcileUploadHealingOperation(
      internalReconcileOperationRequest {
        dataProviderResourceId = parent.dataProviderId
        uploadHealingOperationId = request.uploadHealingOperationId
        uploadHealingOperation = request.uploadHealingOperation.toInternal(parent)
        etag = request.etag
        requestId = request.requestId
      }
    )
    return getOperation(parent, request.uploadHealingOperationId)
  }

  override suspend fun getUploadHealingOperation(
    request: GetUploadHealingOperationRequest
  ): UploadHealingOperation = readStub.getUploadHealingOperation(request)

  override suspend fun listUploadHealingOperations(
    request: ListUploadHealingOperationsRequest
  ): ListUploadHealingOperationsResponse = readStub.listUploadHealingOperations(request)

  override suspend fun advanceUploadHealingOperation(
    request: AdvanceUploadHealingOperationCommand
  ): UploadHealingOperation {
    val key = requireNotNull(UploadHealingOperationKey.fromName(request.name))
    mutationStub.advanceUploadHealingOperation(
      internalAdvanceOperationRequest {
        dataProviderResourceId = key.dataProviderId
        uploadHealingOperationId = key.uploadHealingOperationId
        etag = request.etag
        state = request.state.toInternal()
        requestId = request.requestId
      }
    )
    return getOperation(key.parentKey, key.uploadHealingOperationId)
  }

  override suspend fun advanceUploadHealingStep(
    request: AdvanceUploadHealingStepCommand
  ): UploadHealingStep {
    val key = requireNotNull(UploadHealingStepKey.fromName(request.name))
    val stepId = requireNotNull(key.uploadHealingStepId.toLongOrNull())
    val replacementId =
      request.replacementRawImpressionUpload
        .takeIf(String::isNotEmpty)
        ?.let { requireNotNull(RawImpressionUploadKey.fromName(it)).rawImpressionUploadId }
        .orEmpty()
    mutationStub.advanceUploadHealingStep(
      internalAdvanceStepRequest {
        dataProviderResourceId = key.dataProviderId
        uploadHealingOperationId = key.uploadHealingOperationId
        uploadHealingStepId = stepId
        etag = request.etag
        action = request.action.toInternal()
        replacementRawImpressionUploadResourceId = replacementId
        recoveryDoneBlobGeneration = request.recoveryDoneBlobGeneration
        requestId = request.requestId
      }
    )
    return getOperation(key.parentKey.parentKey, key.uploadHealingOperationId).stepsList.single {
      it.name == request.name
    }
  }

  private suspend fun getOperation(
    parent: DataProviderKey,
    operationId: String,
  ): UploadHealingOperation =
    readStub.getUploadHealingOperation(
      getUploadHealingOperationRequest {
        name = UploadHealingOperationKey(parent, operationId).toName()
      }
    )

  private fun UploadHealingOperation.toInternal(parent: DataProviderKey): InternalOperation =
    internalOperation {
      state =
        if (stepsCount == 0) {
          InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
        } else {
          InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
        }
      reason = this@toInternal.reason
      rawImpressionUploadCorrectionCandidateIds +=
        rawImpressionUploadCorrectionCandidatesList.map {
          val key = requireNotNull(RawImpressionUploadCorrectionCandidateKey.fromName(it))
          require(key.parentKey == parent)
          key.rawImpressionUploadCorrectionCandidateId
        }
      steps +=
        stepsList.mapIndexed { index, step ->
          val source =
            requireNotNull(RawImpressionUploadKey.fromName(step.sourceRawImpressionUpload))
          require(source.parentKey == parent)
          val modelLine =
            requireNotNull(
              RawImpressionUploadModelLineKey.fromName(step.rawImpressionUploadModelLine)
            )
          require(modelLine.parentKey == source)
          require(ModelLineKey.fromName(step.cmmsModelLine) != null)
          internalStep {
            uploadHealingStepId = index.toLong() + 1L
            sequenceNumber = step.sequenceNumber
            sourceRawImpressionUploadResourceId = source.rawImpressionUploadId
            rawImpressionUploadModelLineResourceId = modelLine.rawImpressionUploadModelLineId
            cmmsModelLine = step.cmmsModelLine
            memoized = step.memoized
            recoveryAction = step.recoveryAction.toInternal()
            if (step.recoveryPredecessorRawImpressionUpload.isNotEmpty()) {
              recoveryPredecessorRawImpressionUploadResourceId =
                requireNotNull(
                    RawImpressionUploadKey.fromName(step.recoveryPredecessorRawImpressionUpload)
                  )
                  .rawImpressionUploadId
            }
            recoveryTarget = step.recoveryTarget
            labeledOutputManifest = internalLabeledOutputManifest {
              blobs +=
                step.labeledOutputManifest.blobsList.map { blob ->
                  InternalLabeledOutputManifestKt.blobVersion {
                    blobUri = blob.blobUri
                    if (blob.hasGeneration()) generation = blob.generation
                  }
                }
            }
            if (step.rawImpressionUploadCorrectionCandidate.isNotEmpty()) {
              rawImpressionUploadCorrectionCandidateId =
                requireNotNull(
                    RawImpressionUploadCorrectionCandidateKey.fromName(
                      step.rawImpressionUploadCorrectionCandidate
                    )
                  )
                  .rawImpressionUploadCorrectionCandidateId
            }
          }
        }
    }

  private fun UploadHealingOperation.State.toInternal(): InternalOperation.State =
    InternalOperation.State.forNumber(number)

  private fun RawImpressionUploadModelLine.RecoveryAction.toInternal(): InternalRecoveryAction =
    InternalRecoveryAction.forNumber(number)

  private fun UploadHealingStepAction.toInternal():
    org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest.Action =
    when (this) {
      UploadHealingStepAction.CONFIRM_EVICTION ->
        org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest.Action
          .CONFIRM_EVICTION
      UploadHealingStepAction.RECORD_RECOVERY ->
        org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest.Action
          .RECORD_RECOVERY
      UploadHealingStepAction.CONFIRM_REPLACEMENT ->
        org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest.Action
          .CONFIRM_REPLACEMENT
    }
}
