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
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingHealingControllerServiceGrpcKt.VidLabelingHealingControllerServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.advanceUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.advanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.finalizeUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.reconcileUploadHealingOperationRequest

/** Controller operations through the protected system API gateway. */
class SystemApiHealingOperationStore(
  private val readStub: UploadHealingOperationServiceCoroutineStub,
  private val machineStub: VidLabelingHealingControllerServiceCoroutineStub,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
) : HealingOperationStore {
  override suspend fun reconcileUploadHealingOperation(
    request: ReconcileUploadHealingOperationCommand
  ): UploadHealingOperation =
    rpcThrottlers.metadataWrite.onReady {
      machineStub.reconcileUploadHealingOperation(
        reconcileUploadHealingOperationRequest {
          parent = request.parent
          uploadHealingOperationId = request.uploadHealingOperationId
          uploadHealingOperation = request.uploadHealingOperation
          etag = request.etag
          requestId = request.requestId
        }
      )
    }

  override suspend fun getUploadHealingOperation(
    request: GetUploadHealingOperationRequest
  ): UploadHealingOperation =
    rpcThrottlers.metadataRead.onReady { readStub.getUploadHealingOperation(request) }

  override suspend fun listUploadHealingOperations(
    request: ListUploadHealingOperationsRequest
  ): ListUploadHealingOperationsResponse =
    rpcThrottlers.metadataRead.onReady { readStub.listUploadHealingOperations(request) }

  override suspend fun advanceUploadHealingOperation(
    request: AdvanceUploadHealingOperationCommand
  ): UploadHealingOperation =
    rpcThrottlers.metadataWrite.onReady {
      machineStub.advanceUploadHealingOperation(
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
      machineStub.finalizeUploadHealingOperation(
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
      machineStub.advanceUploadHealingStep(
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
