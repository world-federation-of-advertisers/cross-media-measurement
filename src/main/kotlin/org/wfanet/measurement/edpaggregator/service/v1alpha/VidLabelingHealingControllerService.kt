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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingStepKey
import org.wfanet.measurement.edpaggregator.service.VidLabelingMachineApiConversions
import org.wfanet.measurement.edpaggregator.service.VidLabelingMachineAuthorizer
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FinalizeUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PurgeRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PurgeRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ReconcileUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingHealingControllerServiceGrpcKt.VidLabelingHealingControllerServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.purgeRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.AdvanceUploadHealingOperationCommand
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.AdvanceUploadHealingStepCommand
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.FinalizeUploadHealingOperationCommand
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.HealingOperationStore
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.ReconcileUploadHealingOperationCommand
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.UploadHealingStepAction
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.UploadHealingStepManifest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.purgeExpiredRawImpressionUploadCorrectionCandidatesRequest

/** System API adapter for authenticated controller-owned healing checkpoints. */
class VidLabelingHealingControllerService(
  private val operationStore: HealingOperationStore,
  private val candidatesStub: RawImpressionUploadCorrectionCandidateServiceCoroutineStub,
  private val authorizer: VidLabelingMachineAuthorizer,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : VidLabelingHealingControllerServiceCoroutineImplBase(coroutineContext) {
  override suspend fun reconcileUploadHealingOperation(
    request: ReconcileUploadHealingOperationRequest
  ): UploadHealingOperation = authorized {
    val parent = VidLabelingMachineApiConversions.parent(request.parent)
    val operationKey = UploadHealingOperationKey(parent, request.uploadHealingOperationId)
    if (request.uploadHealingOperation.name.isNotEmpty())
      require(request.uploadHealingOperation.name == operationKey.toName()) {
        "Draft name does not match its operation"
      }
    validatePlan(parent, operationKey, request.uploadHealingOperation)
    operationStore.reconcileUploadHealingOperation(
      ReconcileUploadHealingOperationCommand(
        request.parent,
        request.uploadHealingOperation,
        request.uploadHealingOperationId,
        request.etag,
        request.requestId,
      )
    )
  }

  override suspend fun advanceUploadHealingOperation(
    request: AdvanceUploadHealingOperationRequest
  ): UploadHealingOperation = authorized {
    requireNotNull(UploadHealingOperationKey.fromName(request.name))
    operationStore.advanceUploadHealingOperation(
      AdvanceUploadHealingOperationCommand(
        request.name,
        request.etag,
        request.state,
        request.requestId,
      )
    )
  }

  override suspend fun finalizeUploadHealingOperation(
    request: FinalizeUploadHealingOperationRequest
  ): UploadHealingOperation = authorized {
    val operation = requireNotNull(UploadHealingOperationKey.fromName(request.name))
    val manifests =
      request.stepManifestsList.map {
        val key = requireNotNull(UploadHealingStepKey.fromName(it.uploadHealingStep))
        require(key.parentKey == operation) { "Manifest step belongs to another operation" }
        UploadHealingStepManifest(it.uploadHealingStep, it.labeledOutputManifest)
      }
    operationStore.finalizeUploadHealingOperation(
      FinalizeUploadHealingOperationCommand(
        request.name,
        manifests,
        request.etag,
        request.requestId,
      )
    )
  }

  override suspend fun advanceUploadHealingStep(
    request: AdvanceUploadHealingStepRequest
  ): UploadHealingStep = authorized {
    val step = requireNotNull(UploadHealingStepKey.fromName(request.name))
    if (request.replacementRawImpressionUpload.isNotEmpty()) {
      require(
        VidLabelingMachineApiConversions.upload(request.replacementRawImpressionUpload).parentKey ==
          step.parentKey.parentKey
      ) {
        "Replacement belongs to another DataProvider"
      }
    }
    val action =
      when (request.action) {
        AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION ->
          UploadHealingStepAction.CONFIRM_EVICTION
        AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY ->
          UploadHealingStepAction.RECORD_RECOVERY
        AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT ->
          UploadHealingStepAction.CONFIRM_REPLACEMENT
        AdvanceUploadHealingStepRequest.Action.ACTION_UNSPECIFIED,
        AdvanceUploadHealingStepRequest.Action.UNRECOGNIZED ->
          throw IllegalArgumentException("action must specify a checkpoint")
      }
    operationStore.advanceUploadHealingStep(
      AdvanceUploadHealingStepCommand(
        request.name,
        request.etag,
        action,
        request.replacementRawImpressionUpload,
        request.recoveryDoneBlobGeneration,
        request.requestId,
      )
    )
  }

  override suspend fun purgeRawImpressionUploadCorrectionCandidates(
    request: PurgeRawImpressionUploadCorrectionCandidatesRequest
  ): PurgeRawImpressionUploadCorrectionCandidatesResponse = authorized {
    val parent = VidLabelingMachineApiConversions.parent(request.parent)
    val result =
      rpcThrottlers.metadataWrite.onReady {
        candidatesStub.purgeExpiredRawImpressionUploadCorrectionCandidates(
          purgeExpiredRawImpressionUploadCorrectionCandidatesRequest {
            dataProviderResourceId = parent.dataProviderId
            pageSize = request.pageSize
          }
        )
      }
    purgeRawImpressionUploadCorrectionCandidatesResponse { purgedCount = result.purgedCount }
  }

  private suspend fun <T> authorized(block: suspend () -> T): T =
    VidLabelingMachineApiServices.authorized(
      authorizer,
      VidLabelingMachineAuthorizer.Role.HEALING_CONTROLLER,
      block,
    )

  private fun validatePlan(
    parent: DataProviderKey,
    operation: UploadHealingOperationKey,
    plan: UploadHealingOperation,
  ) {
    fun checkUpload(name: String) =
      require(VidLabelingMachineApiConversions.upload(name).parentKey == parent) {
        "Plan upload belongs to another DataProvider"
      }
    fun checkCandidate(name: String) =
      require(VidLabelingMachineApiConversions.candidate(name).parentKey == parent) {
        "Plan candidate belongs to another DataProvider"
      }
    plan.badRawImpressionUploadsList.forEach { checkUpload(it) }
    plan.rawImpressionUploadCorrectionCandidatesList.forEach { checkCandidate(it) }
    for (step in plan.stepsList) {
      checkUpload(step.sourceRawImpressionUpload)
      val source = requireNotNull(RawImpressionUploadKey.fromName(step.sourceRawImpressionUpload))
      require(
        requireNotNull(RawImpressionUploadModelLineKey.fromName(step.rawImpressionUploadModelLine))
          .parentKey == source
      ) {
        "Model-line execution belongs to another upload"
      }
      if (step.name.isNotEmpty())
        require(requireNotNull(UploadHealingStepKey.fromName(step.name)).parentKey == operation) {
          "Plan step belongs to another operation"
        }
      if (step.recoveryPredecessorRawImpressionUpload.isNotEmpty())
        checkUpload(step.recoveryPredecessorRawImpressionUpload)
      if (step.replacementRawImpressionUpload.isNotEmpty())
        checkUpload(step.replacementRawImpressionUpload)
      if (step.rawImpressionUploadCorrectionCandidate.isNotEmpty())
        checkCandidate(step.rawImpressionUploadCorrectionCandidate)
    }
  }
}
