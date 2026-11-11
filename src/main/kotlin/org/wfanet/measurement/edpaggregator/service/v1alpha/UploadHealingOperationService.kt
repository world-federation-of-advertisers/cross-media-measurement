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

import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadCorrectionCandidateKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingStepKey
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ApproveUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FinalizeUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifest
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.ReconcileUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ReconcileUploadHealingOperationResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RetryUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.labeledOutputManifest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.reconcileUploadHealingOperationResponse
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest as InternalAdvanceStepRequest
import org.wfanet.measurement.internal.edpaggregator.FinalizeUploadHealingOperationRequestKt as InternalFinalizeKt
import org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifest as InternalLabeledOutputManifest
import org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifestKt as InternalManifestKt
import org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsPageToken as InternalListOperationsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsRequestKt as InternalListOperationsRequestKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction as InternalRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation as InternalOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub as InternalOperationStub
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep as InternalStep
import org.wfanet.measurement.internal.edpaggregator.advanceUploadHealingOperationRequest as internalAdvanceRequest
import org.wfanet.measurement.internal.edpaggregator.advanceUploadHealingStepRequest as internalAdvanceStepRequest
import org.wfanet.measurement.internal.edpaggregator.approveUploadHealingOperationRequest as internalApproveOperationRequest
import org.wfanet.measurement.internal.edpaggregator.finalizeUploadHealingOperationRequest as internalFinalizeRequest
import org.wfanet.measurement.internal.edpaggregator.getUploadHealingOperationRequest as internalGetOperationRequest
import org.wfanet.measurement.internal.edpaggregator.labeledOutputManifest as internalManifest
import org.wfanet.measurement.internal.edpaggregator.listUploadHealingOperationsRequest as internalListOperationsRequest
import org.wfanet.measurement.internal.edpaggregator.reconcileUploadHealingOperationRequest as internalReconcileRequest
import org.wfanet.measurement.internal.edpaggregator.retryUploadHealingOperationRequest as internalRetryOperationRequest
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation as internalOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep as internalStep

/** Public API adapter for durable upload-healing workflows. */
class UploadHealingOperationService(
  private val internalOperationStub: InternalOperationStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : UploadHealingOperationServiceCoroutineImplBase(coroutineContext) {

  override suspend fun reconcileUploadHealingOperation(
    request: ReconcileUploadHealingOperationRequest
  ): ReconcileUploadHealingOperationResponse {
    val parent = parseDataProvider(request.parent, "parent")
    validateUuid(request.uploadHealingOperationId, "upload_healing_operation_id")
    validateUuid(request.requestId, "request_id")
    if (!request.hasPlan()) required("plan")
    validatePlanInputs(request.plan)
    val plan = request.plan.toInternal(parent)
    val operation =
      try {
        internalOperationStub
          .reconcileUploadHealingOperation(
            internalReconcileRequest {
              dataProviderResourceId = parent.dataProviderId
              uploadHealingOperationId = request.uploadHealingOperationId
              uploadHealingOperation = plan
              etag = request.etag
              requestId = request.requestId
            }
          )
          .toPublic()
      } catch (e: StatusException) {
        throw translateInternalError(e, "UploadHealingOperation could not be reconciled")
      }
    return reconcileUploadHealingOperationResponse { uploadHealingOperation = operation }
  }

  override suspend fun advanceUploadHealingOperation(
    request: AdvanceUploadHealingOperationRequest
  ): UploadHealingOperation {
    val key = parseOperation(request.name)
    validateUuid(key.uploadHealingOperationId, "name")
    if (request.etag.isBlank()) required("etag")
    validateUuid(request.requestId, "request_id")
    if (
      request.state == UploadHealingOperation.State.STATE_UNSPECIFIED ||
        request.state == UploadHealingOperation.State.UNRECOGNIZED
    )
      invalid("state")
    return try {
      internalOperationStub
        .advanceUploadHealingOperation(
          internalAdvanceRequest {
            dataProviderResourceId = key.dataProviderId
            uploadHealingOperationId = key.uploadHealingOperationId
            state = request.state.toInternal()
            etag = request.etag
            requestId = request.requestId
          }
        )
        .toPublic()
    } catch (e: StatusException) {
      throw translateInternalError(
        e,
        "UploadHealingOperation ${request.name} could not be advanced",
      )
    }
  }

  override suspend fun finalizeUploadHealingOperation(
    request: FinalizeUploadHealingOperationRequest
  ): UploadHealingOperation {
    val key = parseOperation(request.name)
    validateUuid(key.uploadHealingOperationId, "name")
    if (request.etag.isBlank()) required("etag")
    validateUuid(request.requestId, "request_id")
    if (request.stepManifestsList.isEmpty()) required("step_manifests")
    val manifests =
      request.stepManifestsList.map { manifest ->
        val step =
          UploadHealingStepKey.fromName(manifest.uploadHealingStep)
            ?: invalid("step_manifests.upload_healing_step")
        if (step.parentKey != key) invalid("step_manifests.upload_healing_step")
        val stepId =
          step.uploadHealingStepId.toLongOrNull()?.takeIf { it > 0L }
            ?: invalid("step_manifests.upload_healing_step")
        if (!manifest.hasLabeledOutputManifest()) required("step_manifests.labeled_output_manifest")
        InternalFinalizeKt.stepManifest {
          uploadHealingStepId = stepId
          labeledOutputManifest = manifest.labeledOutputManifest.toInternal()
        }
      }
    if (manifests.map { it.uploadHealingStepId }.distinct().size != manifests.size)
      invalid("step_manifests")
    return try {
      internalOperationStub
        .finalizeUploadHealingOperation(
          internalFinalizeRequest {
            dataProviderResourceId = key.dataProviderId
            uploadHealingOperationId = key.uploadHealingOperationId
            stepManifests += manifests
            etag = request.etag
            requestId = request.requestId
          }
        )
        .toPublic()
    } catch (e: StatusException) {
      throw translateInternalError(
        e,
        "UploadHealingOperation ${request.name} could not be finalized",
      )
    }
  }

  override suspend fun advanceUploadHealingStep(
    request: AdvanceUploadHealingStepRequest
  ): UploadHealingStep {
    if (request.name.isEmpty()) required("name")
    val key = UploadHealingStepKey.fromName(request.name) ?: invalid("name")
    val stepId = key.uploadHealingStepId.toLongOrNull()?.takeIf { it > 0L } ?: invalid("name")
    if (request.etag.isBlank()) required("etag")
    validateUuid(request.requestId, "request_id")
    val action =
      when (request.action) {
        AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION ->
          InternalAdvanceStepRequest.Action.CONFIRM_EVICTION
        AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY ->
          InternalAdvanceStepRequest.Action.RECORD_RECOVERY
        AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT ->
          InternalAdvanceStepRequest.Action.CONFIRM_REPLACEMENT
        AdvanceUploadHealingStepRequest.Action.ACTION_UNSPECIFIED,
        AdvanceUploadHealingStepRequest.Action.UNRECOGNIZED -> invalid("action")
      }
    val replacement =
      request.replacementRawImpressionUpload
        .takeIf { it.isNotEmpty() }
        ?.let {
          parseUploadForParent(it, key.parentKey.parentKey, "replacement_raw_impression_upload")
        }
    when (request.action) {
      AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION -> {
        if (replacement != null) invalid("replacement_raw_impression_upload")
        if (request.recoveryDoneBlobGeneration != 0L) invalid("recovery_done_blob_generation")
      }
      AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY -> {
        if (replacement != null) invalid("replacement_raw_impression_upload")
        if (request.recoveryDoneBlobGeneration <= 0L) invalid("recovery_done_blob_generation")
      }
      AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT -> {
        if (replacement == null) required("replacement_raw_impression_upload")
        if (request.recoveryDoneBlobGeneration != 0L) invalid("recovery_done_blob_generation")
      }
      AdvanceUploadHealingStepRequest.Action.ACTION_UNSPECIFIED,
      AdvanceUploadHealingStepRequest.Action.UNRECOGNIZED -> invalid("action")
    }
    return try {
      internalOperationStub
        .advanceUploadHealingStep(
          internalAdvanceStepRequest {
            dataProviderResourceId = key.dataProviderId
            uploadHealingOperationId = key.uploadHealingOperationId
            uploadHealingStepId = stepId
            this.action = action
            replacementRawImpressionUploadResourceId = replacement?.rawImpressionUploadId.orEmpty()
            recoveryDoneBlobGeneration = request.recoveryDoneBlobGeneration
            etag = request.etag
            requestId = request.requestId
          }
        )
        .toPublic(key.parentKey)
    } catch (e: StatusException) {
      throw translateInternalError(e, "UploadHealingStep ${request.name} could not be advanced")
    }
  }

  private fun validatePlanInputs(plan: ReconcileUploadHealingOperationRequest.Plan) {
    if (plan.reason.isBlank()) required("plan.reason")
    if (plan.labeledImpressionsBlobPrefix.isBlank())
      required("plan.labeled_impressions_blob_prefix")
    if (plan.badRawImpressionUploadsList.isEmpty()) required("plan.bad_raw_impression_uploads")
    if (!plan.hasCutoffTime()) required("plan.cutoff_time")
    if (!Timestamps.isValid(plan.cutoffTime)) invalid("plan.cutoff_time")
    if (plan.planFormatVersion !in 1..2) invalid("plan.plan_format_version")
    val expectedManifestState =
      if (plan.planFormatVersion == 2)
        UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_PROVISIONAL
      else UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_FINALIZED
    if (plan.outputManifestState != expectedManifestState) invalid("plan.output_manifest_state")
    if (plan.rawImpressionUploadCorrectionCandidatesList.isEmpty())
      required("plan.raw_impression_upload_correction_candidates")
    if (
      plan.rawImpressionUploadCorrectionCandidatesList.distinct().size !=
        plan.rawImpressionUploadCorrectionCandidatesCount
    )
      invalid("plan.raw_impression_upload_correction_candidates")
    if (plan.stepsList.map { it.sequenceNumber }.distinct().size != plan.stepsCount)
      invalid("plan.steps.sequence_number")
    val sharedVersions = mutableMapOf<String, Long?>()
    for (step in plan.stepsList) {
      if (step.sequenceNumber < 0L) invalid("plan.steps.sequence_number")
      if (
        step.rawImpressionUploadCorrectionCandidate.isNotEmpty() &&
          step.rawImpressionUploadCorrectionCandidate !in
            plan.rawImpressionUploadCorrectionCandidatesList
      )
        invalid("plan.steps.raw_impression_upload_correction_candidate")
      if (!step.hasLabeledOutputManifest() || step.labeledOutputManifest.blobsList.isEmpty())
        required("plan.steps.labeled_output_manifest")
      val blobs = step.labeledOutputManifest.blobsList
      if (blobs.map { it.blobUri }.distinct().size != blobs.size)
        invalid("plan.steps.labeled_output_manifest.blobs")
      for (blob in blobs) {
        if (blob.blobUri.isBlank()) required("plan.steps.labeled_output_manifest.blob_uri")
        if (blob.hasGeneration() && blob.generation <= 0L)
          invalid("plan.steps.labeled_output_manifest.generation")
        if (plan.planFormatVersion == 2 && blob.hasGeneration())
          invalid("plan.steps.labeled_output_manifest.generation")
        val generation = if (blob.hasGeneration()) blob.generation else null
        if (sharedVersions.containsKey(blob.blobUri) && sharedVersions[blob.blobUri] != generation)
          invalid("plan.steps.labeled_output_manifest.generation")
        sharedVersions[blob.blobUri] = generation
      }
    }
  }

  private fun ReconcileUploadHealingOperationRequest.Plan.toInternal(
    parent: DataProviderKey
  ): InternalOperation = internalOperation {
    state =
      if (stepsCount == 0) InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
      else InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
    reason = this@toInternal.reason
    labeledImpressionsBlobPrefix = this@toInternal.labeledImpressionsBlobPrefix
    badRawImpressionUploadResourceIds +=
      this@toInternal.badRawImpressionUploadsList.map {
        parseUploadForParent(it, parent, "plan.bad_raw_impression_uploads").rawImpressionUploadId
      }
    if (this@toInternal.hasCutoffTime()) cutoffTime = this@toInternal.cutoffTime
    rawImpressionUploadCorrectionCandidateIds +=
      this@toInternal.rawImpressionUploadCorrectionCandidatesList.map {
        parseCandidateForParent(it, parent, "plan.raw_impression_upload_correction_candidates")
          .rawImpressionUploadCorrectionCandidateId
      }
    planFormatVersion = this@toInternal.planFormatVersion
    if (
      this@toInternal.outputManifestState ==
        UploadHealingOperation.OutputManifestState.OUTPUT_MANIFEST_STATE_UNSPECIFIED ||
        this@toInternal.outputManifestState ==
          UploadHealingOperation.OutputManifestState.UNRECOGNIZED
    )
      invalid("plan.output_manifest_state")
    outputManifestState =
      InternalOperation.OutputManifestState.forNumber(this@toInternal.outputManifestState.number)
    steps +=
      this@toInternal.stepsList.mapIndexed { index, step ->
        val source =
          parseUploadForParent(
            step.sourceRawImpressionUpload,
            parent,
            "plan.steps.source_raw_impression_upload",
          )
        val modelLine =
          RawImpressionUploadModelLineKey.fromName(step.rawImpressionUploadModelLine)
            ?: invalid("plan.steps.raw_impression_upload_model_line")
        if (modelLine.parentKey != source) invalid("plan.steps.raw_impression_upload_model_line")
        if (ModelLineKey.fromName(step.cmmsModelLine) == null) invalid("plan.steps.cmms_model_line")
        if (
          step.recoveryAction ==
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_UNSPECIFIED ||
            step.recoveryAction == RawImpressionUploadModelLine.RecoveryAction.UNRECOGNIZED
        )
          invalid("plan.steps.recovery_action")
        val predecessor =
          step.recoveryPredecessorRawImpressionUpload
            .takeIf { it.isNotEmpty() }
            ?.let {
              parseUploadForParent(
                it,
                parent,
                "plan.steps.recovery_predecessor_raw_impression_upload",
              )
            }
        val candidate =
          step.rawImpressionUploadCorrectionCandidate
            .takeIf { it.isNotEmpty() }
            ?.let {
              parseCandidateForParent(
                it,
                parent,
                "plan.steps.raw_impression_upload_correction_candidate",
              )
            }
        internalStep {
          uploadHealingStepId = index.toLong() + 1L
          sequenceNumber = step.sequenceNumber
          sourceRawImpressionUploadResourceId = source.rawImpressionUploadId
          rawImpressionUploadModelLineResourceId = modelLine.rawImpressionUploadModelLineId
          cmmsModelLine = step.cmmsModelLine
          memoized = step.memoized
          recoveryAction = InternalRecoveryAction.forNumber(step.recoveryAction.number)
          recoveryPredecessorRawImpressionUploadResourceId =
            predecessor?.rawImpressionUploadId.orEmpty()
          recoveryTarget = step.recoveryTarget
          labeledOutputManifest = step.labeledOutputManifest.toInternal()
          rawImpressionUploadCorrectionCandidateId =
            candidate?.rawImpressionUploadCorrectionCandidateId.orEmpty()
        }
      }
  }

  private fun LabeledOutputManifest.toInternal(): InternalLabeledOutputManifest {
    if (blobsList.isEmpty()) required("labeled_output_manifest.blobs")
    if (blobsList.map { it.blobUri }.distinct().size != blobsCount)
      invalid("labeled_output_manifest.blobs")
    for (blob in blobsList) {
      if (blob.blobUri.isBlank()) required("labeled_output_manifest.blob_uri")
      if (blob.hasGeneration() && blob.generation <= 0L)
        invalid("labeled_output_manifest.generation")
    }
    return internalManifest {
      blobs +=
        this@toInternal.blobsList.map { blob ->
          InternalManifestKt.blobVersion {
            blobUri = blob.blobUri
            if (blob.hasGeneration()) generation = blob.generation
          }
        }
    }
  }

  private fun parseUploadForParent(
    name: String,
    parent: DataProviderKey,
    field: String,
  ): RawImpressionUploadKey {
    val key = RawImpressionUploadKey.fromName(name) ?: invalid(field)
    if (key.parentKey != parent) invalid(field)
    return key
  }

  private fun parseCandidateForParent(
    name: String,
    parent: DataProviderKey,
    field: String,
  ): RawImpressionUploadCorrectionCandidateKey {
    val key = RawImpressionUploadCorrectionCandidateKey.fromName(name) ?: invalid(field)
    if (key.parentKey != parent) invalid(field)
    return key
  }

  override suspend fun getUploadHealingOperation(
    request: GetUploadHealingOperationRequest
  ): UploadHealingOperation {
    if (request.name.isBlank()) required("name")
    val key = UploadHealingOperationKey.fromName(request.name) ?: invalid("name")
    return try {
      internalOperationStub
        .getUploadHealingOperation(
          internalGetOperationRequest {
            dataProviderResourceId = key.dataProviderId
            uploadHealingOperationId = key.uploadHealingOperationId
          }
        )
        .toPublic()
    } catch (e: StatusException) {
      throw translateInternalError(e, "UploadHealingOperation ${request.name} was not found")
    }
  }

  override suspend fun listUploadHealingOperations(
    request: ListUploadHealingOperationsRequest
  ): ListUploadHealingOperationsResponse {
    val dataProviderKey = parseDataProvider(request.parent, "parent")
    if (request.pageSize < 0) invalid("page_size")
    request.filter.stateInList.forEach { state ->
      if (
        state == UploadHealingOperation.State.STATE_UNSPECIFIED ||
          state == UploadHealingOperation.State.UNRECOGNIZED
      ) {
        invalid("filter.state_in")
      }
    }
    val internalPageToken =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListOperationsPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: java.io.IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
    val response =
      try {
        internalOperationStub.listUploadHealingOperations(
          internalListOperationsRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
            pageSize =
              if (request.pageSize == 0) DEFAULT_PAGE_SIZE
              else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
            if (internalPageToken != null) pageToken = internalPageToken
            if (request.hasFilter()) {
              filter =
                InternalListOperationsRequestKt.filter {
                  stateIn += request.filter.stateInList.map { it.toInternal() }
                }
            }
          }
        )
      } catch (e: StatusException) {
        throw translateInternalError(e, "UploadHealingOperations could not be listed")
      }
    return listUploadHealingOperationsResponse {
      uploadHealingOperations += response.uploadHealingOperationsList.map { it.toPublic() }
      if (response.hasNextPageToken()) {
        nextPageToken = response.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun approveUploadHealingOperation(
    request: ApproveUploadHealingOperationRequest
  ): UploadHealingOperation {
    val key = parseOperation(request.name)
    if (request.etag.isBlank()) required("etag")
    validateUuid(request.requestId, "request_id")
    if (request.candidateDecisionsList.isEmpty()) required("candidate_decisions")
    val candidateDecisions =
      request.candidateDecisionsList.map { candidateDecision ->
        val candidateKey =
          RawImpressionUploadCorrectionCandidateKey.fromName(
            candidateDecision.rawImpressionUploadCorrectionCandidate
          ) ?: invalid("candidate_decisions.raw_impression_upload_correction_candidate")
        if (candidateKey.parentKey != key.parentKey) {
          invalid("candidate_decisions.raw_impression_upload_correction_candidate")
        }
        if (
          candidateDecision.decision !=
            RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE &&
            candidateDecision.decision !=
              RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
        ) {
          invalid("candidate_decisions.decision")
        }
        org.wfanet.measurement.internal.edpaggregator.ApproveUploadHealingOperationRequestKt
          .candidateDecision {
            rawImpressionUploadCorrectionCandidateId =
              candidateKey.rawImpressionUploadCorrectionCandidateId
            decision = candidateDecision.decision.toInternal()
          }
      }
    if (
      candidateDecisions.map { it.rawImpressionUploadCorrectionCandidateId }.distinct().size !=
        candidateDecisions.size
    ) {
      invalid("candidate_decisions")
    }
    return try {
      internalOperationStub
        .approveUploadHealingOperation(
          internalApproveOperationRequest {
            dataProviderResourceId = key.dataProviderId
            uploadHealingOperationId = key.uploadHealingOperationId
            etag = request.etag
            this.candidateDecisions += candidateDecisions
            requestId = request.requestId
          }
        )
        .toPublic()
    } catch (e: StatusException) {
      throw translateInternalError(
        e,
        "UploadHealingOperation ${request.name} could not be approved",
      )
    }
  }

  override suspend fun retryUploadHealingOperation(
    request: RetryUploadHealingOperationRequest
  ): UploadHealingOperation {
    val key = parseOperation(request.name)
    if (request.etag.isBlank()) required("etag")
    validateUuid(request.requestId, "request_id")
    return try {
      internalOperationStub
        .retryUploadHealingOperation(
          internalRetryOperationRequest {
            dataProviderResourceId = key.dataProviderId
            uploadHealingOperationId = key.uploadHealingOperationId
            etag = request.etag
            requestId = request.requestId
          }
        )
        .toPublic()
    } catch (e: StatusException) {
      throw translateInternalError(e, "UploadHealingOperation ${request.name} could not be retried")
    }
  }

  private fun InternalOperation.toPublic(): UploadHealingOperation {
    val operationKey = UploadHealingOperationKey(dataProviderResourceId, uploadHealingOperationId)
    return uploadHealingOperation {
      name = operationKey.toName()
      state = this@toPublic.state.toPublic()
      planFormatVersion = this@toPublic.planFormatVersion
      outputManifestState =
        UploadHealingOperation.OutputManifestState.forNumber(
          this@toPublic.outputManifestState.number
        )
      if (
        this@toPublic.resumeState !=
          InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
      ) {
        resumeState = this@toPublic.resumeState.toPublic()
      }
      reason = this@toPublic.reason
      labeledImpressionsBlobPrefix = this@toPublic.labeledImpressionsBlobPrefix
      badRawImpressionUploads +=
        this@toPublic.badRawImpressionUploadResourceIdsList.map {
          RawImpressionUploadKey(dataProviderResourceId, it).toName()
        }
      cutoffTime = this@toPublic.cutoffTime
      labeledImpressionsBlobPrefix = this@toPublic.labeledImpressionsBlobPrefix
      rawImpressionUploadCorrectionCandidates +=
        rawImpressionUploadCorrectionCandidateIdsList.map {
          RawImpressionUploadCorrectionCandidateKey(dataProviderResourceId, it).toName()
        }
      steps += this@toPublic.stepsList.map { it.toPublic(operationKey) }
      createTime = this@toPublic.createTime
      updateTime = this@toPublic.updateTime
      etag = this@toPublic.etag
    }
  }

  private fun InternalStep.toPublic(operationKey: UploadHealingOperationKey): UploadHealingStep =
    uploadHealingStep {
      name = UploadHealingStepKey(operationKey, uploadHealingStepId.toString()).toName()
      sequenceNumber = this@toPublic.sequenceNumber
      val sourceKey =
        RawImpressionUploadKey(operationKey.dataProviderId, sourceRawImpressionUploadResourceId)
      sourceRawImpressionUpload = sourceKey.toName()
      rawImpressionUploadModelLine =
        RawImpressionUploadModelLineKey(sourceKey, rawImpressionUploadModelLineResourceId).toName()
      cmmsModelLine = this@toPublic.cmmsModelLine
      memoized = this@toPublic.memoized
      recoveryAction = this@toPublic.recoveryAction.toPublic()
      if (recoveryPredecessorRawImpressionUploadResourceId.isNotEmpty()) {
        recoveryPredecessorRawImpressionUpload =
          RawImpressionUploadKey(
              operationKey.dataProviderId,
              recoveryPredecessorRawImpressionUploadResourceId,
            )
            .toName()
      }
      recoveryTarget = this@toPublic.recoveryTarget
      labeledOutputManifest = this@toPublic.labeledOutputManifest.toPublic()
      if (rawImpressionUploadCorrectionCandidateId.isNotEmpty()) {
        rawImpressionUploadCorrectionCandidate =
          RawImpressionUploadCorrectionCandidateKey(
              operationKey.dataProviderId,
              rawImpressionUploadCorrectionCandidateId,
            )
            .toName()
      }
      state = this@toPublic.state.toPublic()
      if (replacementRawImpressionUploadResourceId.isNotEmpty()) {
        replacementRawImpressionUpload =
          RawImpressionUploadKey(
              operationKey.dataProviderId,
              replacementRawImpressionUploadResourceId,
            )
            .toName()
      }
      if (this@toPublic.hasEvictionCompleteTime()) {
        evictionCompleteTime = this@toPublic.evictionCompleteTime
      }
      if (this@toPublic.hasRecoveryStartTime()) {
        recoveryStartTime = this@toPublic.recoveryStartTime
      }
      if (this@toPublic.hasCompleteTime()) completeTime = this@toPublic.completeTime
      updateTime = this@toPublic.updateTime
      etag = this@toPublic.etag
      recoveryDoneBlobGeneration = this@toPublic.recoveryDoneBlobGeneration
    }

  private fun InternalLabeledOutputManifest.toPublic(): LabeledOutputManifest =
    labeledOutputManifest {
      blobs +=
        this@toPublic.blobsList.map { blob ->
          LabeledOutputManifestKt.blobVersion {
            blobUri = blob.blobUri
            if (blob.hasGeneration()) generation = blob.generation
          }
        }
    }

  private fun parseDataProvider(value: String, field: String): DataProviderKey {
    if (value.isBlank()) required(field)
    return DataProviderKey.fromName(value) ?: invalid(field)
  }

  private fun parseOperation(value: String): UploadHealingOperationKey {
    if (value.isBlank()) required("name")
    return UploadHealingOperationKey.fromName(value) ?: invalid("name")
  }

  private fun validateUuid(value: String, field: String) {
    if (value.isBlank()) required(field)
    try {
      val uuid = UUID.fromString(value)
      if (
        uuid.version() != 4 ||
          uuid.variant() != 2 ||
          !uuid.toString().equals(value, ignoreCase = true)
      ) {
        invalid(field)
      }
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException(field, e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private fun required(field: String): Nothing =
    throw RequiredFieldNotSetException(field).asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

  private fun invalid(field: String): Nothing =
    throw InvalidFieldValueException(field).asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

  private fun translateInternalError(
    exception: StatusException,
    publicDescription: String,
  ): StatusRuntimeException {
    val publicCode =
      when (exception.status.code) {
        Status.Code.ALREADY_EXISTS,
        Status.Code.NOT_FOUND,
        Status.Code.INVALID_ARGUMENT,
        Status.Code.ABORTED,
        Status.Code.FAILED_PRECONDITION,
        Status.Code.UNAVAILABLE,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.RESOURCE_EXHAUSTED -> exception.status.code
        else -> Status.Code.INTERNAL
      }
    return Status.fromCode(publicCode)
      .withDescription(publicDescription)
      .withCause(exception)
      .asRuntimeException()
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}

private fun UploadHealingOperation.State.toInternal(): InternalOperation.State =
  when (this) {
    UploadHealingOperation.State.APPROVAL_REQUIRED ->
      InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
    UploadHealingOperation.State.APPROVED ->
      InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED
    UploadHealingOperation.State.DRAINING ->
      InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING
    UploadHealingOperation.State.EVICTING ->
      InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING
    UploadHealingOperation.State.REPLAYING ->
      InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING
    UploadHealingOperation.State.RECOVERING ->
      InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING
    UploadHealingOperation.State.NEEDS_ATTENTION ->
      InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
    UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING ->
      InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_AVAILABILITY_HANDOFF_PENDING
    UploadHealingOperation.State.COMPLETE ->
      InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE
    UploadHealingOperation.State.STATE_UNSPECIFIED,
    UploadHealingOperation.State.UNRECOGNIZED ->
      InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
  }

private fun RawImpressionUploadCorrectionCandidate.Decision.toInternal():
  InternalCandidate.Decision =
  when (this) {
    RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE ->
      InternalCandidate.Decision.DECISION_APPLY_CANDIDATE
    RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT ->
      InternalCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
    RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED,
    RawImpressionUploadCorrectionCandidate.Decision.UNRECOGNIZED ->
      InternalCandidate.Decision.DECISION_UNSPECIFIED
  }

private fun InternalRecoveryAction.toPublic(): RawImpressionUploadModelLine.RecoveryAction =
  when (this) {
    InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION ->
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
    InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY ->
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
    InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT ->
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT
    InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_UNSPECIFIED,
    InternalRecoveryAction.UNRECOGNIZED ->
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_UNSPECIFIED
  }

private fun InternalOperation.State.toPublic(): UploadHealingOperation.State =
  when (this) {
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED ->
      UploadHealingOperation.State.APPROVAL_REQUIRED
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED ->
      UploadHealingOperation.State.APPROVED
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING ->
      UploadHealingOperation.State.DRAINING
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING ->
      UploadHealingOperation.State.EVICTING
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING ->
      UploadHealingOperation.State.REPLAYING
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING ->
      UploadHealingOperation.State.RECOVERING
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION ->
      UploadHealingOperation.State.NEEDS_ATTENTION
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_AVAILABILITY_HANDOFF_PENDING ->
      UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE ->
      UploadHealingOperation.State.COMPLETE
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED,
    InternalOperation.State.UNRECOGNIZED -> UploadHealingOperation.State.STATE_UNSPECIFIED
  }

private fun InternalStep.State.toPublic(): UploadHealingStep.State =
  when (this) {
    InternalStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION ->
      UploadHealingStep.State.PENDING_EVICTION
    InternalStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT ->
      UploadHealingStep.State.WAITING_FOR_REPLACEMENT
    InternalStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED ->
      UploadHealingStep.State.RECOVERY_STARTED
    InternalStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE -> UploadHealingStep.State.COMPLETE
    InternalStep.State.UPLOAD_HEALING_STEP_STATE_UNSPECIFIED,
    InternalStep.State.UNRECOGNIZED -> UploadHealingStep.State.STATE_UNSPECIFIED
  }
