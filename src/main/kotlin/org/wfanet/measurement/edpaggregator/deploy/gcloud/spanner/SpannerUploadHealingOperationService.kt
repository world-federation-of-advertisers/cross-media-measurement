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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Options
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import io.grpc.Status
import java.security.MessageDigest
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.UploadHealingOperationResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.approveRawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.assignRawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.completeUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.deleteVidLabelingEvictionFence
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findLatestUploadByDoneBlobUri
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findUploadHealingOperationByMutationRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadModelLineByResourceIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getVidLabelingEvictionFence
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readProcessingDeferredRawImpressionUploadIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRankIndexBlobs
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionUploadCorrectionCandidates
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionUploadModelLines
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readUploadHealingOperations
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.recordUploadHealingOperationMutation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.reopenRawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.replaceUploadHealingOperationPlan
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.touchUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.transferVidLabelingEvictionFence
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.unassignRawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadProcessingDeferred
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateUploadHealingOperationState
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateUploadHealingStep
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateVidLabelingEvictionFenceState
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.internal.edpaggregator.ApproveUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.BlobType
import org.wfanet.measurement.internal.edpaggregator.GetUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsRequest
import org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineFailureReason
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.ReconcileUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.RetryUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.UpdateUploadHealingOperationPlanRequest
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listUploadHealingOperationsPageToken
import org.wfanet.measurement.internal.edpaggregator.listUploadHealingOperationsResponse

/** Spanner-backed persistence for resumable VID-labeling upload healing. */
class SpannerUploadHealingOperationService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : UploadHealingOperationServiceCoroutineImplBase(coroutineContext) {

  override suspend fun reconcileUploadHealingOperation(
    request: ReconcileUploadHealingOperationRequest
  ): UploadHealingOperation {
    val operation = normalizeOperation(request)
    validatePlan(operation)
    require(
      operation.state ==
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED ||
        operation.state ==
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
    ) {
      "a reconciled plan must require approval or attention"
    }
    val requestFingerprint = request.fingerprint()
    databaseClient
      .readWriteTransaction(Options.tag("action=reconcileUploadHealingOperation"))
      .run { txn ->
        val existing =
          txn.findUploadHealingOperation(
            request.dataProviderResourceId,
            request.uploadHealingOperationId,
          )
        if (existing == null) {
          require(request.etag.isEmpty()) { "etag must be empty when creating a plan" }
          syncCandidateAssignments(txn, null, operation)
          txn.insertUploadHealingOperation(operation, request.requestId)
          return@run
        }
        if (existing.reconcileRequestId == request.requestId) {
          if (hasSamePlan(existing.uploadHealingOperation, operation)) return@run
          throw requestIdAlreadyUsed()
        }
        val mutationIndex = existing.mutationRequestIds.indexOf(request.requestId)
        if (mutationIndex >= 0) {
          if (
            existing.mutationRequestFingerprints.getOrNull(mutationIndex) == requestFingerprint &&
              hasSamePlan(existing.uploadHealingOperation, operation)
          ) {
            return@run
          }
          throw requestIdAlreadyUsed()
        }
        precondition(
          existing.uploadHealingOperation.state ==
            UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED ||
            existing.uploadHealingOperation.state ==
              UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION &&
              existing.uploadHealingOperation.resumeState ==
                UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
        ) {
          "only a pre-approval plan can be reconciled"
        }
        requireNotBlank(request.etag, "etag")
        if (existing.uploadHealingOperation.etag != request.etag) {
          throw Status.ABORTED.withDescription("upload_healing_operation etag mismatch")
            .asRuntimeException()
        }
        if (hasSamePlan(existing.uploadHealingOperation, operation)) {
          txn.recordUploadHealingOperationMutation(
            existing.uploadHealingOperation,
            existing.mutationRequestIds + request.requestId,
            existing.mutationRequestFingerprints + listOf(requestFingerprint),
          )
          return@run
        }
        syncCandidateAssignments(txn, existing.uploadHealingOperation, operation)
        txn.replaceUploadHealingOperationPlan(
          operation,
          existing.mutationRequestIds + request.requestId,
          existing.mutationRequestFingerprints + listOf(requestFingerprint),
        )
      }
    return getOperation(request.dataProviderResourceId, request.uploadHealingOperationId)
  }

  override suspend fun getUploadHealingOperation(
    request: GetUploadHealingOperationRequest
  ): UploadHealingOperation {
    requireNotBlank(request.dataProviderResourceId, "data_provider_resource_id")
    requireNotBlank(request.uploadHealingOperationId, "upload_healing_operation_id")
    return getOperation(request.dataProviderResourceId, request.uploadHealingOperationId)
  }

  override suspend fun listUploadHealingOperations(
    request: ListUploadHealingOperationsRequest
  ): ListUploadHealingOperationsResponse {
    requireNotBlank(request.dataProviderResourceId, "data_provider_resource_id")
    require(request.pageSize >= 0) { "page_size must be non-negative" }
    request.filter.stateInList.forEach { state ->
      require(
        state != UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED &&
          state != UploadHealingOperation.State.UNRECOGNIZED
      ) {
        "filter.state_in contains an unspecified state"
      }
    }
    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
    val after =
      if (request.hasPageToken()) {
        require(request.pageToken.dataProviderResourceId == request.dataProviderResourceId) {
          "page_token does not belong to data_provider_resource_id"
        }
        require(request.pageToken.filter == request.filter) { "page_token does not match filter" }
        request.pageToken.after
      } else {
        null
      }
    databaseClient.readOnlyTransaction().use { txn ->
      return listUploadHealingOperationsResponse {
        txn
          .readUploadHealingOperations(
            request.dataProviderResourceId,
            request.filter,
            pageSize + 1,
            after,
          )
          .collectIndexed { index, result ->
            if (index == pageSize) {
              val last = uploadHealingOperations.last()
              nextPageToken = listUploadHealingOperationsPageToken {
                dataProviderResourceId = request.dataProviderResourceId
                filter = request.filter
                this.after =
                  ListUploadHealingOperationsPageTokenKt.after {
                    createTime = last.createTime
                    uploadHealingOperationId = last.uploadHealingOperationId
                  }
              }
            } else {
              uploadHealingOperations += result.uploadHealingOperation
            }
          }
      }
    }
  }

  override suspend fun updateUploadHealingOperationPlan(
    request: UpdateUploadHealingOperationPlanRequest
  ): UploadHealingOperation {
    requireNotBlank(request.dataProviderResourceId, "data_provider_resource_id")
    requireUuid(request.uploadHealingOperationId, "upload_healing_operation_id")
    requireNotBlank(request.etag, "etag")
    require(request.hasUploadHealingOperation()) { "upload_healing_operation is required" }
    val requested =
      request.uploadHealingOperation.copy {
        dataProviderResourceId = request.dataProviderResourceId
        uploadHealingOperationId = request.uploadHealingOperationId
      }
    validatePlan(requested)
    databaseClient
      .readWriteTransaction(Options.tag("action=updateUploadHealingOperationPlan"))
      .run { txn ->
        val existing =
          txn.findUploadHealingOperation(
            request.dataProviderResourceId,
            request.uploadHealingOperationId,
          ) ?: throw notFound(request.uploadHealingOperationId)
        if (hasSamePlan(existing.uploadHealingOperation, requested)) return@run
        precondition(
          existing.uploadHealingOperation.state ==
            UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED ||
            existing.uploadHealingOperation.state ==
              UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION &&
              existing.uploadHealingOperation.resumeState ==
                UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
        ) {
          "only a pre-approval plan can be changed"
        }
        if (existing.uploadHealingOperation.etag != request.etag) {
          throw Status.ABORTED.withDescription("upload_healing_operation etag mismatch")
            .asRuntimeException()
        }
        syncCandidateAssignments(txn, existing.uploadHealingOperation, requested)
        txn.replaceUploadHealingOperationPlan(requested)
      }
    return getOperation(request.dataProviderResourceId, request.uploadHealingOperationId)
  }

  override suspend fun advanceUploadHealingOperation(
    request: AdvanceUploadHealingOperationRequest
  ): UploadHealingOperation {
    requireNotBlank(request.dataProviderResourceId, "data_provider_resource_id")
    requireUuid(request.uploadHealingOperationId, "upload_healing_operation_id")
    requireNotBlank(request.etag, "etag")
    require(
      request.state != UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED &&
        request.state != UploadHealingOperation.State.UNRECOGNIZED
    ) {
      "state is required"
    }
    databaseClient.readWriteTransaction(Options.tag("action=advanceUploadHealingOperation")).run {
      txn ->
      val operation =
        txn
          .findUploadHealingOperation(
            request.dataProviderResourceId,
            request.uploadHealingOperationId,
          )
          ?.uploadHealingOperation ?: throw notFound(request.uploadHealingOperationId)
      if (operation.etag != request.etag) {
        throw Status.ABORTED.withDescription("upload_healing_operation etag mismatch")
          .asRuntimeException()
      }
      if (operation.state == request.state) return@run
      if (
        request.state ==
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
      ) {
        reopenSupersededPlan(txn, operation)
        txn.updateUploadHealingOperationState(
          request.dataProviderResourceId,
          request.uploadHealingOperationId,
          request.state,
        )
        return@run
      }
      requireNoSupersededPlanCandidates(txn, operation)
      precondition(request.state in allowedOperationStates(operation.state)) {
        "cannot advance upload-healing operation from ${operation.state} to ${request.state}"
      }
      val resumeState =
        if (
          request.state ==
            UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
        ) {
          markPlanCandidatesNeedAttention(txn, operation)
          operation.state
        } else {
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
        }
      if (request.state == UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING) {
        updatePlanCandidateState(
          txn,
          operation,
          RawImpressionUploadCorrectionCandidate.State.STATE_HEALING,
        )
      }
      txn.updateUploadHealingOperationState(
        request.dataProviderResourceId,
        request.uploadHealingOperationId,
        request.state,
        resumeState,
      )
    }
    return getOperation(request.dataProviderResourceId, request.uploadHealingOperationId)
  }

  override suspend fun approveUploadHealingOperation(
    request: ApproveUploadHealingOperationRequest
  ): UploadHealingOperation {
    requireNotBlank(request.dataProviderResourceId, "data_provider_resource_id")
    requireUuid(request.uploadHealingOperationId, "upload_healing_operation_id")
    requireNotBlank(request.etag, "etag")
    requireUuid(request.requestId, "request_id")
    require(request.candidateDecisionsCount > 0) { "candidate_decisions is required" }
    val decisions =
      request.candidateDecisionsList.associate { candidateDecision ->
        requireNotBlank(
          candidateDecision.rawImpressionUploadCorrectionCandidateId,
          "candidate_decisions.raw_impression_upload_correction_candidate_id",
        )
        require(
          candidateDecision.decision ==
            RawImpressionUploadCorrectionCandidate.Decision.DECISION_CORRECT ||
            candidateDecision.decision ==
              RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT
        ) {
          "candidate_decisions.decision is required"
        }
        candidateDecision.rawImpressionUploadCorrectionCandidateId to candidateDecision.decision
      }
    require(decisions.size == request.candidateDecisionsCount) {
      "candidate_decisions must identify unique candidates"
    }
    val requestFingerprint = request.fingerprint()
    databaseClient.readWriteTransaction(Options.tag("action=approveUploadHealingOperation")).run {
      txn ->
      val existingByRequestId =
        txn.findUploadHealingOperationByMutationRequestId(
          request.dataProviderResourceId,
          request.requestId,
        )
      if (existingByRequestId != null) {
        if (
          isIdempotentMutation(
            existingByRequestId,
            request.uploadHealingOperationId,
            request.requestId,
            requestFingerprint,
          )
        ) {
          return@run
        }
        throw requestIdAlreadyUsed()
      }
      val result =
        txn.findUploadHealingOperation(
          request.dataProviderResourceId,
          request.uploadHealingOperationId,
        ) ?: throw notFound(request.uploadHealingOperationId)
      val operation = result.uploadHealingOperation
      precondition(
        operation.state ==
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
      ) {
        "only a plan awaiting approval can be approved"
      }
      if (operation.etag != request.etag) {
        throw Status.ABORTED.withDescription("upload_healing_operation etag mismatch")
          .asRuntimeException()
      }
      val unassignedCandidate =
        txn
          .readRawImpressionUploadCorrectionCandidates(
            request.dataProviderResourceId,
            ListRawImpressionUploadCorrectionCandidatesRequestKt.filter {
              stateIn += RawImpressionUploadCorrectionCandidate.State.STATE_PENDING
            },
            limit = 1,
          )
          .firstOrNull()
      precondition(unassignedCandidate == null) {
        "pending correction candidates must be added to the plan before approval"
      }
      precondition(
        decisions.keys == operation.rawImpressionUploadCorrectionCandidateIdsList.toSet()
      ) {
        "candidate_decisions must contain every plan candidate exactly once"
      }
      val approvedPlan = finalizeDecisionPlan(operation, decisions)
      validateDecisionPlan(approvedPlan, decisions)
      val candidates = readPlanCandidates(txn, operation)
      precondition(
        candidates.all {
          it.uploadHealingOperationId == operation.uploadHealingOperationId &&
            it.state == RawImpressionUploadCorrectionCandidate.State.STATE_PLANNED
        }
      ) {
        "every correction candidate must still belong to the draft plan"
      }
      for (candidate in candidates) {
        txn.approveRawImpressionUploadCorrectionCandidate(
          candidate,
          decisions.getValue(candidate.rawImpressionUploadCorrectionCandidateId),
        )
      }
      txn.replaceUploadHealingOperationPlan(
        approvedPlan,
        result.mutationRequestIds + request.requestId,
        result.mutationRequestFingerprints + listOf(requestFingerprint),
      )
    }
    return getOperation(request.dataProviderResourceId, request.uploadHealingOperationId)
  }

  override suspend fun retryUploadHealingOperation(
    request: RetryUploadHealingOperationRequest
  ): UploadHealingOperation {
    requireNotBlank(request.dataProviderResourceId, "data_provider_resource_id")
    requireUuid(request.uploadHealingOperationId, "upload_healing_operation_id")
    requireNotBlank(request.etag, "etag")
    requireUuid(request.requestId, "request_id")
    val requestFingerprint = request.fingerprint()
    databaseClient.readWriteTransaction(Options.tag("action=retryUploadHealingOperation")).run { txn
      ->
      val existingByRequestId =
        txn.findUploadHealingOperationByMutationRequestId(
          request.dataProviderResourceId,
          request.requestId,
        )
      if (existingByRequestId != null) {
        if (
          isIdempotentMutation(
            existingByRequestId,
            request.uploadHealingOperationId,
            request.requestId,
            requestFingerprint,
          )
        ) {
          return@run
        }
        throw requestIdAlreadyUsed()
      }
      val result =
        txn.findUploadHealingOperation(
          request.dataProviderResourceId,
          request.uploadHealingOperationId,
        ) ?: throw notFound(request.uploadHealingOperationId)
      val operation = result.uploadHealingOperation
      precondition(
        operation.state ==
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
      ) {
        "only an operation needing attention can be retried"
      }
      precondition(
        operation.resumeState !=
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
      ) {
        "the operation requires replanning instead of retry"
      }
      if (operation.etag != request.etag) {
        throw Status.ABORTED.withDescription("upload_healing_operation etag mismatch")
          .asRuntimeException()
      }
      restorePlanCandidateState(txn, operation, operation.resumeState)
      txn.updateUploadHealingOperationState(
        request.dataProviderResourceId,
        request.uploadHealingOperationId,
        operation.resumeState,
        mutationRequestIds = result.mutationRequestIds + request.requestId,
        mutationRequestFingerprints =
          result.mutationRequestFingerprints + listOf(requestFingerprint),
      )
    }
    return getOperation(request.dataProviderResourceId, request.uploadHealingOperationId)
  }

  override suspend fun advanceUploadHealingStep(
    request: AdvanceUploadHealingStepRequest
  ): UploadHealingStep {
    requireNotBlank(request.dataProviderResourceId, "data_provider_resource_id")
    requireNotBlank(request.uploadHealingOperationId, "upload_healing_operation_id")
    require(request.uploadHealingStepId > 0L) { "upload_healing_step_id must be positive" }
    requireNotBlank(request.etag, "etag")
    requireUuid(request.requestId, "request_id")
    require(
      request.action != AdvanceUploadHealingStepRequest.Action.ACTION_UNSPECIFIED &&
        request.action != AdvanceUploadHealingStepRequest.Action.UNRECOGNIZED
    ) {
      "action is required"
    }
    when (request.action) {
      AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION -> {
        require(request.replacementRawImpressionUploadResourceId.isEmpty()) {
          "replacement_raw_impression_upload_resource_id must be empty for CONFIRM_EVICTION"
        }
        require(request.recoveryDoneBlobGeneration == 0L) {
          "recovery_done_blob_generation must be zero for CONFIRM_EVICTION"
        }
      }
      AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY -> {
        require(request.replacementRawImpressionUploadResourceId.isEmpty()) {
          "replacement_raw_impression_upload_resource_id must be empty for RECORD_RECOVERY"
        }
        require(request.recoveryDoneBlobGeneration > 0L) {
          "recovery_done_blob_generation must be positive for RECORD_RECOVERY"
        }
      }
      AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT -> {
        require(request.replacementRawImpressionUploadResourceId.isNotEmpty()) {
          "replacement_raw_impression_upload_resource_id is required for CONFIRM_REPLACEMENT"
        }
        require(request.recoveryDoneBlobGeneration == 0L) {
          "recovery_done_blob_generation must be zero for CONFIRM_REPLACEMENT"
        }
      }
      AdvanceUploadHealingStepRequest.Action.ACTION_UNSPECIFIED,
      AdvanceUploadHealingStepRequest.Action.UNRECOGNIZED -> error("action was validated")
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=advanceUploadHealingStep"))
    transactionRunner.run { txn ->
      val result =
        txn.findUploadHealingOperation(
          request.dataProviderResourceId,
          request.uploadHealingOperationId,
        ) ?: throw notFound(request.uploadHealingOperationId)
      val operation = result.uploadHealingOperation
      requireNoSupersededPlanCandidates(txn, operation)
      val current =
        operation.stepsList.firstOrNull { it.uploadHealingStepId == request.uploadHealingStepId }
          ?: throw Status.NOT_FOUND.withDescription(
              "UploadHealingStep ${request.uploadHealingStepId} not found"
            )
            .asRuntimeException()
      if (isIdempotentReplay(current, request)) {
        return@run
      }
      precondition(
        operation.state in
          setOf(
            UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING,
            UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING,
            UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING,
          )
      ) {
        "upload-healing steps cannot advance while the operation is ${operation.state}"
      }
      if (current.etag != request.etag) {
        throw Status.ABORTED.withDescription("upload_healing_step etag mismatch")
          .asRuntimeException()
      }
      val nextState =
        when (request.action) {
          AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION -> {
            precondition(
              current.state == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION
            ) {
              "eviction can only be confirmed from PENDING_EVICTION"
            }
            validateEviction(txn, request, current)
            if (current.recoveryTarget) {
              UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT
            } else {
              UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE
            }
          }
          AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY -> {
            precondition(
              current.state ==
                UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT
            ) {
              "recovery can only start from WAITING_FOR_REPLACEMENT"
            }
            validateRecoveryStart(operation, current, request)
            UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED
          }
          AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT -> {
            precondition(
              current.state ==
                UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT ||
                current.state == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED
            ) {
              "replacement can only be confirmed while waiting for recovery"
            }
            validateReplacement(txn, operation, current, request)
            UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE
          }
          AdvanceUploadHealingStepRequest.Action.ACTION_UNSPECIFIED,
          AdvanceUploadHealingStepRequest.Action.UNRECOGNIZED -> error("action was validated")
        }
      txn.updateUploadHealingStep(
        request.dataProviderResourceId,
        request.uploadHealingOperationId,
        request.uploadHealingStepId,
        current.state,
        nextState,
        request.replacementRawImpressionUploadResourceId,
        request.recoveryDoneBlobGeneration,
        request.requestId,
      )
      if (
        nextState == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE &&
          operation.stepsList.all {
            it.uploadHealingStepId == request.uploadHealingStepId ||
              it.state == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE
          }
      ) {
        txn.completeUploadHealingOperation(
          request.dataProviderResourceId,
          request.uploadHealingOperationId,
        )
        completePlanCandidates(txn, operation)
        releasePlanFence(txn, operation)
      } else {
        txn.touchUploadHealingOperation(
          request.dataProviderResourceId,
          request.uploadHealingOperationId,
        )
      }
    }
    return getOperation(request.dataProviderResourceId, request.uploadHealingOperationId)
      .stepsList
      .single { it.uploadHealingStepId == request.uploadHealingStepId }
  }

  private suspend fun getOperation(
    dataProviderResourceId: String,
    operationId: String,
  ): UploadHealingOperation =
    databaseClient.readOnlyTransaction().use { readContext ->
      readContext
        .findUploadHealingOperation(dataProviderResourceId, operationId)
        ?.uploadHealingOperation ?: throw notFound(operationId)
    }

  private fun normalizeOperation(
    request: ReconcileUploadHealingOperationRequest
  ): UploadHealingOperation {
    requireNotBlank(request.dataProviderResourceId, "data_provider_resource_id")
    requireUuid(request.uploadHealingOperationId, "upload_healing_operation_id")
    requireUuid(request.requestId, "request_id")
    require(request.hasUploadHealingOperation()) { "upload_healing_operation is required" }
    return request.uploadHealingOperation.copy {
      dataProviderResourceId = request.dataProviderResourceId
      uploadHealingOperationId = request.uploadHealingOperationId
    }
  }

  private fun validatePlan(operation: UploadHealingOperation) {
    val allowedStates =
      setOf(
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION,
      )
    require(operation.state in allowedStates) {
      "upload_healing_operation.state is not valid for a new plan"
    }
    require(operation.reason.isNotBlank()) { "upload_healing_operation.reason is required" }
    require(
      operation.resumeState ==
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
    ) {
      "upload_healing_operation.resume_state is output-only"
    }
    require(
      operation.stepsList.isNotEmpty() ||
        operation.state ==
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
    ) {
      "upload_healing_operation.steps is required"
    }
    require(
      operation.rawImpressionUploadCorrectionCandidateIdsList.distinct().size ==
        operation.rawImpressionUploadCorrectionCandidateIdsCount
    ) {
      "upload_healing_operation.raw_impression_upload_correction_candidate_ids must be unique"
    }
    if (
      operation.state ==
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED ||
        operation.state ==
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
    ) {
      require(operation.rawImpressionUploadCorrectionCandidateIdsCount > 0) {
        "upload_healing_operation.raw_impression_upload_correction_candidate_ids is required"
      }
    }
    if (
      operation.state ==
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
    ) {
      val candidateIdsOwnedBySteps =
        operation.stepsList
          .map { it.rawImpressionUploadCorrectionCandidateId }
          .filterTo(mutableSetOf()) { it.isNotEmpty() }
      require(
        candidateIdsOwnedBySteps == operation.rawImpressionUploadCorrectionCandidateIdsList.toSet()
      ) {
        "every correction candidate must own at least one healing step"
      }
    }
    require(
      operation.stepsList.map { it.uploadHealingStepId }.distinct().size == operation.stepsCount
    ) {
      "upload_healing_operation.steps must have unique IDs"
    }
    require(operation.stepsList.map { it.sequenceNumber }.distinct().size == operation.stepsCount) {
      "upload_healing_operation.steps must have unique sequence numbers"
    }
    for (step in operation.stepsList) {
      require(step.uploadHealingStepId > 0L) { "upload healing step IDs must be positive" }
      require(step.sequenceNumber >= 0L) { "step sequence numbers must be non-negative" }
      require(step.sourceRawImpressionUploadResourceId.isNotBlank()) {
        "step source_raw_impression_upload_resource_id is required"
      }
      require(step.rawImpressionUploadModelLineResourceId.isNotBlank()) {
        "step raw_impression_upload_model_line_resource_id is required"
      }
      require(step.cmmsModelLine.isNotBlank()) { "step cmms_model_line is required" }
      require(
        step.recoveryAction !=
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_UNSPECIFIED
      ) {
        "step recovery_action is required"
      }
      require(
        step.rawImpressionUploadCorrectionCandidateId.isEmpty() ||
          step.rawImpressionUploadCorrectionCandidateId in
            operation.rawImpressionUploadCorrectionCandidateIdsList
      ) {
        "step correction candidate must belong to the operation"
      }
    }
  }

  private fun hasSamePlan(
    existing: UploadHealingOperation,
    requested: UploadHealingOperation,
  ): Boolean {
    if (
      existing.reason != requested.reason ||
        existing.state != requested.state ||
        existing.resumeState != requested.resumeState ||
        existing.rawImpressionUploadCorrectionCandidateIdsList !=
          requested.rawImpressionUploadCorrectionCandidateIdsList ||
        existing.stepsCount != requested.stepsCount
    ) {
      return false
    }
    return existing.stepsList.zip(requested.stepsList).all { (left, right) ->
      left.uploadHealingStepId == right.uploadHealingStepId &&
        left.sequenceNumber == right.sequenceNumber &&
        left.sourceRawImpressionUploadResourceId == right.sourceRawImpressionUploadResourceId &&
        left.rawImpressionUploadModelLineResourceId ==
          right.rawImpressionUploadModelLineResourceId &&
        left.cmmsModelLine == right.cmmsModelLine &&
        left.memoized == right.memoized &&
        left.recoveryAction == right.recoveryAction &&
        left.recoveryPredecessorRawImpressionUploadResourceId ==
          right.recoveryPredecessorRawImpressionUploadResourceId &&
        left.recoveryTarget == right.recoveryTarget &&
        left.rawImpressionUploadCorrectionCandidateId ==
          right.rawImpressionUploadCorrectionCandidateId
    }
  }

  private suspend fun syncCandidateAssignments(
    txn: AsyncDatabaseClient.TransactionContext,
    previousOperation: UploadHealingOperation?,
    operation: UploadHealingOperation,
  ) {
    val previousCandidateIds =
      previousOperation?.rawImpressionUploadCorrectionCandidateIdsList.orEmpty()
    val previousCandidateState = previousOperation?.let { candidateState(it.state) }
    val nextCandidateIds = operation.rawImpressionUploadCorrectionCandidateIdsList.toSet()
    for (candidateId in previousCandidateIds.filter { it !in nextCandidateIds }) {
      val candidate =
        txn
          .findRawImpressionUploadCorrectionCandidate(operation.dataProviderResourceId, candidateId)
          ?.rawImpressionUploadCorrectionCandidate ?: throw candidateNotFound(candidateId)
      if (
        candidate.uploadHealingOperationId == operation.uploadHealingOperationId &&
          candidate.state == RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED
      ) {
        continue
      }
      precondition(
        candidate.uploadHealingOperationId == operation.uploadHealingOperationId &&
          candidate.state == previousCandidateState
      ) {
        "correction candidate $candidateId is not assigned to this mutable plan"
      }
      txn.unassignRawImpressionUploadCorrectionCandidate(candidate)
    }
    val candidateState = candidateState(operation.state)
    for (candidateId in nextCandidateIds) {
      val candidate =
        txn
          .findRawImpressionUploadCorrectionCandidate(operation.dataProviderResourceId, candidateId)
          ?.rawImpressionUploadCorrectionCandidate ?: throw candidateNotFound(candidateId)
      precondition(
        candidate.state == RawImpressionUploadCorrectionCandidate.State.STATE_PENDING ||
          (candidate.uploadHealingOperationId == operation.uploadHealingOperationId &&
            (candidate.state == candidateState || candidate.state == previousCandidateState))
      ) {
        "correction candidate $candidateId is already assigned or no longer pending"
      }
      if (
        candidate.state != candidateState ||
          candidate.uploadHealingOperationId != operation.uploadHealingOperationId
      ) {
        txn.assignRawImpressionUploadCorrectionCandidate(
          candidate,
          operation.uploadHealingOperationId,
          candidateState,
        )
      }
    }
  }

  private fun candidateState(
    operationState: UploadHealingOperation.State
  ): RawImpressionUploadCorrectionCandidate.State =
    if (
      operationState == UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
    ) {
      RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED
    } else {
      RawImpressionUploadCorrectionCandidate.State.STATE_PLANNED
    }

  private suspend fun readPlanCandidates(
    txn: AsyncDatabaseClient.TransactionContext,
    operation: UploadHealingOperation,
  ): List<RawImpressionUploadCorrectionCandidate> {
    precondition(operation.rawImpressionUploadCorrectionCandidateIdsCount > 0) {
      "an approval requires correction candidates"
    }
    return operation.rawImpressionUploadCorrectionCandidateIdsList.map { candidateId ->
      txn
        .findRawImpressionUploadCorrectionCandidate(operation.dataProviderResourceId, candidateId)
        ?.rawImpressionUploadCorrectionCandidate ?: throw candidateNotFound(candidateId)
    }
  }

  private suspend fun requireNoSupersededPlanCandidates(
    txn: AsyncDatabaseClient.TransactionContext,
    operation: UploadHealingOperation,
  ) {
    if (operation.rawImpressionUploadCorrectionCandidateIdsCount == 0) return
    precondition(
      readPlanCandidates(txn, operation).none {
        it.state == RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED
      }
    ) {
      "the healing plan contains a superseded correction candidate"
    }
  }

  private suspend fun markPlanCandidatesNeedAttention(
    txn: AsyncDatabaseClient.TransactionContext,
    operation: UploadHealingOperation,
  ) {
    for (candidateId in operation.rawImpressionUploadCorrectionCandidateIdsList) {
      val candidate =
        txn
          .findRawImpressionUploadCorrectionCandidate(operation.dataProviderResourceId, candidateId)
          ?.rawImpressionUploadCorrectionCandidate ?: throw candidateNotFound(candidateId)
      precondition(candidate.uploadHealingOperationId == operation.uploadHealingOperationId) {
        "correction candidate $candidateId no longer belongs to this plan"
      }
      txn.assignRawImpressionUploadCorrectionCandidate(
        candidate,
        operation.uploadHealingOperationId,
        RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED,
      )
    }
  }

  private suspend fun updatePlanCandidateState(
    txn: AsyncDatabaseClient.TransactionContext,
    operation: UploadHealingOperation,
    state: RawImpressionUploadCorrectionCandidate.State,
  ) {
    for (candidateId in operation.rawImpressionUploadCorrectionCandidateIdsList) {
      val candidate =
        txn
          .findRawImpressionUploadCorrectionCandidate(operation.dataProviderResourceId, candidateId)
          ?.rawImpressionUploadCorrectionCandidate ?: throw candidateNotFound(candidateId)
      precondition(candidate.uploadHealingOperationId == operation.uploadHealingOperationId) {
        "correction candidate $candidateId no longer belongs to this plan"
      }
      txn.assignRawImpressionUploadCorrectionCandidate(
        candidate,
        operation.uploadHealingOperationId,
        state,
      )
    }
  }

  private suspend fun completePlanCandidates(
    txn: AsyncDatabaseClient.TransactionContext,
    operation: UploadHealingOperation,
  ) {
    for (candidateId in operation.rawImpressionUploadCorrectionCandidateIdsList) {
      val candidate =
        txn
          .findRawImpressionUploadCorrectionCandidate(operation.dataProviderResourceId, candidateId)
          ?.rawImpressionUploadCorrectionCandidate ?: throw candidateNotFound(candidateId)
      val state =
        when (candidate.decision) {
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_CORRECT ->
            RawImpressionUploadCorrectionCandidate.State.STATE_COMPLETE
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT ->
            RawImpressionUploadCorrectionCandidate.State.STATE_NO_REPLACEMENT
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED,
          RawImpressionUploadCorrectionCandidate.Decision.UNRECOGNIZED ->
            error("completed plan candidate has no decision")
        }
      txn.assignRawImpressionUploadCorrectionCandidate(
        candidate,
        operation.uploadHealingOperationId,
        state,
      )
    }
  }

  private suspend fun releasePlanFence(
    txn: AsyncDatabaseClient.TransactionContext,
    operation: UploadHealingOperation,
  ) {
    val fence = txn.getVidLabelingEvictionFence(operation.dataProviderResourceId) ?: return
    precondition(fence.evictionOperationId == operation.uploadHealingOperationId) {
      "another healing operation owns the DataProvider fence"
    }
    val nextCandidate =
      txn
        .readRawImpressionUploadCorrectionCandidates(
          operation.dataProviderResourceId,
          ListRawImpressionUploadCorrectionCandidatesRequestKt.filter {
            stateIn += RawImpressionUploadCorrectionCandidate.State.STATE_PENDING
          },
          limit = 1,
        )
        .firstOrNull()
        ?.rawImpressionUploadCorrectionCandidate
    if (nextCandidate != null) {
      txn.transferVidLabelingEvictionFence(
        operation.dataProviderResourceId,
        nextCandidate.rawImpressionUploadCorrectionCandidateId,
      )
      return
    }
    txn.readProcessingDeferredRawImpressionUploadIds(operation.dataProviderResourceId).collect {
      rawImpressionUploadId ->
      txn.updateRawImpressionUploadProcessingDeferred(
        operation.dataProviderResourceId,
        rawImpressionUploadId,
        false,
      )
    }
    txn.deleteVidLabelingEvictionFence(operation.dataProviderResourceId)
  }

  private suspend fun restorePlanCandidateState(
    txn: AsyncDatabaseClient.TransactionContext,
    operation: UploadHealingOperation,
    state: UploadHealingOperation.State,
  ) {
    val candidateState =
      when (state) {
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED ->
          RawImpressionUploadCorrectionCandidate.State.STATE_PLANNED
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING ->
          RawImpressionUploadCorrectionCandidate.State.STATE_APPROVED
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING ->
          RawImpressionUploadCorrectionCandidate.State.STATE_HEALING
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE,
        UploadHealingOperation.State.UNRECOGNIZED -> error("invalid resume state")
      }
    for (candidateId in operation.rawImpressionUploadCorrectionCandidateIdsList) {
      val candidate =
        txn
          .findRawImpressionUploadCorrectionCandidate(operation.dataProviderResourceId, candidateId)
          ?.rawImpressionUploadCorrectionCandidate ?: throw candidateNotFound(candidateId)
      precondition(
        candidate.uploadHealingOperationId == operation.uploadHealingOperationId &&
          candidate.state ==
            RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED
      ) {
        "correction candidate $candidateId cannot resume with this plan"
      }
      txn.assignRawImpressionUploadCorrectionCandidate(
        candidate,
        operation.uploadHealingOperationId,
        candidateState,
      )
    }
  }

  private suspend fun reopenSupersededPlan(
    txn: AsyncDatabaseClient.TransactionContext,
    operation: UploadHealingOperation,
  ) {
    precondition(operation.state in SUPERSEDED_PLAN_REOPEN_STATES) {
      "only an approved or active plan can be reopened"
    }
    val candidates = readPlanCandidates(txn, operation)
    precondition(
      candidates.any { it.state == RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED }
    ) {
      "the plan has no superseded correction candidate"
    }
    for (candidate in candidates) {
      if (candidate.state != RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED) {
        txn.reopenRawImpressionUploadCorrectionCandidate(candidate)
      }
    }
    val fence = txn.getVidLabelingEvictionFence(operation.dataProviderResourceId)
    if (fence != null) {
      precondition(fence.evictionOperationId == operation.uploadHealingOperationId) {
        "another healing operation owns the DataProvider fence"
      }
      txn.updateVidLabelingEvictionFenceState(
        operation.dataProviderResourceId,
        org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
          .VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING,
      )
    }
  }

  private fun finalizeDecisionPlan(
    operation: UploadHealingOperation,
    decisions: Map<String, RawImpressionUploadCorrectionCandidate.Decision>,
  ): UploadHealingOperation {
    val noReplacementCandidateIds =
      decisions
        .filterValues {
          it == RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT
        }
        .keys
    if (noReplacementCandidateIds.isEmpty()) {
      return operation.copy {
        state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED
      }
    }
    val rewiredSteps = mutableMapOf<Long, UploadHealingStep>()
    for (modelLineSteps in
      operation.stepsList.filter { it.memoized }.groupBy { it.cmmsModelLine }.values) {
      val orderedSteps = modelLineSteps.sortedBy { it.sequenceNumber }
      var predecessor = orderedSteps.first().recoveryPredecessorRawImpressionUploadResourceId
      for (step in orderedSteps) {
        val removed = step.rawImpressionUploadCorrectionCandidateId in noReplacementCandidateIds
        rewiredSteps[step.uploadHealingStepId] =
          step.copy {
            recoveryPredecessorRawImpressionUploadResourceId = predecessor
            if (removed) {
              recoveryAction =
                RawImpressionUploadModelLineRecoveryAction
                  .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT
              recoveryTarget = false
            }
          }
        if (!removed && step.recoveryTarget) {
          predecessor = step.sourceRawImpressionUploadResourceId
        }
      }
    }
    return operation.copy {
      state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED
      steps.clear()
      steps +=
        operation.stepsList.map { step ->
          rewiredSteps[step.uploadHealingStepId]
            ?: if (step.rawImpressionUploadCorrectionCandidateId in noReplacementCandidateIds) {
              step.copy {
                recoveryAction =
                  RawImpressionUploadModelLineRecoveryAction
                    .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT
                recoveryPredecessorRawImpressionUploadResourceId = ""
                recoveryTarget = false
              }
            } else {
              step
            }
        }
    }
  }

  private fun validateDecisionPlan(
    operation: UploadHealingOperation,
    decisions: Map<String, RawImpressionUploadCorrectionCandidate.Decision>,
  ) {
    for ((candidateId, decision) in decisions) {
      val noReplacement =
        decision == RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT
      val expectedAction =
        if (noReplacement) {
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT
        } else {
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION
        }
      val ownerSteps =
        operation.stepsList.filter { it.rawImpressionUploadCorrectionCandidateId == candidateId }
      precondition(
        ownerSteps.isNotEmpty() &&
          ownerSteps.all { step ->
            step.recoveryAction == expectedAction && (!noReplacement || !step.recoveryTarget)
          } &&
          (noReplacement || ownerSteps.any { it.recoveryTarget })
      ) {
        "the healing plan does not match the decision for candidate $candidateId"
      }
    }
  }

  private fun allowedOperationStates(
    state: UploadHealingOperation.State
  ): Set<UploadHealingOperation.State> =
    when (state) {
      UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED ->
        setOf(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION)
      UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED ->
        setOf(
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING,
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION,
        )
      UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING ->
        setOf(
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING,
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION,
        )
      UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING ->
        setOf(
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING,
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING,
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION,
        )
      UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING ->
        setOf(
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING,
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION,
        )
      UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING ->
        setOf(
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING,
          UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION,
        )
      UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION -> emptySet()
      UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE -> emptySet()
      UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED,
      UploadHealingOperation.State.UNRECOGNIZED -> error("invalid upload-healing operation state")
    }

  private fun isIdempotentReplay(
    current: UploadHealingStep,
    request: AdvanceUploadHealingStepRequest,
  ): Boolean =
    when (request.action) {
      AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION ->
        current.state != UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION
      AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY ->
        (current.state == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED ||
          current.state == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE) &&
          current.recoveryDoneBlobGeneration == request.recoveryDoneBlobGeneration
      AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT ->
        current.state == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE &&
          current.replacementRawImpressionUploadResourceId ==
            request.replacementRawImpressionUploadResourceId
      AdvanceUploadHealingStepRequest.Action.ACTION_UNSPECIFIED,
      AdvanceUploadHealingStepRequest.Action.UNRECOGNIZED -> false
    }

  private suspend fun validateEviction(
    txn: AsyncDatabaseClient.TransactionContext,
    request: AdvanceUploadHealingStepRequest,
    current: UploadHealingStep,
  ) {
    val modelLine =
      txn
        .getRawImpressionUploadModelLineByResourceIds(
          request.dataProviderResourceId,
          current.sourceRawImpressionUploadResourceId,
          current.rawImpressionUploadModelLineResourceId,
        )
        ?.rawImpressionUploadModelLine
        ?: throw Status.FAILED_PRECONDITION.withDescription("evicted model-line row was not found")
          .asRuntimeException()
    precondition(
      modelLine.state ==
        RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED &&
        modelLine.failureReason ==
          RawImpressionUploadModelLineFailureReason
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_EVICTED_OUTPUT &&
        modelLine.evictionOperationId == request.uploadHealingOperationId
    ) {
      "source model line has not been evicted by this healing operation"
    }
  }

  private fun validateRecoveryStart(
    operation: UploadHealingOperation,
    current: UploadHealingStep,
    request: AdvanceUploadHealingStepRequest,
  ) {
    precondition(
      current.recoveryAction in
        setOf(
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION,
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY,
        )
    ) {
      "only replacement steps can record a replay"
    }
    precondition(request.recoveryDoneBlobGeneration > 0L) {
      "recovery_done_blob_generation must be positive"
    }
    validatePredecessor(operation, current)
  }

  private suspend fun validateReplacement(
    txn: AsyncDatabaseClient.TransactionContext,
    operation: UploadHealingOperation,
    current: UploadHealingStep,
    request: AdvanceUploadHealingStepRequest,
  ) {
    precondition(request.replacementRawImpressionUploadResourceId.isNotBlank()) {
      "replacement_raw_impression_upload_resource_id is required"
    }
    validatePredecessor(operation, current)

    val source =
      txn
        .getRawImpressionUploadByResourceId(
          request.dataProviderResourceId,
          current.sourceRawImpressionUploadResourceId,
        )
        .rawImpressionUpload
    val replacementResult =
      txn.getRawImpressionUploadByResourceId(
        request.dataProviderResourceId,
        request.replacementRawImpressionUploadResourceId,
      )
    val replacement = replacementResult.rawImpressionUpload
    precondition(replacement.doneBlobUri == source.doneBlobUri) {
      "replacement does not use the source done-object path"
    }
    val latest =
      txn
        .findLatestUploadByDoneBlobUri(request.dataProviderResourceId, source.doneBlobUri)
        ?.rawImpressionUpload
    precondition(
      latest?.rawImpressionUploadResourceId == request.replacementRawImpressionUploadResourceId
    ) {
      "replacement is not the latest upload revision"
    }
    val inPlaceRecovery =
      replacement.rawImpressionUploadResourceId == current.sourceRawImpressionUploadResourceId &&
        current.recoveryAction ==
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY &&
        current.recoveryDoneBlobGeneration == source.doneBlobGeneration
    precondition(
      inPlaceRecovery ||
        replacesUpload(
          txn,
          request.dataProviderResourceId,
          replacement,
          current.sourceRawImpressionUploadResourceId,
        )
    ) {
      "replacement does not descend from the source upload"
    }
    precondition(
      replacement.registrationComplete &&
        replacement.state == RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED
    ) {
      "replacement upload has not completed registration and processing"
    }
    if (
      current.recoveryAction !=
        RawImpressionUploadModelLineRecoveryAction
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT
    ) {
      precondition(
        current.recoveryDoneBlobGeneration > 0L &&
          replacement.doneBlobGeneration == current.recoveryDoneBlobGeneration
      ) {
        "replacement does not match the recorded recovery generation"
      }
    }

    val replacementModelLine =
      txn
        .readRawImpressionUploadModelLines(
          request.dataProviderResourceId,
          replacement.rawImpressionUploadResourceId,
          ListRawImpressionUploadModelLinesRequest.Filter.newBuilder()
            .setCmmsModelLine(current.cmmsModelLine)
            .build(),
          limit = 1,
        )
        .firstOrNull()
        ?.rawImpressionUploadModelLine
        ?: throw Status.FAILED_PRECONDITION.withDescription("replacement model line was not found")
          .asRuntimeException()
    precondition(
      replacementModelLine.cmmsModelLine == current.cmmsModelLine &&
        replacementModelLine.state ==
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
    ) {
      "replacement model line has not completed"
    }
    if (current.memoized) {
      val snapshot =
        txn
          .readRankIndexBlobs(
            request.dataProviderResourceId,
            replacement.rawImpressionUploadResourceId,
            ListRankIndexBlobsRequest.Filter.newBuilder()
              .setBlobType(BlobType.BLOB_TYPE_SNAPSHOT)
              .setCmmsModelLine(current.cmmsModelLine)
              .build(),
            showDeleted = false,
            limit = 1,
          )
          .firstOrNull()
      precondition(snapshot != null) { "replacement has no active memoized snapshot" }
    }
  }

  private fun validatePredecessor(operation: UploadHealingOperation, current: UploadHealingStep) {
    val predecessorId = current.recoveryPredecessorRawImpressionUploadResourceId
    if (predecessorId.isEmpty()) return
    val predecessorSteps =
      operation.stepsList.filter {
        it.sourceRawImpressionUploadResourceId == predecessorId && it.recoveryTarget
      }
    precondition(
      predecessorSteps.isEmpty() ||
        predecessorSteps.all {
          it.state == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE
        }
    ) {
      "the predecessor upload has not completed recovery"
    }
  }

  private suspend fun replacesUpload(
    txn: AsyncDatabaseClient.TransactionContext,
    dataProviderResourceId: String,
    candidate: org.wfanet.measurement.internal.edpaggregator.RawImpressionUpload,
    sourceResourceId: String,
  ): Boolean {
    val visited = mutableSetOf<String>()
    var predecessorId = candidate.replacesRawImpressionUploadResourceId
    while (predecessorId.isNotEmpty() && visited.add(predecessorId)) {
      if (predecessorId == sourceResourceId) return true
      predecessorId =
        txn
          .getRawImpressionUploadByResourceId(dataProviderResourceId, predecessorId)
          .rawImpressionUpload
          .replacesRawImpressionUploadResourceId
    }
    return false
  }

  private fun precondition(condition: Boolean, message: () -> String) {
    if (!condition) {
      throw Status.FAILED_PRECONDITION.withDescription(message()).asRuntimeException()
    }
  }

  private fun requireNotBlank(value: String, field: String) {
    require(value.isNotBlank()) { "$field is required" }
  }

  private fun requireUuid(value: String, field: String) {
    requireNotBlank(value, field)
    try {
      val uuid = UUID.fromString(value)
      require(
        uuid.version() == 4 &&
          uuid.variant() == 2 &&
          uuid.toString().equals(value, ignoreCase = true)
      ) {
        "$field must be a UUID4"
      }
    } catch (e: IllegalArgumentException) {
      throw IllegalArgumentException("$field must be a UUID4", e)
    }
  }

  private fun ApproveUploadHealingOperationRequest.fingerprint(): ByteString =
    MessageDigest.getInstance("SHA-256").digest(toByteArray()).toByteString()

  private fun RetryUploadHealingOperationRequest.fingerprint(): ByteString =
    MessageDigest.getInstance("SHA-256").digest(toByteArray()).toByteString()

  private fun ReconcileUploadHealingOperationRequest.fingerprint(): ByteString =
    MessageDigest.getInstance("SHA-256")
      .digest(toBuilder().clearEtag().build().toByteArray())
      .toByteString()

  private fun isIdempotentMutation(
    result: UploadHealingOperationResult,
    uploadHealingOperationId: String,
    requestId: String,
    requestFingerprint: ByteString,
  ): Boolean {
    if (result.uploadHealingOperation.uploadHealingOperationId != uploadHealingOperationId) {
      return false
    }
    val requestIndex = result.mutationRequestIds.indexOf(requestId)
    return requestIndex >= 0 &&
      result.mutationRequestFingerprints.getOrNull(requestIndex) == requestFingerprint
  }

  private fun requestIdAlreadyUsed() =
    Status.ALREADY_EXISTS.withDescription("request_id was already used for another mutation")
      .asRuntimeException()

  private fun notFound(operationId: String) =
    Status.NOT_FOUND.withDescription("UploadHealingOperation $operationId not found")
      .asRuntimeException()

  private fun candidateNotFound(candidateId: String) =
    Status.NOT_FOUND.withDescription(
        "RawImpressionUploadCorrectionCandidate $candidateId not found"
      )
      .asRuntimeException()

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
    private val SUPERSEDED_PLAN_REOPEN_STATES =
      setOf(
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION,
      )
  }
}
