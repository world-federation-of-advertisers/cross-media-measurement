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
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.completeUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findLatestUploadByDoneBlobUri
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadModelLineByResourceIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRankIndexBlobs
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionUploadModelLines
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.touchUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateUploadHealingStep
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.internal.edpaggregator.BlobType
import org.wfanet.measurement.internal.edpaggregator.CreateUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.GetUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.ListRankIndexBlobsRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineFailureReason
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep

/** Spanner-backed persistence for resumable VID-labeling upload healing. */
class SpannerUploadHealingOperationService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : UploadHealingOperationServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createUploadHealingOperation(
    request: CreateUploadHealingOperationRequest
  ): UploadHealingOperation {
    validateCreateRequest(request)
    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createUploadHealingOperation"))
    transactionRunner.run { txn ->
      val existing =
        txn.findUploadHealingOperation(
          request.dataProviderResourceId,
          request.uploadHealingOperationId,
        )
      if (existing != null) {
        if (
          existing.createRequestId == request.requestId &&
            hasSamePlan(existing.uploadHealingOperation, request.uploadHealingOperation)
        ) {
          return@run
        }
        throw Status.ALREADY_EXISTS.withDescription(
            "UploadHealingOperation ${request.uploadHealingOperationId} already exists"
          )
          .asRuntimeException()
      }
      txn.insertUploadHealingOperation(
        request.uploadHealingOperation
          .toBuilder()
          .setDataProviderResourceId(request.dataProviderResourceId)
          .setUploadHealingOperationId(request.uploadHealingOperationId)
          .build(),
        request.requestId,
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
      val current =
        operation.stepsList.firstOrNull { it.uploadHealingStepId == request.uploadHealingStepId }
          ?: throw Status.NOT_FOUND.withDescription(
              "UploadHealingStep ${request.uploadHealingStepId} not found"
            )
            .asRuntimeException()
      if (isIdempotentReplay(current, request)) {
        return@run
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

  private fun validateCreateRequest(request: CreateUploadHealingOperationRequest) {
    requireNotBlank(request.dataProviderResourceId, "data_provider_resource_id")
    requireUuid(request.uploadHealingOperationId, "upload_healing_operation_id")
    requireUuid(request.requestId, "request_id")
    require(request.hasUploadHealingOperation()) { "upload_healing_operation is required" }
    val operation = request.uploadHealingOperation
    require(operation.reason.isNotBlank()) { "upload_healing_operation.reason is required" }
    require(operation.labeledImpressionsBlobPrefix.isNotBlank()) {
      "upload_healing_operation.labeled_impressions_blob_prefix is required"
    }
    require(operation.badRawImpressionUploadResourceIdsList.isNotEmpty()) {
      "upload_healing_operation.bad_raw_impression_upload_resource_ids is required"
    }
    require(operation.hasCutoffTime() && Timestamps.isValid(operation.cutoffTime)) {
      "upload_healing_operation.cutoff_time is required and must be valid"
    }
    require(operation.stepsList.isNotEmpty()) { "upload_healing_operation.steps is required" }
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
    }
  }

  private fun hasSamePlan(
    existing: UploadHealingOperation,
    requested: UploadHealingOperation,
  ): Boolean {
    if (
      existing.reason != requested.reason ||
        existing.labeledImpressionsBlobPrefix != requested.labeledImpressionsBlobPrefix ||
        existing.badRawImpressionUploadResourceIdsList !=
          requested.badRawImpressionUploadResourceIdsList ||
        existing.cutoffTime != requested.cutoffTime ||
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
        left.recoveryTarget == right.recoveryTarget
    }
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
      current.recoveryAction ==
        RawImpressionUploadModelLineRecoveryAction
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
    ) {
      "only operator-recovery steps can record a recovery"
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
    precondition(
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
      current.recoveryAction ==
        RawImpressionUploadModelLineRecoveryAction
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
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
      UUID.fromString(value)
    } catch (e: IllegalArgumentException) {
      throw IllegalArgumentException("$field must be a UUID", e)
    }
  }

  private fun notFound(operationId: String) =
    Status.NOT_FOUND.withDescription("UploadHealingOperation $operationId not found")
      .asRuntimeException()
}
