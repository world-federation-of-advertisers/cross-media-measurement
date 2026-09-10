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
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.completeUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.touchUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateUploadHealingStep
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.CreateUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.GetUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UpdateUploadHealingStepRequest
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

  override suspend fun updateUploadHealingStep(
    request: UpdateUploadHealingStepRequest
  ): UploadHealingStep {
    requireNotBlank(request.dataProviderResourceId, "data_provider_resource_id")
    requireNotBlank(request.uploadHealingOperationId, "upload_healing_operation_id")
    require(request.hasUploadHealingStep()) { "upload_healing_step is required" }
    if (request.requestId.isNotEmpty()) requireUuid(request.requestId, "request_id")
    require(request.uploadHealingStep.uploadHealingStepId > 0L) {
      "upload_healing_step.upload_healing_step_id must be positive"
    }
    require(
      request.updateMask.pathsList.toSet() ==
        setOf(
          "state",
          "replacement_raw_impression_upload_resource_id",
          "recovery_done_blob_generation",
        )
    ) {
      "update_mask must contain state and replacement_raw_impression_upload_resource_id"
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=updateUploadHealingStep"))
    transactionRunner.run { txn ->
      val result =
        txn.findUploadHealingOperation(
          request.dataProviderResourceId,
          request.uploadHealingOperationId,
        ) ?: throw notFound(request.uploadHealingOperationId)
      val operation = result.uploadHealingOperation
      val requested = request.uploadHealingStep
      val current =
        operation.stepsList.firstOrNull { it.uploadHealingStepId == requested.uploadHealingStepId }
          ?: throw Status.NOT_FOUND.withDescription(
              "UploadHealingStep ${requested.uploadHealingStepId} not found"
            )
            .asRuntimeException()
      if (
        current.state == requested.state &&
          current.replacementRawImpressionUploadResourceId ==
            requested.replacementRawImpressionUploadResourceId &&
          current.recoveryDoneBlobGeneration == requested.recoveryDoneBlobGeneration
      ) {
        return@run
      }
      if (current.etag != requested.etag) {
        throw Status.ABORTED.withDescription("upload_healing_step etag mismatch")
          .asRuntimeException()
      }
      validateTransition(current, requested)
      txn.updateUploadHealingStep(
        request.dataProviderResourceId,
        request.uploadHealingOperationId,
        requested.uploadHealingStepId,
        requested.state,
        requested.replacementRawImpressionUploadResourceId,
        requested.recoveryDoneBlobGeneration,
        request.requestId,
      )
      if (
        requested.state == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE &&
          operation.stepsList.all {
            it.uploadHealingStepId == requested.uploadHealingStepId ||
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
      .single { it.uploadHealingStepId == request.uploadHealingStep.uploadHealingStepId }
  }

  private suspend fun getOperation(
    dataProviderResourceId: String,
    operationId: String,
  ): UploadHealingOperation =
    databaseClient.singleUse().use { readContext ->
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

  private fun validateTransition(current: UploadHealingStep, requested: UploadHealingStep) {
    val allowed =
      when (current.state) {
        UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION ->
          setOf(
            UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT,
            UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE,
          )
        UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT ->
          setOf(
            UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED,
            UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE,
          )
        UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED ->
          setOf(UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE)
        UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE -> emptySet()
        UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_UNSPECIFIED,
        UploadHealingStep.State.UNRECOGNIZED -> emptySet()
      }
    require(requested.state in allowed) {
      "invalid upload-healing step transition ${current.state} -> ${requested.state}"
    }
    if (requested.state == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED) {
      require(
        current.recoveryAction ==
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
      ) {
        "only operator-recovery steps can enter RECOVERY_STARTED"
      }
      require(requested.recoveryDoneBlobGeneration > 0L) {
        "recovery_done_blob_generation is required for RECOVERY_STARTED"
      }
    }
    if (
      requested.state == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE &&
        current.recoveryTarget
    ) {
      require(requested.replacementRawImpressionUploadResourceId.isNotBlank()) {
        "replacement_raw_impression_upload_resource_id is required for a recovery target"
      }
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
