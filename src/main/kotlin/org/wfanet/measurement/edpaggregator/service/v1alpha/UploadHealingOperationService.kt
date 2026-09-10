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

import com.google.protobuf.FieldMask
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingStepKey
import org.wfanet.measurement.edpaggregator.v1alpha.CreateUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.UpdateUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction as InternalRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation as InternalOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub as InternalOperationStub
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep as InternalStep
import org.wfanet.measurement.internal.edpaggregator.createUploadHealingOperationRequest as internalCreateOperationRequest
import org.wfanet.measurement.internal.edpaggregator.getUploadHealingOperationRequest as internalGetOperationRequest
import org.wfanet.measurement.internal.edpaggregator.updateUploadHealingStepRequest as internalUpdateStepRequest
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation as internalOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep as internalStep

/** Public API adapter for durable upload-healing workflows. */
class UploadHealingOperationService(
  private val internalOperationStub: InternalOperationStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : UploadHealingOperationServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createUploadHealingOperation(
    request: CreateUploadHealingOperationRequest
  ): UploadHealingOperation {
    val dataProviderKey = parseDataProvider(request.parent, "parent")
    if (!request.hasUploadHealingOperation()) required("upload_healing_operation")
    validateUuid(request.uploadHealingOperationId, "upload_healing_operation_id")
    if (request.requestId.isNotEmpty()) validateUuid(request.requestId, "request_id")
    val operation = request.uploadHealingOperation
    if (operation.reason.isBlank()) required("upload_healing_operation.reason")
    if (operation.labeledImpressionsBlobPrefix.isBlank()) {
      required("upload_healing_operation.labeled_impressions_blob_prefix")
    }
    if (operation.badRawImpressionUploadsList.isEmpty()) {
      required("upload_healing_operation.bad_raw_impression_uploads")
    }
    if (!operation.hasCutoffTime()) required("upload_healing_operation.cutoff_time")
    if (!Timestamps.isValid(operation.cutoffTime)) invalid("upload_healing_operation.cutoff_time")
    if (operation.stepsList.isEmpty()) required("upload_healing_operation.steps")

    val badUploadIds =
      operation.badRawImpressionUploadsList.map {
        parseUpload(it, "upload_healing_operation.bad_raw_impression_uploads", dataProviderKey)
          .rawImpressionUploadId
      }
    val internalSteps =
      operation.stepsList.mapIndexed { index, step ->
        val source =
          parseUpload(
            step.sourceRawImpressionUpload,
            "upload_healing_operation.steps.source_raw_impression_upload",
            dataProviderKey,
          )
        val modelLine =
          RawImpressionUploadModelLineKey.fromName(step.rawImpressionUploadModelLine)
            ?: invalid("upload_healing_operation.steps.raw_impression_upload_model_line")
        if (modelLine.parentKey != source) {
          invalid("upload_healing_operation.steps.raw_impression_upload_model_line")
        }
        val predecessorId =
          if (step.recoveryPredecessorRawImpressionUpload.isEmpty()) {
            ""
          } else {
            parseUpload(
                step.recoveryPredecessorRawImpressionUpload,
                "upload_healing_operation.steps.recovery_predecessor_raw_impression_upload",
                dataProviderKey,
              )
              .rawImpressionUploadId
          }
        if (ModelLineKey.fromName(step.cmmsModelLine) == null) {
          invalid("upload_healing_operation.steps.cmms_model_line")
        }
        if (
          step.recoveryAction ==
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_UNSPECIFIED ||
            step.recoveryAction == RawImpressionUploadModelLine.RecoveryAction.UNRECOGNIZED
        ) {
          invalid("upload_healing_operation.steps.recovery_action")
        }
        internalStep {
          uploadHealingStepId = index.toLong() + 1L
          sequenceNumber = step.sequenceNumber
          sourceRawImpressionUploadResourceId = source.rawImpressionUploadId
          rawImpressionUploadModelLineResourceId = modelLine.rawImpressionUploadModelLineId
          cmmsModelLine = step.cmmsModelLine
          memoized = step.memoized
          recoveryAction = step.recoveryAction.toInternal()
          recoveryPredecessorRawImpressionUploadResourceId = predecessorId
          recoveryTarget = step.recoveryTarget
        }
      }
    val internalResponse =
      internalOperationStub.createUploadHealingOperation(
        internalCreateOperationRequest {
          dataProviderResourceId = dataProviderKey.dataProviderId
          uploadHealingOperationId = request.uploadHealingOperationId
          uploadHealingOperation = internalOperation {
            reason = operation.reason
            labeledImpressionsBlobPrefix = operation.labeledImpressionsBlobPrefix
            badRawImpressionUploadResourceIds += badUploadIds
            cutoffTime = operation.cutoffTime
            steps += internalSteps
          }
          requestId = request.requestId.ifEmpty { request.uploadHealingOperationId }
        }
      )
    return internalResponse.toPublic()
  }

  override suspend fun getUploadHealingOperation(
    request: GetUploadHealingOperationRequest
  ): UploadHealingOperation {
    if (request.name.isBlank()) required("name")
    val key = UploadHealingOperationKey.fromName(request.name) ?: invalid("name")
    return internalOperationStub
      .getUploadHealingOperation(
        internalGetOperationRequest {
          dataProviderResourceId = key.dataProviderId
          uploadHealingOperationId = key.uploadHealingOperationId
        }
      )
      .toPublic()
  }

  override suspend fun updateUploadHealingStep(
    request: UpdateUploadHealingStepRequest
  ): UploadHealingStep {
    if (!request.hasUploadHealingStep()) required("upload_healing_step")
    val step = request.uploadHealingStep
    if (step.name.isBlank()) required("upload_healing_step.name")
    if (step.etag.isBlank()) required("upload_healing_step.etag")
    if (request.requestId.isNotEmpty()) validateUuid(request.requestId, "request_id")
    val key = UploadHealingStepKey.fromName(step.name) ?: invalid("upload_healing_step.name")
    val stepId = key.uploadHealingStepId.toLongOrNull() ?: invalid("upload_healing_step.name")
    if (
      step.state == UploadHealingStep.State.STATE_UNSPECIFIED ||
        step.state == UploadHealingStep.State.UNRECOGNIZED
    ) {
      invalid("upload_healing_step.state")
    }
    val allowedPaths =
      setOf("state", "replacement_raw_impression_upload", "recovery_done_blob_generation")
    if (request.updateMask.pathsList.toSet() != allowedPaths) invalid("update_mask")
    val replacementId =
      if (step.replacementRawImpressionUpload.isEmpty()) {
        ""
      } else {
        parseUpload(
            step.replacementRawImpressionUpload,
            "upload_healing_step.replacement_raw_impression_upload",
            key.parentKey.parentKey,
          )
          .rawImpressionUploadId
      }
    return internalOperationStub
      .updateUploadHealingStep(
        internalUpdateStepRequest {
          dataProviderResourceId = key.dataProviderId
          uploadHealingOperationId = key.uploadHealingOperationId
          uploadHealingStep = internalStep {
            uploadHealingStepId = stepId
            state = step.state.toInternal()
            replacementRawImpressionUploadResourceId = replacementId
            recoveryDoneBlobGeneration = step.recoveryDoneBlobGeneration
            etag = step.etag
          }
          updateMask =
            FieldMask.newBuilder()
              .addPaths("state")
              .addPaths("replacement_raw_impression_upload_resource_id")
              .addPaths("recovery_done_blob_generation")
              .build()
          requestId = request.requestId
        }
      )
      .toPublic(key.parentKey)
  }

  private fun InternalOperation.toPublic(): UploadHealingOperation {
    val operationKey = UploadHealingOperationKey(dataProviderResourceId, uploadHealingOperationId)
    return uploadHealingOperation {
      name = operationKey.toName()
      state = this@toPublic.state.toPublic()
      reason = this@toPublic.reason
      labeledImpressionsBlobPrefix = this@toPublic.labeledImpressionsBlobPrefix
      badRawImpressionUploads +=
        badRawImpressionUploadResourceIdsList.map {
          RawImpressionUploadKey(dataProviderResourceId, it).toName()
        }
      cutoffTime = this@toPublic.cutoffTime
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

  private fun parseDataProvider(value: String, field: String): DataProviderKey {
    if (value.isBlank()) required(field)
    return DataProviderKey.fromName(value) ?: invalid(field)
  }

  private fun parseUpload(
    value: String,
    field: String,
    expectedParent: DataProviderKey,
  ): RawImpressionUploadKey {
    val key = RawImpressionUploadKey.fromName(value) ?: invalid(field)
    if (key.parentKey != expectedParent) invalid(field)
    return key
  }

  private fun validateUuid(value: String, field: String) {
    if (value.isBlank()) required(field)
    try {
      UUID.fromString(value)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException(field, e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private fun required(field: String): Nothing =
    throw RequiredFieldNotSetException(field).asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

  private fun invalid(field: String): Nothing =
    throw InvalidFieldValueException(field).asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
}

private fun RawImpressionUploadModelLine.RecoveryAction.toInternal(): InternalRecoveryAction =
  when (this) {
    RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION ->
      InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION
    RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY ->
      InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
    RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_UNSPECIFIED,
    RawImpressionUploadModelLine.RecoveryAction.UNRECOGNIZED ->
      InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_UNSPECIFIED
  }

private fun InternalRecoveryAction.toPublic(): RawImpressionUploadModelLine.RecoveryAction =
  when (this) {
    InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION ->
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
    InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY ->
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
    InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_UNSPECIFIED,
    InternalRecoveryAction.UNRECOGNIZED ->
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_UNSPECIFIED
  }

private fun InternalOperation.State.toPublic(): UploadHealingOperation.State =
  when (this) {
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_IN_PROGRESS ->
      UploadHealingOperation.State.IN_PROGRESS
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE ->
      UploadHealingOperation.State.COMPLETE
    InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED,
    InternalOperation.State.UNRECOGNIZED -> UploadHealingOperation.State.STATE_UNSPECIFIED
  }

private fun UploadHealingStep.State.toInternal(): InternalStep.State =
  when (this) {
    UploadHealingStep.State.PENDING_EVICTION ->
      InternalStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION
    UploadHealingStep.State.WAITING_FOR_REPLACEMENT ->
      InternalStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT
    UploadHealingStep.State.RECOVERY_STARTED ->
      InternalStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED
    UploadHealingStep.State.COMPLETE -> InternalStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE
    UploadHealingStep.State.STATE_UNSPECIFIED,
    UploadHealingStep.State.UNRECOGNIZED -> InternalStep.State.UPLOAD_HEALING_STEP_STATE_UNSPECIFIED
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
