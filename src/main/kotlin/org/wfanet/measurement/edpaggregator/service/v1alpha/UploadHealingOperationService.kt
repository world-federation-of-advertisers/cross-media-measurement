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
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingStepKey
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.CreateUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest as InternalAdvanceRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction as InternalRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation as InternalOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub as InternalOperationStub
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep as InternalStep
import org.wfanet.measurement.internal.edpaggregator.advanceUploadHealingStepRequest as internalAdvanceStepRequest
import org.wfanet.measurement.internal.edpaggregator.createUploadHealingOperationRequest as internalCreateOperationRequest
import org.wfanet.measurement.internal.edpaggregator.getUploadHealingOperationRequest as internalGetOperationRequest
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
    validateUuid(request.requestId, "request_id")
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
      try {
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
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw translateInternalError(e, "UploadHealingOperation could not be created")
      }
    return internalResponse.toPublic()
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

  override suspend fun advanceUploadHealingStep(
    request: AdvanceUploadHealingStepRequest
  ): UploadHealingStep {
    if (request.name.isBlank()) required("name")
    if (request.etag.isBlank()) required("etag")
    validateUuid(request.requestId, "request_id")
    val key = UploadHealingStepKey.fromName(request.name) ?: invalid("name")
    val stepId = key.uploadHealingStepId.toLongOrNull() ?: invalid("name")
    if (
      request.action == AdvanceUploadHealingStepRequest.Action.ACTION_UNSPECIFIED ||
        request.action == AdvanceUploadHealingStepRequest.Action.UNRECOGNIZED
    ) {
      invalid("action")
    }
    if (
      request.action == AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY &&
        request.recoveryDoneBlobGeneration <= 0L
    ) {
      invalid("recovery_done_blob_generation")
    }
    if (
      request.action != AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY &&
        request.recoveryDoneBlobGeneration != 0L
    ) {
      invalid("recovery_done_blob_generation")
    }
    val replacementId =
      if (
        request.action != AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT &&
          request.replacementRawImpressionUpload.isNotEmpty()
      ) {
        invalid("replacement_raw_impression_upload")
      } else if (request.replacementRawImpressionUpload.isEmpty()) {
        if (request.action == AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT) {
          required("replacement_raw_impression_upload")
        }
        ""
      } else {
        parseUpload(
            request.replacementRawImpressionUpload,
            "replacement_raw_impression_upload",
            key.parentKey.parentKey,
          )
          .rawImpressionUploadId
      }
    return try {
      internalOperationStub
        .advanceUploadHealingStep(
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
        .toPublic(key.parentKey)
    } catch (e: StatusException) {
      throw translateInternalError(e, "UploadHealingStep ${request.name} could not advance")
    }
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

  private fun translateInternalError(
    exception: StatusException,
    publicDescription: String,
  ): StatusRuntimeException {
    val publicCode =
      when (exception.status.code) {
        Status.Code.ALREADY_EXISTS,
        Status.Code.NOT_FOUND,
        Status.Code.ABORTED,
        Status.Code.FAILED_PRECONDITION -> exception.status.code
        else -> Status.Code.INTERNAL
      }
    return Status.fromCode(publicCode)
      .withDescription(publicDescription)
      .withCause(exception)
      .asRuntimeException()
  }
}

private fun AdvanceUploadHealingStepRequest.Action.toInternal(): InternalAdvanceRequest.Action =
  when (this) {
    AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION ->
      InternalAdvanceRequest.Action.CONFIRM_EVICTION
    AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY ->
      InternalAdvanceRequest.Action.RECORD_RECOVERY
    AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT ->
      InternalAdvanceRequest.Action.CONFIRM_REPLACEMENT
    AdvanceUploadHealingStepRequest.Action.ACTION_UNSPECIFIED,
    AdvanceUploadHealingStepRequest.Action.UNRECOGNIZED ->
      InternalAdvanceRequest.Action.ACTION_UNSPECIFIED
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
