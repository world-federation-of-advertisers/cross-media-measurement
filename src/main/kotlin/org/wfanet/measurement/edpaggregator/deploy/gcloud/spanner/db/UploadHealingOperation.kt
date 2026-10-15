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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db

import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep

data class UploadHealingOperationResult(
  val uploadHealingOperation: UploadHealingOperation,
  val createRequestId: String,
)

/** Reads an upload-healing operation and all of its ordered steps. */
suspend fun AsyncDatabaseClient.ReadContext.findUploadHealingOperation(
  dataProviderResourceId: String,
  uploadHealingOperationId: String,
): UploadHealingOperationResult? {
  val operationSql =
    """
    SELECT
      DataProviderResourceId,
      UploadHealingOperationId,
      CreateRequestId,
      Reason,
      LabeledImpressionsBlobPrefix,
      BadRawImpressionUploadResourceIds,
      CutoffTime,
      CompleteTime,
      CreateTime,
      UpdateTime,
    FROM UploadHealingOperation
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND UploadHealingOperationId = @uploadHealingOperationId
    """
      .trimIndent()
  val operationRow =
    executeQuery(
        statement(operationSql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("uploadHealingOperationId").to(uploadHealingOperationId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  val stepSql =
    """
    SELECT
      UploadHealingStepId,
      SequenceNumber,
      SourceRawImpressionUploadResourceId,
      RawImpressionUploadModelLineResourceId,
      CmmsModelLine,
      Memoized,
      RecoveryAction,
      RecoveryPredecessorRawImpressionUploadResourceId,
      RecoveryTarget,
      EvictionCompleteTime,
      RecoveryStartTime,
      RecoveryDoneBlobGeneration,
      ReplacementRawImpressionUploadResourceId,
      CompleteTime,
      UpdateTime,
    FROM UploadHealingStep
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND UploadHealingOperationId = @uploadHealingOperationId
    ORDER BY SequenceNumber, UploadHealingStepId
    """
      .trimIndent()
  val stepRows =
    executeQuery(
        statement(stepSql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("uploadHealingOperationId").to(uploadHealingOperationId)
        }
      )
      .toList()
  return buildUploadHealingOperationResult(operationRow, stepRows)
}

/** Buffers one operation and all of its steps in the caller's transaction. */
fun AsyncDatabaseClient.TransactionContext.insertUploadHealingOperation(
  operation: UploadHealingOperation,
  createRequestId: String,
) {
  bufferInsertMutation("UploadHealingOperation") {
    set("DataProviderResourceId").to(operation.dataProviderResourceId)
    set("UploadHealingOperationId").to(operation.uploadHealingOperationId)
    set("CreateRequestId").to(createRequestId)
    set("Reason").to(operation.reason)
    set("LabeledImpressionsBlobPrefix").to(operation.labeledImpressionsBlobPrefix)
    set("BadRawImpressionUploadResourceIds")
      .toStringArray(operation.badRawImpressionUploadResourceIdsList)
    set("CutoffTime").to(operation.cutoffTime.toGcloudTimestamp())
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  for (step in operation.stepsList) {
    bufferInsertMutation("UploadHealingStep") {
      set("DataProviderResourceId").to(operation.dataProviderResourceId)
      set("UploadHealingOperationId").to(operation.uploadHealingOperationId)
      set("UploadHealingStepId").to(step.uploadHealingStepId)
      set("SequenceNumber").to(step.sequenceNumber)
      set("SourceRawImpressionUploadResourceId").to(step.sourceRawImpressionUploadResourceId)
      set("RawImpressionUploadModelLineResourceId").to(step.rawImpressionUploadModelLineResourceId)
      set("CmmsModelLine").to(step.cmmsModelLine)
      set("Memoized").to(step.memoized)
      set("RecoveryAction").to(step.recoveryAction)
      if (step.recoveryPredecessorRawImpressionUploadResourceId.isNotEmpty()) {
        set("RecoveryPredecessorRawImpressionUploadResourceId")
          .to(step.recoveryPredecessorRawImpressionUploadResourceId)
      }
      set("RecoveryTarget").to(step.recoveryTarget)
      set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    }
  }
}

/** Buffers an idempotent checkpoint update for one step. */
fun AsyncDatabaseClient.TransactionContext.updateUploadHealingStep(
  dataProviderResourceId: String,
  uploadHealingOperationId: String,
  uploadHealingStepId: Long,
  previousState: UploadHealingStep.State,
  state: UploadHealingStep.State,
  replacementRawImpressionUploadResourceId: String,
  recoveryDoneBlobGeneration: Long,
  requestId: String,
) {
  bufferUpdateMutation("UploadHealingStep") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("UploadHealingOperationId").to(uploadHealingOperationId)
    set("UploadHealingStepId").to(uploadHealingStepId)
    when (state) {
      UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT -> {
        set("EvictionCompleteTime").to(Value.COMMIT_TIMESTAMP)
      }
      UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED -> {
        set("RecoveryStartTime").to(Value.COMMIT_TIMESTAMP)
        set("RecoveryDoneBlobGeneration").to(recoveryDoneBlobGeneration)
      }
      UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE -> {
        if (previousState == UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION) {
          set("EvictionCompleteTime").to(Value.COMMIT_TIMESTAMP)
        }
        set("CompleteTime").to(Value.COMMIT_TIMESTAMP)
      }
      UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION,
      UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_UNSPECIFIED,
      UploadHealingStep.State.UNRECOGNIZED -> error("Unsupported healing-step state: $state")
    }
    if (replacementRawImpressionUploadResourceId.isNotEmpty()) {
      set("ReplacementRawImpressionUploadResourceId").to(replacementRawImpressionUploadResourceId)
    }
    if (requestId.isNotEmpty()) {
      set("UpdateRequestId").to(requestId)
    }
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Marks the operation complete in the same transaction as its final step. */
fun AsyncDatabaseClient.TransactionContext.completeUploadHealingOperation(
  dataProviderResourceId: String,
  uploadHealingOperationId: String,
) {
  bufferUpdateMutation("UploadHealingOperation") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("UploadHealingOperationId").to(uploadHealingOperationId)
    set("CompleteTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Updates the parent operation timestamp when a child checkpoint advances. */
fun AsyncDatabaseClient.TransactionContext.touchUploadHealingOperation(
  dataProviderResourceId: String,
  uploadHealingOperationId: String,
) {
  bufferUpdateMutation("UploadHealingOperation") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("UploadHealingOperationId").to(uploadHealingOperationId)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

private fun buildUploadHealingOperationResult(
  operationRow: Struct,
  stepRows: List<Struct>,
): UploadHealingOperationResult {
  val dataProviderResourceId = operationRow.getString("DataProviderResourceId")
  val operationId = operationRow.getString("UploadHealingOperationId")
  val operationUpdateTime = operationRow.getTimestamp("UpdateTime").toProto()
  return UploadHealingOperationResult(
    uploadHealingOperation {
      this.dataProviderResourceId = dataProviderResourceId
      uploadHealingOperationId = operationId
      reason = operationRow.getString("Reason")
      labeledImpressionsBlobPrefix = operationRow.getString("LabeledImpressionsBlobPrefix")
      badRawImpressionUploadResourceIds +=
        operationRow.getStringList("BadRawImpressionUploadResourceIds")
      cutoffTime = operationRow.getTimestamp("CutoffTime").toProto()
      createTime = operationRow.getTimestamp("CreateTime").toProto()
      updateTime = operationUpdateTime
      etag = ETags.computeETag(operationUpdateTime.toInstant())
      if (operationRow.isNull("CompleteTime")) {
        state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_IN_PROGRESS
      } else {
        state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE
      }
      steps += stepRows.map { buildUploadHealingStep(it) }
    },
    operationRow.getString("CreateRequestId"),
  )
}

private fun buildUploadHealingStep(row: Struct): UploadHealingStep {
  val updateTime = row.getTimestamp("UpdateTime").toProto()
  return uploadHealingStep {
    uploadHealingStepId = row.getLong("UploadHealingStepId")
    sequenceNumber = row.getLong("SequenceNumber")
    sourceRawImpressionUploadResourceId = row.getString("SourceRawImpressionUploadResourceId")
    rawImpressionUploadModelLineResourceId = row.getString("RawImpressionUploadModelLineResourceId")
    cmmsModelLine = row.getString("CmmsModelLine")
    memoized = row.getBoolean("Memoized")
    recoveryAction =
      row.getProtoEnum("RecoveryAction", RawImpressionUploadModelLineRecoveryAction::forNumber)
    if (!row.isNull("RecoveryPredecessorRawImpressionUploadResourceId")) {
      recoveryPredecessorRawImpressionUploadResourceId =
        row.getString("RecoveryPredecessorRawImpressionUploadResourceId")
    }
    recoveryTarget = row.getBoolean("RecoveryTarget")
    if (!row.isNull("EvictionCompleteTime")) {
      evictionCompleteTime = row.getTimestamp("EvictionCompleteTime").toProto()
    }
    if (!row.isNull("RecoveryStartTime")) {
      recoveryStartTime = row.getTimestamp("RecoveryStartTime").toProto()
    }
    if (!row.isNull("RecoveryDoneBlobGeneration")) {
      recoveryDoneBlobGeneration = row.getLong("RecoveryDoneBlobGeneration")
    }
    if (!row.isNull("ReplacementRawImpressionUploadResourceId")) {
      replacementRawImpressionUploadResourceId =
        row.getString("ReplacementRawImpressionUploadResourceId")
    }
    if (!row.isNull("CompleteTime")) {
      completeTime = row.getTimestamp("CompleteTime").toProto()
    }
    this.updateTime = updateTime
    etag = ETags.computeETag(updateTime.toInstant())
    state =
      when {
        hasCompleteTime() -> UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE
        hasRecoveryStartTime() -> UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED
        hasEvictionCompleteTime() ->
          UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT
        else -> UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION
      }
  }
}
