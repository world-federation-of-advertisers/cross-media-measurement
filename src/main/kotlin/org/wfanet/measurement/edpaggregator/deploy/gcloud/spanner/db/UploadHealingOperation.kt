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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep

data class UploadHealingOperationResult(
  val uploadHealingOperation: UploadHealingOperation,
  val createRequestId: String,
  val mutationRequestIds: List<String>,
  val mutationRequestFingerprints: List<ByteString>,
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
      State,
      ResumeState,
      Reason,
      LabeledImpressionsBlobPrefix,
      BadRawImpressionUploadResourceIds,
      RawImpressionUploadCorrectionCandidateIds,
      MutationRequestIds,
      MutationRequestFingerprints,
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
      RawImpressionUploadCorrectionCandidateId,
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

/** Finds an upload-healing operation by lifecycle request ID. */
suspend fun AsyncDatabaseClient.ReadContext.findUploadHealingOperationByMutationRequestId(
  dataProviderResourceId: String,
  requestId: String,
): UploadHealingOperationResult? {
  val sql =
    """
    SELECT
      DataProviderResourceId,
      UploadHealingOperationId,
      CreateRequestId,
      State,
      ResumeState,
      Reason,
      LabeledImpressionsBlobPrefix,
      BadRawImpressionUploadResourceIds,
      RawImpressionUploadCorrectionCandidateIds,
      MutationRequestIds,
      MutationRequestFingerprints,
      CutoffTime,
      CompleteTime,
      CreateTime,
      UpdateTime,
    FROM UploadHealingOperation
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND @requestId IN UNNEST(MutationRequestIds)
    LIMIT 1
    """
      .trimIndent()
  val operationRow =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("requestId").to(requestId)
        },
        Options.tag("action=findUploadHealingOperationByMutationRequestId"),
      )
      .singleOrNullIfEmpty() ?: return null
  return buildUploadHealingOperationResult(
    operationRow,
    readUploadHealingStepRows(
      dataProviderResourceId,
      operationRow.getString("UploadHealingOperationId"),
    ),
  )
}

/** Reads upload-healing operations in creation order. */
fun AsyncDatabaseClient.ReadContext.readUploadHealingOperations(
  dataProviderResourceId: String,
  filter: ListUploadHealingOperationsRequest.Filter,
  limit: Int,
  after: ListUploadHealingOperationsPageToken.After? = null,
): Flow<UploadHealingOperationResult> {
  val sql = buildString {
    appendLine(
      """
      SELECT
        DataProviderResourceId,
        UploadHealingOperationId,
        CreateRequestId,
        State,
        ResumeState,
        Reason,
        LabeledImpressionsBlobPrefix,
        BadRawImpressionUploadResourceIds,
        RawImpressionUploadCorrectionCandidateIds,
        MutationRequestIds,
        MutationRequestFingerprints,
        CutoffTime,
        CompleteTime,
        CreateTime,
        UpdateTime,
      FROM UploadHealingOperation
      """
        .trimIndent()
    )
    val conjuncts = mutableListOf("DataProviderResourceId = @dataProviderResourceId")
    if (filter.stateInList.isNotEmpty()) {
      conjuncts += "State IN UNNEST(@stateIn)"
    }
    if (after != null) {
      conjuncts +=
        "((CreateTime > @afterCreateTime) OR " +
          "(CreateTime = @afterCreateTime AND UploadHealingOperationId > @afterOperationId))"
    }
    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine("ORDER BY CreateTime, UploadHealingOperationId")
    appendLine("LIMIT @limit")
  }
  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("limit").to(limit.toLong())
      if (filter.stateInList.isNotEmpty()) {
        bind("stateIn").toInt64Array(filter.stateInList.map { it.number.toLong() })
      }
      if (after != null) {
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
        bind("afterOperationId").to(after.uploadHealingOperationId)
      }
    }
  return executeQuery(query, Options.tag("action=readUploadHealingOperations")).map { row ->
    buildUploadHealingOperationResult(
      row,
      readUploadHealingStepRows(dataProviderResourceId, row.getString("UploadHealingOperationId")),
    )
  }
}

private suspend fun AsyncDatabaseClient.ReadContext.readUploadHealingStepRows(
  dataProviderResourceId: String,
  uploadHealingOperationId: String,
): List<Struct> {
  val sql =
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
      RawImpressionUploadCorrectionCandidateId,
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
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("uploadHealingOperationId").to(uploadHealingOperationId)
      }
    )
    .toList()
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
    set("State")
      .to(
        if (
            operation.state ==
              UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
          ) {
            UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING
          } else {
            operation.state
          }
          .number
          .toLong()
      )
    if (
      operation.resumeState !=
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
    ) {
      set("ResumeState").to(operation.resumeState.number.toLong())
    }
    set("Reason").to(operation.reason)
    set("LabeledImpressionsBlobPrefix").to(operation.labeledImpressionsBlobPrefix)
    set("BadRawImpressionUploadResourceIds")
      .toStringArray(operation.badRawImpressionUploadResourceIdsList)
    set("RawImpressionUploadCorrectionCandidateIds")
      .toStringArray(operation.rawImpressionUploadCorrectionCandidateIdsList)
    set("MutationRequestIds").toStringArray(emptyList())
    set("MutationRequestFingerprints").toBytesArray(emptyList())
    set("CutoffTime").to(operation.cutoffTime.toGcloudTimestamp())
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  insertUploadHealingSteps(operation)
}

/** Replaces the mutable fields and steps of a draft upload-healing operation. */
fun AsyncDatabaseClient.TransactionContext.replaceUploadHealingOperationPlan(
  operation: UploadHealingOperation,
  mutationRequestIds: List<String>? = null,
  mutationRequestFingerprints: List<ByteString>? = null,
) {
  bufferUpdateMutation("UploadHealingOperation") {
    set("DataProviderResourceId").to(operation.dataProviderResourceId)
    set("UploadHealingOperationId").to(operation.uploadHealingOperationId)
    set("Reason").to(operation.reason)
    set("LabeledImpressionsBlobPrefix").to(operation.labeledImpressionsBlobPrefix)
    set("BadRawImpressionUploadResourceIds")
      .toStringArray(operation.badRawImpressionUploadResourceIdsList)
    set("RawImpressionUploadCorrectionCandidateIds")
      .toStringArray(operation.rawImpressionUploadCorrectionCandidateIdsList)
    set("State").to(operation.state.number.toLong())
    if (
      operation.resumeState ==
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
    ) {
      set("ResumeState").to(null as Long?)
    } else {
      set("ResumeState").to(operation.resumeState.number.toLong())
    }
    if (mutationRequestIds != null) {
      set("MutationRequestIds").toStringArray(mutationRequestIds)
    }
    if (mutationRequestFingerprints != null) {
      set("MutationRequestFingerprints")
        .toBytesArray(mutationRequestFingerprints.map { it.toGcloudByteArray() })
    }
    set("CutoffTime").to(operation.cutoffTime.toGcloudTimestamp())
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
  buffer(
    Mutation.delete(
      "UploadHealingStep",
      KeySet.prefixRange(
        Key.of(operation.dataProviderResourceId, operation.uploadHealingOperationId)
      ),
    )
  )
  insertUploadHealingSteps(operation)
}

private fun AsyncDatabaseClient.TransactionContext.insertUploadHealingSteps(
  operation: UploadHealingOperation
) {
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
      if (step.rawImpressionUploadCorrectionCandidateId.isNotEmpty()) {
        set("RawImpressionUploadCorrectionCandidateId")
          .to(step.rawImpressionUploadCorrectionCandidateId)
      }
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
    set("State")
      .to(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE.number.toLong())
    set("ResumeState").to(null as Long?)
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

/** Updates the lifecycle state of an upload-healing operation. */
fun AsyncDatabaseClient.TransactionContext.updateUploadHealingOperationState(
  dataProviderResourceId: String,
  uploadHealingOperationId: String,
  state: UploadHealingOperation.State,
  resumeState: UploadHealingOperation.State =
    UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED,
  mutationRequestIds: List<String>? = null,
  mutationRequestFingerprints: List<ByteString>? = null,
) {
  bufferUpdateMutation("UploadHealingOperation") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("UploadHealingOperationId").to(uploadHealingOperationId)
    set("State").to(state.number.toLong())
    if (resumeState == UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED) {
      set("ResumeState").to(null as Long?)
    } else {
      set("ResumeState").to(resumeState.number.toLong())
    }
    if (state == UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE) {
      set("CompleteTime").to(Value.COMMIT_TIMESTAMP)
    }
    if (mutationRequestIds != null) {
      set("MutationRequestIds").toStringArray(mutationRequestIds)
    }
    if (mutationRequestFingerprints != null) {
      set("MutationRequestFingerprints")
        .toBytesArray(mutationRequestFingerprints.map { it.toGcloudByteArray() })
    }
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
      state = UploadHealingOperation.State.forNumber(operationRow.getLong("State").toInt())
      if (!operationRow.isNull("ResumeState")) {
        resumeState =
          UploadHealingOperation.State.forNumber(operationRow.getLong("ResumeState").toInt())
      }
      reason = operationRow.getString("Reason")
      labeledImpressionsBlobPrefix = operationRow.getString("LabeledImpressionsBlobPrefix")
      badRawImpressionUploadResourceIds +=
        operationRow.getStringList("BadRawImpressionUploadResourceIds")
      rawImpressionUploadCorrectionCandidateIds +=
        operationRow.getStringList("RawImpressionUploadCorrectionCandidateIds")
      cutoffTime = operationRow.getTimestamp("CutoffTime").toProto()
      createTime = operationRow.getTimestamp("CreateTime").toProto()
      updateTime = operationUpdateTime
      etag = ETags.computeETag(operationUpdateTime.toInstant())
      steps += stepRows.map { buildUploadHealingStep(it) }
    },
    createRequestId = operationRow.getString("CreateRequestId"),
    mutationRequestIds = operationRow.getStringList("MutationRequestIds"),
    mutationRequestFingerprints =
      operationRow.getBytesList("MutationRequestFingerprints").map {
        it.toByteArray().toByteString()
      },
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
      if (!row.isNull("RawImpressionUploadCorrectionCandidateId")) {
        rawImpressionUploadCorrectionCandidateId =
          row.getString("RawImpressionUploadCorrectionCandidateId")
      }
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
