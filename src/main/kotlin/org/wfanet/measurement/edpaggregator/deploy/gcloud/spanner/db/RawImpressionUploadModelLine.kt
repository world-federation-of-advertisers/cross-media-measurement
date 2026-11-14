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
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toInt64Array
import org.wfanet.measurement.internal.edpaggregator.BlobType
import org.wfanet.measurement.internal.edpaggregator.EncryptedDek
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineFailureReason
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState as State
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadModelLine

data class RawImpressionUploadModelLineResult(
  /** The [RawImpressionUploadModelLine] read from Spanner. */
  val rawImpressionUploadModelLine: RawImpressionUploadModelLine,
  /** Internal numeric ID of the parent `RawImpressionUpload` row. */
  val rawImpressionUploadId: Long,
  /** Internal numeric ID of the `RawImpressionUploadModelLine` row. */
  val rawImpressionUploadModelLineId: Long,
  /**
   * AIP-155 `request_id` of the mark that moved this line to `POOL_ASSIGNING`, or empty if that
   * transition hasn't happened. Used to short-circuit an idempotent replay of the same mark.
   */
  val markPoolAssigningRequestId: String,
  /**
   * AIP-155 `request_id` of the mark that moved this line to `RANKING`, or empty if that transition
   * hasn't happened. Used to short-circuit an idempotent replay of the same mark.
   */
  val markRankingRequestId: String,
  /**
   * AIP-155 `request_id` of the mark that moved this line to `LABELING`, or empty if that
   * transition hasn't happened. Used to short-circuit an idempotent replay of the same mark.
   */
  val markLabelingRequestId: String,
  /** AIP-155 request ID of the transition to `AVAILABILITY_SYNCING`. */
  val markAvailabilitySyncingRequestId: String,
  /**
   * AIP-155 `request_id` of the mark that moved this line to `COMPLETED`, or empty if that
   * transition hasn't happened. Used to short-circuit an idempotent replay of the same mark.
   */
  val markCompletedRequestId: String,
  /**
   * AIP-155 `request_id` of the mark that moved this line to `FAILED`, or empty if that transition
   * hasn't happened. Used to short-circuit an idempotent replay of the same mark.
   */
  val markFailedRequestId: String,
)

/** Returns whether a [RawImpressionUploadModelLine] with the specified keys exists. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionUploadModelLineExists(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  rawImpressionUploadModelLineId: Long,
): Boolean {
  return readRow(
    "RawImpressionUploadModelLine",
    Key.of(dataProviderResourceId, rawImpressionUploadId, rawImpressionUploadModelLineId),
    listOf("RawImpressionUploadModelLineId"),
  ) != null
}

/** Returns the state of the model-line row identified by its CMMS resource name. */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadModelLineStateByCmmsModelLine(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  cmmsModelLine: String,
): State? {
  val sql =
    """
    SELECT State
    FROM RawImpressionUploadModelLine
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadId = @rawImpressionUploadId
      AND CmmsModelLine = @cmmsModelLine
    LIMIT 1
    """
      .trimIndent()
  val row =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadId").to(rawImpressionUploadId)
          bind("cmmsModelLine").to(cmmsModelLine)
        },
        Options.tag("action=getRawImpressionUploadModelLineStateByCmmsModelLine"),
      )
      .singleOrNullIfEmpty() ?: return null
  return row.getProtoEnum("State", State::forNumber)
}

/** Returns whether [rawImpressionUploadId] has any registered model-line children. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionUploadHasModelLines(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
): Boolean {
  val sql =
    """
    SELECT RawImpressionUploadModelLineId
    FROM RawImpressionUploadModelLine
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadId = @rawImpressionUploadId
    LIMIT 1
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("rawImpressionUploadId").to(rawImpressionUploadId)
      }
    )
    .singleOrNullIfEmpty() != null
}

/**
 * Returns the parent [RawImpressionUpload]'s current [RawImpressionUploadState], or `null` if the
 * row is missing.
 */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadState(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
): RawImpressionUploadState? {
  val row =
    readRow(
      "RawImpressionUpload",
      Key.of(dataProviderResourceId, rawImpressionUploadId),
      listOf("State"),
    ) ?: return null
  return row.getProtoEnum("State", RawImpressionUploadState::forNumber)
}

/**
 * Counts the [RawImpressionUploadModelLine] rows under the given upload that are not yet COMPLETED,
 * excluding [excludeRawImpressionUploadModelLineId] (its terminal write is buffered and not visible
 * to reads in the same transaction).
 */
suspend fun AsyncDatabaseClient.ReadContext.countNonCompletedRawImpressionUploadModelLines(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  excludeRawImpressionUploadModelLineId: Long,
): Long {
  val sql =
    """
    SELECT COUNT(*) AS NonCompletedCount
    FROM RawImpressionUploadModelLine
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadId = @rawImpressionUploadId
      AND RawImpressionUploadModelLineId != @excludeRawImpressionUploadModelLineId
      AND CAST(State AS INT64) != @completedState
    """
      .trimIndent()

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadId").to(rawImpressionUploadId)
          bind("excludeRawImpressionUploadModelLineId").to(excludeRawImpressionUploadModelLineId)
          bind("completedState")
            .to(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED.number.toLong())
        }
      )
      .singleOrNullIfEmpty() ?: return 0
  return row.getLong("NonCompletedCount")
}

/** A conflicting in-progress model-line row: the owning upload's resource ID and its state. */
data class InProgressModelLine(val rawImpressionUploadResourceId: String, val state: State)

/**
 * Finds up to [limit] model-line rows for ([dataProviderResourceId], [cmmsModelLine]) that are in a
 * processing state (POOL_ASSIGNING/RANKING/LABELING) under an upload OTHER than
 * [excludeRawImpressionUploadId], returning each conflicting upload's resource ID and state.
 *
 * Used to serialize computation per (DataProvider, model line) and to name the blocking uploads.
 * Ordinary uploads may follow durable availability handoffs when their output keys do not overlap.
 * A direct healing labeling transition may do the same only when neither upload has memoized
 * processing evidence. Other healing transitions retain their approved predecessor restrictions. A
 * processing failure with a live rank checkpoint remains a conflict so later computation cannot
 * invalidate that checkpoint's cumulative basis.
 */
suspend fun AsyncDatabaseClient.ReadContext.findInProgressModelLinesForModelLine(
  dataProviderResourceId: String,
  cmmsModelLine: String,
  excludeRawImpressionUploadId: Long,
  permittedEvictionOperationId: String = "",
  onlyComputation: Boolean = false,
  directLabeling: Boolean = false,
  historicalAvailabilityUploads: List<String> = emptyList(),
  limit: Int = 5,
): List<InProgressModelLine> {
  val sql =
    """
    SELECT
      RawImpressionUpload.RawImpressionUploadResourceId AS RawImpressionUploadResourceId,
      RawImpressionUploadModelLine.State AS State
    FROM RawImpressionUploadModelLine
    JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
    JOIN RawImpressionUpload AS candidate
      ON candidate.DataProviderResourceId = RawImpressionUpload.DataProviderResourceId
      AND candidate.RawImpressionUploadId = @excludeRawImpressionUploadId
    WHERE RawImpressionUploadModelLine.DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadModelLine.CmmsModelLine = @cmmsModelLine
      AND RawImpressionUploadModelLine.RawImpressionUploadId != @excludeRawImpressionUploadId
      AND (
        CAST(RawImpressionUploadModelLine.State AS INT64) IN UNNEST(@inProgressStates)
        OR (
          NOT @onlyComputation
          AND RawImpressionUploadModelLine.State = @availabilitySyncingState
          AND NOT (
            @permittedEvictionOperationId != ''
            AND COALESCE(RawImpressionUpload.EvictionOperationId, '') = @permittedEvictionOperationId
          )
          AND (
            (
              COALESCE(candidate.EvictionOperationId, '') != ''
              AND RawImpressionUpload.RawImpressionUploadResourceId NOT IN UNNEST(@historicalAvailabilityUploads)
              AND NOT (
                @directLabeling
                AND NOT EXISTS (
                  SELECT 1 FROM PoolAssignmentJob AS pool
                  WHERE pool.DataProviderResourceId = candidate.DataProviderResourceId
                    AND pool.CmmsModelLine = @cmmsModelLine
                    AND pool.RawImpressionUploadId IN (
                      candidate.RawImpressionUploadId, RawImpressionUpload.RawImpressionUploadId
                    )
                )
                AND NOT EXISTS (
                  SELECT 1 FROM RankerJob AS ranker
                  WHERE ranker.DataProviderResourceId = candidate.DataProviderResourceId
                    AND ranker.CmmsModelLine = @cmmsModelLine
                    AND ranker.RawImpressionUploadId IN (
                      candidate.RawImpressionUploadId, RawImpressionUpload.RawImpressionUploadId
                    )
                )
                AND NOT EXISTS (
                  SELECT 1 FROM RankIndexBlob AS rank
                  WHERE rank.DataProviderResourceId = candidate.DataProviderResourceId
                    AND rank.CmmsModelLine = @cmmsModelLine
                    AND rank.RawImpressionUploadId IN (
                      candidate.RawImpressionUploadId, RawImpressionUpload.RawImpressionUploadId
                    )
                )
              )
            )
            OR COALESCE(candidate.RegistrationComplete, FALSE) = FALSE
            OR COALESCE(RawImpressionUpload.RegistrationComplete, FALSE) = FALSE
            OR NOT EXISTS (
              SELECT 1 FROM RawImpressionUploadFile AS input
              WHERE input.DataProviderResourceId = candidate.DataProviderResourceId
                AND input.RawImpressionUploadId = candidate.RawImpressionUploadId
                AND input.DeleteTime IS NULL
            )
            OR EXISTS (
              SELECT 1 FROM RawImpressionUploadFile AS file
              WHERE file.DataProviderResourceId = candidate.DataProviderResourceId
                AND file.RawImpressionUploadId IN (
                  candidate.RawImpressionUploadId, RawImpressionUpload.RawImpressionUploadId
                )
                AND file.DeleteTime IS NULL
                AND (file.BlobUri = '' OR file.EventDate IS NULL)
            )
            OR NOT EXISTS (
              SELECT 1 FROM RawImpressionUploadFile AS prior
              WHERE prior.DataProviderResourceId = RawImpressionUpload.DataProviderResourceId
                AND prior.RawImpressionUploadId = RawImpressionUpload.RawImpressionUploadId
                AND prior.DeleteTime IS NULL
            )
            OR EXISTS (
              SELECT 1
              FROM RawImpressionUploadFile AS input
              JOIN RawImpressionUploadFile AS prior
                ON input.DataProviderResourceId = prior.DataProviderResourceId
                AND input.BlobUri = prior.BlobUri
                AND input.EventDate = prior.EventDate
              WHERE input.DataProviderResourceId = candidate.DataProviderResourceId
                AND input.RawImpressionUploadId = candidate.RawImpressionUploadId
                AND prior.RawImpressionUploadId = RawImpressionUpload.RawImpressionUploadId
                AND input.DeleteTime IS NULL AND prior.DeleteTime IS NULL
            )
          )
        )
        OR (
          NOT @onlyComputation
          AND COALESCE(candidate.EvictionOperationId, '') = ''
          AND RawImpressionUploadModelLine.State = @failedState
          AND COALESCE(CAST(RawImpressionUploadModelLine.FailureReason AS INT64), 0)
            IN UNNEST(@retryableFailureReasons)
          AND EXISTS (
            SELECT 1 FROM RankIndexBlob AS checkpoint
            WHERE checkpoint.DataProviderResourceId = RawImpressionUpload.DataProviderResourceId
              AND checkpoint.RawImpressionUploadId = RawImpressionUpload.RawImpressionUploadId
              AND checkpoint.CmmsModelLine = @cmmsModelLine
              AND checkpoint.BlobType = @snapshotBlobType
              AND checkpoint.DeleteTime IS NULL
          )
        )
      )
    LIMIT @limit
    """
      .trimIndent()

  return buildList {
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("cmmsModelLine").to(cmmsModelLine)
          bind("excludeRawImpressionUploadId").to(excludeRawImpressionUploadId)
          bind("permittedEvictionOperationId").to(permittedEvictionOperationId)
          bind("onlyComputation").to(onlyComputation)
          bind("directLabeling").to(directLabeling)
          bind("historicalAvailabilityUploads").toStringArray(historicalAvailabilityUploads)
          bind("availabilitySyncingState")
            .to(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING)
          bind("failedState").to(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED)
          bind("snapshotBlobType").to(BlobType.BLOB_TYPE_SNAPSHOT)
          bind("retryableFailureReasons")
            .toInt64Array(
              listOf(
                  RawImpressionUploadModelLineFailureReason
                    .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_UNSPECIFIED,
                  RawImpressionUploadModelLineFailureReason
                    .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_PROCESSING_FAILURE,
                )
                .map { it.number.toLong() }
            )
          bind("limit").to(limit.toLong())
          bind("inProgressStates")
            .toInt64Array(
              listOf(
                  State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING,
                  State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING,
                  State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING,
                )
                .map { it.number.toLong() }
            )
        }
      )
      .collect { row ->
        add(
          InProgressModelLine(
            row.getString("RawImpressionUploadResourceId"),
            row.getProtoEnum("State", State::forNumber),
          )
        )
      }
  }
}

/**
 * Finds unaffected availability owners whose snapshots were already historical when the approved
 * replacement's original source computed. Deleted original snapshots remain boundaries. If the
 * source never wrote a particular pool, its first checkpoint is the boundary; source creation is
 * used when it wrote no checkpoint.
 */
suspend fun AsyncDatabaseClient.ReadContext.findHistoricalAvailabilityUploads(
  dataProviderResourceId: String,
  cmmsModelLine: String,
  candidateRawImpressionUploadId: Long,
): List<String> {
  val query =
    statement(
      """
    SELECT owner.RawImpressionUploadResourceId AS ResourceId
    FROM RawImpressionUpload AS candidate
    JOIN RawImpressionUpload AS source
      ON source.DataProviderResourceId = candidate.DataProviderResourceId
      AND source.RawImpressionUploadResourceId = candidate.ReplacesRawImpressionUploadResourceId
    JOIN RawImpressionUpload AS owner
      ON owner.DataProviderResourceId = candidate.DataProviderResourceId
    JOIN RawImpressionUploadModelLine AS model
      ON model.DataProviderResourceId = owner.DataProviderResourceId
      AND model.RawImpressionUploadId = owner.RawImpressionUploadId
    WHERE candidate.DataProviderResourceId = @provider
      AND candidate.RawImpressionUploadId = @candidate
      AND COALESCE(candidate.EvictionOperationId, '') != ''
      AND COALESCE(candidate.RegistrationComplete, FALSE)
      AND COALESCE(source.RegistrationComplete, FALSE)
      AND COALESCE(owner.RegistrationComplete, FALSE)
      AND COALESCE(owner.EvictionOperationId, '') = ''
      AND model.CmmsModelLine = @model
      AND model.State = @availability
      AND EXISTS (
        SELECT 1 FROM UploadHealingStep AS target
        WHERE target.DataProviderResourceId = candidate.DataProviderResourceId
          AND target.UploadHealingOperationId = candidate.EvictionOperationId
          AND target.SourceRawImpressionUploadResourceId = source.RawImpressionUploadResourceId
          AND target.CmmsModelLine = @model
          AND target.RecoveryTarget AND target.Memoized
      )
      AND NOT EXISTS (
        SELECT 1 FROM UploadHealingStep AS affected
        WHERE affected.DataProviderResourceId = candidate.DataProviderResourceId
          AND affected.UploadHealingOperationId = candidate.EvictionOperationId
          AND affected.SourceRawImpressionUploadResourceId = owner.RawImpressionUploadResourceId
          AND affected.CmmsModelLine = @model
      )
      AND EXISTS (
        SELECT 1 FROM RankIndexBlob AS checkpoint
        WHERE checkpoint.DataProviderResourceId = owner.DataProviderResourceId
          AND checkpoint.RawImpressionUploadId = owner.RawImpressionUploadId
          AND checkpoint.CmmsModelLine = @model
          AND checkpoint.BlobType = @snapshot
          AND checkpoint.DeleteTime IS NULL
      )
      AND NOT EXISTS (
        SELECT 1 FROM RankIndexBlob AS checkpoint
        WHERE checkpoint.DataProviderResourceId = owner.DataProviderResourceId
          AND checkpoint.RawImpressionUploadId = owner.RawImpressionUploadId
          AND checkpoint.CmmsModelLine = @model
          AND checkpoint.BlobType = @snapshot
          AND checkpoint.DeleteTime IS NULL
          AND checkpoint.CreateTime >= COALESCE((
            SELECT MIN(boundary.CreateTime) FROM RankIndexBlob AS boundary
            WHERE boundary.DataProviderResourceId = source.DataProviderResourceId
              AND boundary.RawImpressionUploadId = source.RawImpressionUploadId
              AND boundary.CmmsModelLine = @model
              AND boundary.PoolOffset = checkpoint.PoolOffset
              AND boundary.BlobType = @snapshot
          ), (
            SELECT MIN(firstBoundary.CreateTime) FROM RankIndexBlob AS firstBoundary
            WHERE firstBoundary.DataProviderResourceId = source.DataProviderResourceId
              AND firstBoundary.RawImpressionUploadId = source.RawImpressionUploadId
              AND firstBoundary.CmmsModelLine = @model
              AND firstBoundary.BlobType = @snapshot
          ), source.CreateTime)
      )
    """
        .trimIndent()
    ) {
      bind("provider").to(dataProviderResourceId)
      bind("candidate").to(candidateRawImpressionUploadId)
      bind("model").to(cmmsModelLine)
      bind("snapshot").to(BlobType.BLOB_TYPE_SNAPSHOT)
      bind("availability").to(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING)
    }
  return executeQuery(query, Options.tag("action=findHistoricalAvailabilityUploads"))
    .map { it.getString("ResourceId") }
    .toList()
}

/**
 * Finds a newer usable cumulative basis for a checkpoint retained by [rawImpressionUploadId].
 *
 * A rank retry reuses its own per-pool SNAPSHOT before choosing prior snapshots. If another
 * eligible owner has advanced that same pool since the retained checkpoint, reusing it would mix
 * cumulative bases across the retry. This detects histories that predate computation serialization
 * around failed checkpoints; approved healing must reconcile them instead.
 */
suspend fun AsyncDatabaseClient.ReadContext.findNewerSnapshotBasis(
  dataProviderResourceId: String,
  cmmsModelLine: String,
  rawImpressionUploadId: Long,
): InProgressModelLine? {
  val query =
    statement(
      """
    SELECT owner.RawImpressionUploadResourceId AS RawImpressionUploadResourceId,
      model.State AS State
    FROM RankIndexBlob AS checkpoint
    JOIN RankIndexBlob AS newer
      ON newer.DataProviderResourceId = checkpoint.DataProviderResourceId
      AND newer.CmmsModelLine = checkpoint.CmmsModelLine
      AND newer.PoolOffset = checkpoint.PoolOffset
    JOIN RawImpressionUpload AS owner
      ON owner.DataProviderResourceId = newer.DataProviderResourceId
      AND owner.RawImpressionUploadId = newer.RawImpressionUploadId
    JOIN RawImpressionUploadModelLine AS model
      ON model.DataProviderResourceId = newer.DataProviderResourceId
      AND model.RawImpressionUploadId = newer.RawImpressionUploadId
      AND model.CmmsModelLine = newer.CmmsModelLine
    WHERE checkpoint.DataProviderResourceId = @provider
      AND checkpoint.RawImpressionUploadId = @upload
      AND checkpoint.CmmsModelLine = @model
      AND checkpoint.BlobType = @snapshot
      AND checkpoint.DeleteTime IS NULL
      AND newer.BlobType = @snapshot
      AND newer.DeleteTime IS NULL
      AND newer.RawImpressionUploadId != checkpoint.RawImpressionUploadId
      AND newer.CreateTime > checkpoint.CreateTime
      AND (
        model.State = @completed
        OR (model.State = @availability AND COALESCE(owner.RegistrationComplete, FALSE))
      )
    LIMIT 1
    """
        .trimIndent()
    ) {
      bind("provider").to(dataProviderResourceId)
      bind("upload").to(rawImpressionUploadId)
      bind("model").to(cmmsModelLine)
      bind("snapshot").to(BlobType.BLOB_TYPE_SNAPSHOT)
      bind("completed").to(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED)
      bind("availability").to(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_AVAILABILITY_SYNCING)
    }
  val row =
    executeQuery(query, Options.tag("action=findNewerSnapshotBasis")).singleOrNullIfEmpty()
      ?: return null
  return InProgressModelLine(
    row.getString("RawImpressionUploadResourceId"),
    row.getProtoEnum("State", State::forNumber),
  )
}

/** Buffers an update to the parent [RawImpressionUpload] row's state. */
fun AsyncDatabaseClient.TransactionContext.updateRawImpressionUploadState(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  state: RawImpressionUploadState,
) {
  bufferUpdateMutation("RawImpressionUpload") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("State").to(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/**
 * Reads a [RawImpressionUploadModelLine] by its composite resource IDs.
 *
 * @return The [RawImpressionUploadModelLineResult], or `null` if not found
 */
suspend fun AsyncDatabaseClient.ReadContext.getRawImpressionUploadModelLineByResourceIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  rawImpressionUploadModelLineResourceId: String,
): RawImpressionUploadModelLineResult? {
  val sql = buildString {
    appendLine(RawImpressionUploadModelLineEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE RawImpressionUploadModelLine.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RawImpressionUploadModelLine.RawImpressionUploadModelLineResourceId =
          @rawImpressionUploadModelLineResourceId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("rawImpressionUploadModelLineResourceId").to(rawImpressionUploadModelLineResourceId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return RawImpressionUploadModelLineEntity.buildResult(row)
}

/** Finds an existing [RawImpressionUploadModelLine] by request ID for idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findRawImpressionUploadModelLineByRequestId(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestId: String,
): RawImpressionUploadModelLineResult? {
  val sql = buildString {
    appendLine(RawImpressionUploadModelLineEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE RawImpressionUploadModelLine.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RawImpressionUploadModelLine.CreateRequestId = @createRequestId
      """
        .trimIndent()
    )
  }

  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("createRequestId").to(requestId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return RawImpressionUploadModelLineEntity.buildResult(row)
}

/** Finds existing [RawImpressionUploadModelLine] entries by request IDs for batch idempotency. */
suspend fun AsyncDatabaseClient.ReadContext.findRawImpressionUploadModelLinesByRequestIds(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  requestIds: List<String>,
): Map<String, RawImpressionUploadModelLineResult> {
  if (requestIds.isEmpty()) return emptyMap()

  val sql = buildString {
    appendLine(RawImpressionUploadModelLineEntity.BASE_SQL)
    appendLine(
      """
      JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)
      WHERE RawImpressionUploadModelLine.DataProviderResourceId = @dataProviderResourceId
        AND RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
        AND RawImpressionUploadModelLine.CreateRequestId IN UNNEST(@createRequestIds)
      """
        .trimIndent()
    )
  }

  return buildMap {
    executeQuery(
        statement(sql) {
          bind("dataProviderResourceId").to(dataProviderResourceId)
          bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
          bind("createRequestIds").toStringArray(requestIds)
        },
        Options.tag("action=findRawImpressionUploadModelLinesByRequestIds"),
      )
      .collect { row ->
        val result = RawImpressionUploadModelLineEntity.buildResult(row)
        if (!row.isNull("CreateRequestId")) {
          put(row.getString("CreateRequestId"), result)
        }
      }
  }
}

/** Reads [RawImpressionUploadModelLine] entries with filtering and pagination. */
fun AsyncDatabaseClient.ReadContext.readRawImpressionUploadModelLines(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String?,
  filter: ListRawImpressionUploadModelLinesRequest.Filter?,
  limit: Int,
  after: ListRawImpressionUploadModelLinesPageToken.After? = null,
): Flow<RawImpressionUploadModelLineResult> {
  val sql = buildString {
    appendLine(RawImpressionUploadModelLineEntity.BASE_SQL)
    appendLine("JOIN RawImpressionUpload USING (DataProviderResourceId, RawImpressionUploadId)")

    val conjuncts =
      mutableListOf("RawImpressionUploadModelLine.DataProviderResourceId = @dataProviderResourceId")

    if (rawImpressionUploadResourceId != null) {
      conjuncts.add(
        "RawImpressionUpload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId"
      )
    }

    if (filter != null) {
      if (filter.stateInList.isNotEmpty()) {
        conjuncts.add("CAST(RawImpressionUploadModelLine.State AS INT64) IN UNNEST(@state_in)")
      }
      if (filter.hasCreateTimeIn()) {
        if (filter.createTimeIn.hasStartTime()) {
          conjuncts.add("RawImpressionUploadModelLine.CreateTime >= @createTimeStart")
        }
        if (filter.createTimeIn.hasEndTime()) {
          conjuncts.add("RawImpressionUploadModelLine.CreateTime < @createTimeEnd")
        }
      }
      if (filter.cmmsModelLine.isNotEmpty()) {
        conjuncts.add("RawImpressionUploadModelLine.CmmsModelLine = @cmmsModelLine")
      }
    }

    if (after != null) {
      conjuncts.add(
        "(RawImpressionUploadModelLine.CreateTime > @afterCreateTime OR (RawImpressionUploadModelLine.CreateTime = @afterCreateTime AND RawImpressionUploadModelLine.CmmsModelLine > @afterCmmsModelLine) OR (RawImpressionUploadModelLine.CreateTime = @afterCreateTime AND RawImpressionUploadModelLine.CmmsModelLine = @afterCmmsModelLine AND RawImpressionUpload.RawImpressionUploadResourceId > @afterRawImpressionUploadResourceId))"
      )
    }

    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine(
      "ORDER BY RawImpressionUploadModelLine.CreateTime ASC, " +
        "RawImpressionUploadModelLine.CmmsModelLine ASC, " +
        "RawImpressionUpload.RawImpressionUploadResourceId ASC"
    )
    appendLine("LIMIT @limit")
  }

  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      if (rawImpressionUploadResourceId != null) {
        bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
      }
      bind("limit").to(limit.toLong())

      if (filter != null) {
        if (filter.stateInList.isNotEmpty()) {
          bind("state_in").toInt64Array(filter.stateInList.map { it.number.toLong() })
        }
        if (filter.hasCreateTimeIn()) {
          if (filter.createTimeIn.hasStartTime()) {
            bind("createTimeStart").to(filter.createTimeIn.startTime.toGcloudTimestamp())
          }
          if (filter.createTimeIn.hasEndTime()) {
            bind("createTimeEnd").to(filter.createTimeIn.endTime.toGcloudTimestamp())
          }
        }
        if (filter.cmmsModelLine.isNotEmpty()) {
          bind("cmmsModelLine").to(filter.cmmsModelLine)
        }
      }

      if (after != null) {
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
        bind("afterCmmsModelLine").to(after.cmmsModelLine)
        bind("afterRawImpressionUploadResourceId").to(after.rawImpressionUploadResourceId)
      }
    }

  return executeQuery(query, Options.tag("action=readRawImpressionUploadModelLines")).map { row ->
    RawImpressionUploadModelLineEntity.buildResult(row)
  }
}

/** Buffers an insert mutation for a [RawImpressionUploadModelLine] row. */
fun AsyncDatabaseClient.TransactionContext.insertRawImpressionUploadModelLine(
  rawImpressionUploadId: Long,
  rawImpressionUploadModelLineId: Long,
  rawImpressionUploadModelLineResourceId: String,
  dataProviderResourceId: String,
  cmmsModelLine: String,
  createRequestId: String,
) {
  bufferInsertMutation("RawImpressionUploadModelLine") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("RawImpressionUploadModelLineId").to(rawImpressionUploadModelLineId)
    set("RawImpressionUploadModelLineResourceId").to(rawImpressionUploadModelLineResourceId)
    set("CmmsModelLine").to(cmmsModelLine)
    set("CreateRequestId").to(createRequestId)
    set("State").to(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an update to a [RawImpressionUploadModelLine] row's state and related fields. */
fun AsyncDatabaseClient.TransactionContext.updateRawImpressionUploadModelLineState(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  rawImpressionUploadModelLineId: Long,
  state: State,
  block: (Mutation.WriteBuilder.() -> Unit)? = null,
) {
  bufferUpdateMutation("RawImpressionUploadModelLine") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RawImpressionUploadId").to(rawImpressionUploadId)
    set("RawImpressionUploadModelLineId").to(rawImpressionUploadModelLineId)
    set("State").to(state)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    if (block != null) {
      block()
    }
  }
}

private object RawImpressionUploadModelLineEntity {
  val BASE_SQL =
    """
    SELECT
      RawImpressionUploadModelLine.DataProviderResourceId,
      RawImpressionUpload.RawImpressionUploadResourceId,
      RawImpressionUploadModelLine.RawImpressionUploadId,
      RawImpressionUploadModelLine.RawImpressionUploadModelLineId,
      RawImpressionUploadModelLine.RawImpressionUploadModelLineResourceId,
      RawImpressionUploadModelLine.CmmsModelLine,
      RawImpressionUploadModelLine.CreateRequestId,
      RawImpressionUploadModelLine.State,
      RawImpressionUploadModelLine.ErrorMessage,
      RawImpressionUploadModelLine.CreateTime,
      RawImpressionUploadModelLine.UpdateTime,
      RawImpressionUploadModelLine.PoolOffsets,
      RawImpressionUploadModelLine.MaxEventDate,
      RawImpressionUploadModelLine.EncryptedMergedDek,
      RawImpressionUploadModelLine.MarkPoolAssigningRequestId,
      RawImpressionUploadModelLine.MarkRankingRequestId,
      RawImpressionUploadModelLine.MarkLabelingRequestId,
      RawImpressionUploadModelLine.MarkAvailabilitySyncingRequestId,
      RawImpressionUploadModelLine.MarkCompletedRequestId,
      RawImpressionUploadModelLine.MarkFailedRequestId,
      RawImpressionUploadModelLine.FailureReason,
      RawImpressionUploadModelLine.EvictionOperationId,
      RawImpressionUploadModelLine.RecoveryAction,
      RawImpressionUploadModelLine.RecoveryPredecessorRawImpressionUploadResourceId,
      RawImpressionUploadModelLine.PendingAvailabilityDates,
      RawImpressionUploadModelLine.SynchronizedAvailabilityDates,
    FROM
      RawImpressionUploadModelLine
    """
      .trimIndent()

  fun buildResult(struct: Struct): RawImpressionUploadModelLineResult {
    fun markId(column: String): String = if (struct.isNull(column)) "" else struct.getString(column)
    return RawImpressionUploadModelLineResult(
      rawImpressionUploadModelLine {
        dataProviderResourceId = struct.getString("DataProviderResourceId")
        rawImpressionUploadResourceId = struct.getString("RawImpressionUploadResourceId")
        rawImpressionUploadModelLineResourceId =
          struct.getString("RawImpressionUploadModelLineResourceId")
        cmmsModelLine = struct.getString("CmmsModelLine")
        state = struct.getProtoEnum("State", State::forNumber)
        createTime = struct.getTimestamp("CreateTime").toProto()
        updateTime = struct.getTimestamp("UpdateTime").toProto()
        etag = ETags.computeETag(updateTime.toInstant())
        if (!struct.isNull("ErrorMessage")) {
          errorMessage = struct.getString("ErrorMessage")
        }
        // Phase-0 last-shard-out outputs on the parent row, read back by a retrying
        // SubpoolAssigner (see SubpoolAssigner.recoverIfLastShardOut). All three are
        // populated together on last-shard-out; null until then.
        if (!struct.isNull("PoolOffsets")) {
          poolOffsets += struct.getLongList("PoolOffsets")
        }
        if (!struct.isNull("MaxEventDate")) {
          maxEventDate = struct.getDate("MaxEventDate").toProtoDate()
        }
        if (!struct.isNull("EncryptedMergedDek")) {
          encryptedMergedDek =
            struct.getProtoMessage("EncryptedMergedDek", EncryptedDek.getDefaultInstance())
        }
        failureAttemptId = markId("MarkFailedRequestId")
        if (!struct.isNull("FailureReason")) {
          failureReason =
            struct.getProtoEnum(
              "FailureReason",
              RawImpressionUploadModelLineFailureReason::forNumber,
            )
        }
        if (!struct.isNull("EvictionOperationId")) {
          evictionOperationId = struct.getString("EvictionOperationId")
        }
        if (!struct.isNull("RecoveryAction")) {
          recoveryAction =
            struct.getProtoEnum(
              "RecoveryAction",
              RawImpressionUploadModelLineRecoveryAction::forNumber,
            )
        }
        if (!struct.isNull("RecoveryPredecessorRawImpressionUploadResourceId")) {
          recoveryPredecessorRawImpressionUploadResourceId =
            struct.getString("RecoveryPredecessorRawImpressionUploadResourceId")
        }
        if (!struct.isNull("PendingAvailabilityDates")) {
          pendingAvailabilityDates +=
            struct.getDateList("PendingAvailabilityDates").map { it.toProtoDate() }
        }
        if (!struct.isNull("SynchronizedAvailabilityDates")) {
          synchronizedAvailabilityDates +=
            struct.getDateList("SynchronizedAvailabilityDates").map { it.toProtoDate() }
        }
      },
      struct.getLong("RawImpressionUploadId"),
      struct.getLong("RawImpressionUploadModelLineId"),
      markPoolAssigningRequestId = markId("MarkPoolAssigningRequestId"),
      markRankingRequestId = markId("MarkRankingRequestId"),
      markLabelingRequestId = markId("MarkLabelingRequestId"),
      markAvailabilitySyncingRequestId = markId("MarkAvailabilitySyncingRequestId"),
      markCompletedRequestId = markId("MarkCompletedRequestId"),
      markFailedRequestId = markId("MarkFailedRequestId"),
    )
  }
}
