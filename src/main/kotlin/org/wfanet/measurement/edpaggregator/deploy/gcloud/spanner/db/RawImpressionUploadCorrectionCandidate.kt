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
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate

data class RawImpressionUploadCorrectionCandidateResult(
  val rawImpressionUploadCorrectionCandidate: RawImpressionUploadCorrectionCandidate,
  val createRequestId: String,
  val advanceRequestIds: List<String>,
  val advanceRequestFingerprints: List<ByteString>,
)

data class ExpiredRawImpressionUploadCorrectionCandidate(
  val rawImpressionUploadCorrectionCandidateId: String,
  val rawImpressionUploadResourceId: String,
)

/** Reads a raw-impression upload correction candidate by resource ID. */
suspend fun AsyncDatabaseClient.ReadContext.findRawImpressionUploadCorrectionCandidate(
  dataProviderResourceId: String,
  rawImpressionUploadCorrectionCandidateId: String,
): RawImpressionUploadCorrectionCandidateResult? {
  val sql =
    """
    SELECT
      DataProviderResourceId,
      RawImpressionUploadCorrectionCandidateId,
      RawImpressionUploadResourceId,
      CreateRequestId,
      Classification,
      PriorManifestDigest,
      CurrentManifestDigest,
      ManifestComparison,
      State,
      Decision,
      SupersedingRawImpressionUploadCorrectionCandidateId,
      UploadHealingOperationId,
      ExpireTime,
      AdvanceRequestIds,
      AdvanceRequestFingerprints,
      CreateTime,
      UpdateTime,
    FROM RawImpressionUploadCorrectionCandidate
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadCorrectionCandidateId = @rawImpressionUploadCorrectionCandidateId
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("rawImpressionUploadCorrectionCandidateId")
          .to(rawImpressionUploadCorrectionCandidateId)
      },
      Options.tag("action=findRawImpressionUploadCorrectionCandidate"),
    )
    .singleOrNullIfEmpty()
    ?.let(::buildRawImpressionUploadCorrectionCandidateResult)
}

/** Finds a raw-impression upload correction candidate by create request ID. */
suspend fun AsyncDatabaseClient.ReadContext
  .findRawImpressionUploadCorrectionCandidateByCreateRequestId(
  dataProviderResourceId: String,
  createRequestId: String,
): RawImpressionUploadCorrectionCandidateResult? {
  val sql =
    """
    SELECT
      DataProviderResourceId,
      RawImpressionUploadCorrectionCandidateId,
      RawImpressionUploadResourceId,
      CreateRequestId,
      Classification,
      PriorManifestDigest,
      CurrentManifestDigest,
      ManifestComparison,
      State,
      Decision,
      SupersedingRawImpressionUploadCorrectionCandidateId,
      UploadHealingOperationId,
      ExpireTime,
      AdvanceRequestIds,
      AdvanceRequestFingerprints,
      CreateTime,
      UpdateTime,
    FROM RawImpressionUploadCorrectionCandidate@{
      FORCE_INDEX=RawImpressionUploadCorrectionCandidateByCreateRequestId,
      spanner_emulator.disable_query_null_filtered_index_check=true
    }
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND CreateRequestId = @createRequestId
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("createRequestId").to(createRequestId)
      },
      Options.tag("action=findRawImpressionUploadCorrectionCandidateByCreateRequestId"),
    )
    .singleOrNullIfEmpty()
    ?.let(::buildRawImpressionUploadCorrectionCandidateResult)
}

/** Finds a raw-impression upload correction candidate by lifecycle request ID. */
suspend fun AsyncDatabaseClient.ReadContext
  .findRawImpressionUploadCorrectionCandidateByAdvanceRequestId(
  dataProviderResourceId: String,
  requestId: String,
): RawImpressionUploadCorrectionCandidateResult? {
  val sql =
    """
    SELECT
      DataProviderResourceId,
      RawImpressionUploadCorrectionCandidateId,
      RawImpressionUploadResourceId,
      CreateRequestId,
      Classification,
      PriorManifestDigest,
      CurrentManifestDigest,
      ManifestComparison,
      State,
      Decision,
      SupersedingRawImpressionUploadCorrectionCandidateId,
      UploadHealingOperationId,
      ExpireTime,
      AdvanceRequestIds,
      AdvanceRequestFingerprints,
      CreateTime,
      UpdateTime,
    FROM RawImpressionUploadCorrectionCandidate
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND @requestId IN UNNEST(AdvanceRequestIds)
    LIMIT 1
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("requestId").to(requestId)
      },
      Options.tag("action=findRawImpressionUploadCorrectionCandidateByAdvanceRequestId"),
    )
    .singleOrNullIfEmpty()
    ?.let(::buildRawImpressionUploadCorrectionCandidateResult)
}

/** Returns whether a quarantined upload is assigned to a healing operation. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionUploadCorrectionCandidateHasOperation(
  dataProviderResourceId: String,
  rawImpressionUploadResourceId: String,
  uploadHealingOperationId: String,
): Boolean {
  val sql =
    """
    SELECT RawImpressionUploadCorrectionCandidateId
    FROM RawImpressionUploadCorrectionCandidate
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadResourceId = @rawImpressionUploadResourceId
      AND UploadHealingOperationId = @uploadHealingOperationId
    LIMIT 1
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
        bind("uploadHealingOperationId").to(uploadHealingOperationId)
      },
      Options.tag("action=rawImpressionUploadCorrectionCandidateHasOperation"),
    )
    .singleOrNullIfEmpty() != null
}

/** Reads raw-impression upload correction candidates in creation order. */
fun AsyncDatabaseClient.ReadContext.readRawImpressionUploadCorrectionCandidates(
  dataProviderResourceId: String,
  filter: ListRawImpressionUploadCorrectionCandidatesRequest.Filter,
  limit: Int,
  after: ListRawImpressionUploadCorrectionCandidatesPageToken.After? = null,
): Flow<RawImpressionUploadCorrectionCandidateResult> {
  val sql = buildString {
    appendLine(
      """
      SELECT
        DataProviderResourceId,
        RawImpressionUploadCorrectionCandidateId,
        RawImpressionUploadResourceId,
        CreateRequestId,
        Classification,
        PriorManifestDigest,
        CurrentManifestDigest,
        ManifestComparison,
        State,
        Decision,
        SupersedingRawImpressionUploadCorrectionCandidateId,
        UploadHealingOperationId,
        ExpireTime,
        AdvanceRequestIds,
        AdvanceRequestFingerprints,
        CreateTime,
        UpdateTime,
      FROM RawImpressionUploadCorrectionCandidate
      """
        .trimIndent()
    )
    val conjuncts = mutableListOf("DataProviderResourceId = @dataProviderResourceId")
    if (filter.stateInList.isNotEmpty()) {
      conjuncts += "CAST(State AS INT64) IN UNNEST(@stateIn)"
    }
    if (filter.classificationInList.isNotEmpty()) {
      conjuncts += "CAST(Classification AS INT64) IN UNNEST(@classificationIn)"
    }
    if (after != null) {
      conjuncts +=
        "((CreateTime > @afterCreateTime) OR " +
          "(CreateTime = @afterCreateTime AND RawImpressionUploadCorrectionCandidateId > @afterCandidateId))"
    }
    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine("ORDER BY CreateTime, RawImpressionUploadCorrectionCandidateId")
    appendLine("LIMIT @limit")
  }
  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("limit").to(limit.toLong())
      if (filter.stateInList.isNotEmpty()) {
        bind("stateIn").toInt64Array(filter.stateInList.map { it.number.toLong() })
      }
      if (filter.classificationInList.isNotEmpty()) {
        bind("classificationIn")
          .toInt64Array(filter.classificationInList.map { it.number.toLong() })
      }
      if (after != null) {
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
        bind("afterCandidateId").to(after.rawImpressionUploadCorrectionCandidateId)
      }
    }
  return executeQuery(query, Options.tag("action=readRawImpressionUploadCorrectionCandidates"))
    .map { buildRawImpressionUploadCorrectionCandidateResult(it) }
}

/** Reads expired correction candidates in one terminal state. */
fun AsyncDatabaseClient.ReadContext.readExpiredRawImpressionUploadCorrectionCandidates(
  dataProviderResourceId: String,
  state: RawImpressionUploadCorrectionCandidate.State,
  now: Instant,
  limit: Int,
): Flow<ExpiredRawImpressionUploadCorrectionCandidate> {
  val sql =
    """
    SELECT
      candidate.RawImpressionUploadCorrectionCandidateId,
      candidate.RawImpressionUploadResourceId,
    FROM RawImpressionUploadCorrectionCandidate@{
      FORCE_INDEX=RawImpressionUploadCorrectionCandidateByStateAndExpireTime
    } AS candidate
    LEFT JOIN UploadHealingOperation AS operation
      ON operation.DataProviderResourceId = candidate.DataProviderResourceId
      AND operation.UploadHealingOperationId = candidate.UploadHealingOperationId
    WHERE candidate.DataProviderResourceId = @dataProviderResourceId
      AND candidate.State = @state
      AND candidate.ExpireTime <= @now
      AND (
        candidate.UploadHealingOperationId IS NULL
        OR operation.State = @completeOperationState
      )
    ORDER BY candidate.ExpireTime, candidate.RawImpressionUploadCorrectionCandidateId
    LIMIT @limit
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("state").to(Value.protoEnum(state))
        bind("now").to(now.toGcloudTimestamp())
        bind("completeOperationState")
          .to(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE.number.toLong())
        bind("limit").to(limit.toLong())
      },
      Options.tag("action=readExpiredRawImpressionUploadCorrectionCandidates"),
    )
    .map { row ->
      ExpiredRawImpressionUploadCorrectionCandidate(
        row.getString("RawImpressionUploadCorrectionCandidateId"),
        row.getString("RawImpressionUploadResourceId"),
      )
    }
}

/** Buffers a raw-impression upload correction candidate insert. */
fun AsyncDatabaseClient.TransactionContext.insertRawImpressionUploadCorrectionCandidate(
  rawImpressionUploadCorrectionCandidate: RawImpressionUploadCorrectionCandidate,
  createRequestId: String,
) {
  bufferInsertMutation("RawImpressionUploadCorrectionCandidate") {
    set("DataProviderResourceId").to(rawImpressionUploadCorrectionCandidate.dataProviderResourceId)
    set("RawImpressionUploadCorrectionCandidateId")
      .to(rawImpressionUploadCorrectionCandidate.rawImpressionUploadCorrectionCandidateId)
    set("RawImpressionUploadResourceId")
      .to(rawImpressionUploadCorrectionCandidate.rawImpressionUploadResourceId)
    set("CreateRequestId").to(createRequestId)
    set("Classification").to(rawImpressionUploadCorrectionCandidate.classification)
    set("PriorManifestDigest")
      .to(rawImpressionUploadCorrectionCandidate.priorManifestDigest.toGcloudByteArray())
    set("CurrentManifestDigest")
      .to(rawImpressionUploadCorrectionCandidate.currentManifestDigest.toGcloudByteArray())
    set("ManifestComparison")
      .to(
        rawImpressionUploadCorrectionCandidate.manifestComparison.toByteArray().toGcloudByteArray()
      )
    set("State").to(RawImpressionUploadCorrectionCandidate.State.STATE_PENDING)
    set("Decision").to(RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED)
    set("AdvanceRequestIds").toStringArray(emptyList())
    set("AdvanceRequestFingerprints").toBytesArray(emptyList())
    set("ExpireTime").to(rawImpressionUploadCorrectionCandidate.expireTime.toGcloudTimestamp())
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers deletion of a correction candidate. */
fun AsyncDatabaseClient.TransactionContext.deleteRawImpressionUploadCorrectionCandidate(
  dataProviderResourceId: String,
  rawImpressionUploadCorrectionCandidateId: String,
) {
  buffer(
    Mutation.delete(
      "RawImpressionUploadCorrectionCandidate",
      Key.of(dataProviderResourceId, rawImpressionUploadCorrectionCandidateId),
    )
  )
}

/** Buffers a raw-impression upload correction candidate lifecycle update. */
fun AsyncDatabaseClient.TransactionContext.updateRawImpressionUploadCorrectionCandidate(
  rawImpressionUploadCorrectionCandidate: RawImpressionUploadCorrectionCandidate,
  advanceRequestIds: List<String>,
  advanceRequestFingerprints: List<ByteString>,
) {
  bufferUpdateMutation("RawImpressionUploadCorrectionCandidate") {
    set("DataProviderResourceId").to(rawImpressionUploadCorrectionCandidate.dataProviderResourceId)
    set("RawImpressionUploadCorrectionCandidateId")
      .to(rawImpressionUploadCorrectionCandidate.rawImpressionUploadCorrectionCandidateId)
    set("State").to(rawImpressionUploadCorrectionCandidate.state)
    set("Decision").to(rawImpressionUploadCorrectionCandidate.decision)
    if (
      rawImpressionUploadCorrectionCandidate.supersedingRawImpressionUploadCorrectionCandidateId
        .isNotEmpty()
    ) {
      set("SupersedingRawImpressionUploadCorrectionCandidateId")
        .to(
          rawImpressionUploadCorrectionCandidate.supersedingRawImpressionUploadCorrectionCandidateId
        )
    }
    if (rawImpressionUploadCorrectionCandidate.uploadHealingOperationId.isNotEmpty()) {
      set("UploadHealingOperationId")
        .to(rawImpressionUploadCorrectionCandidate.uploadHealingOperationId)
    }
    set("AdvanceRequestIds").toStringArray(advanceRequestIds)
    set("AdvanceRequestFingerprints")
      .toBytesArray(advanceRequestFingerprints.map { it.toGcloudByteArray() })
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Associates a correction candidate with a persisted healing plan. */
fun AsyncDatabaseClient.TransactionContext.assignRawImpressionUploadCorrectionCandidate(
  candidate: RawImpressionUploadCorrectionCandidate,
  uploadHealingOperationId: String,
) {
  bufferUpdateMutation("RawImpressionUploadCorrectionCandidate") {
    set("DataProviderResourceId").to(candidate.dataProviderResourceId)
    set("RawImpressionUploadCorrectionCandidateId")
      .to(candidate.rawImpressionUploadCorrectionCandidateId)
    set("UploadHealingOperationId").to(uploadHealingOperationId)
    set("State").to(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Records the operator decision for a correction candidate. */
fun AsyncDatabaseClient.TransactionContext.approveRawImpressionUploadCorrectionCandidate(
  candidate: RawImpressionUploadCorrectionCandidate,
  decision: RawImpressionUploadCorrectionCandidate.Decision,
) {
  bufferUpdateMutation("RawImpressionUploadCorrectionCandidate") {
    set("DataProviderResourceId").to(candidate.dataProviderResourceId)
    set("RawImpressionUploadCorrectionCandidateId")
      .to(candidate.rawImpressionUploadCorrectionCandidateId)
    set("State").to(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
    set("Decision").to(decision)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Marks an approved correction candidate complete after its healing operation finishes. */
fun AsyncDatabaseClient.TransactionContext.completeRawImpressionUploadCorrectionCandidate(
  candidate: RawImpressionUploadCorrectionCandidate
) {
  bufferUpdateMutation("RawImpressionUploadCorrectionCandidate") {
    set("DataProviderResourceId").to(candidate.dataProviderResourceId)
    set("RawImpressionUploadCorrectionCandidateId")
      .to(candidate.rawImpressionUploadCorrectionCandidateId)
    set("State").to(RawImpressionUploadCorrectionCandidate.State.STATE_COMPLETE)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Returns an approved correction candidate to its mutable healing plan. */
fun AsyncDatabaseClient.TransactionContext.reopenRawImpressionUploadCorrectionCandidate(
  candidate: RawImpressionUploadCorrectionCandidate
) {
  bufferUpdateMutation("RawImpressionUploadCorrectionCandidate") {
    set("DataProviderResourceId").to(candidate.dataProviderResourceId)
    set("RawImpressionUploadCorrectionCandidateId")
      .to(candidate.rawImpressionUploadCorrectionCandidateId)
    set("State").to(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
    set("Decision").to(RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Marks an assigned correction candidate as requiring operator intervention. */
fun AsyncDatabaseClient.TransactionContext
  .markRawImpressionUploadCorrectionCandidateManualInterventionRequired(
  candidate: RawImpressionUploadCorrectionCandidate,
  uploadHealingOperationId: String,
) {
  bufferUpdateMutation("RawImpressionUploadCorrectionCandidate") {
    set("DataProviderResourceId").to(candidate.dataProviderResourceId)
    set("RawImpressionUploadCorrectionCandidateId")
      .to(candidate.rawImpressionUploadCorrectionCandidateId)
    set("UploadHealingOperationId").to(uploadHealingOperationId)
    set("State").to(RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Removes a correction candidate from a mutable healing plan. */
fun AsyncDatabaseClient.TransactionContext.unassignRawImpressionUploadCorrectionCandidate(
  candidate: RawImpressionUploadCorrectionCandidate
) {
  bufferUpdateMutation("RawImpressionUploadCorrectionCandidate") {
    set("DataProviderResourceId").to(candidate.dataProviderResourceId)
    set("RawImpressionUploadCorrectionCandidateId")
      .to(candidate.rawImpressionUploadCorrectionCandidateId)
    set("UploadHealingOperationId").to(null as String?)
    set("State").to(RawImpressionUploadCorrectionCandidate.State.STATE_PENDING)
    set("Decision").to(RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

private fun buildRawImpressionUploadCorrectionCandidateResult(
  row: Struct
): RawImpressionUploadCorrectionCandidateResult {
  val updateTime = row.getTimestamp("UpdateTime").toProto()
  return RawImpressionUploadCorrectionCandidateResult(
    rawImpressionUploadCorrectionCandidate {
      dataProviderResourceId = row.getString("DataProviderResourceId")
      rawImpressionUploadCorrectionCandidateId =
        row.getString("RawImpressionUploadCorrectionCandidateId")
      rawImpressionUploadResourceId = row.getString("RawImpressionUploadResourceId")
      classification =
        row.getProtoEnum(
          "Classification",
          RawImpressionUploadCorrectionCandidate.Classification::forNumber,
        )
      priorManifestDigest = row.getBytes("PriorManifestDigest").toByteArray().toByteString()
      currentManifestDigest = row.getBytes("CurrentManifestDigest").toByteArray().toByteString()
      manifestComparison =
        RawImpressionUploadCorrectionCandidate.ManifestComparison.parseFrom(
          row.getBytes("ManifestComparison").toByteArray()
        )
      state = row.getProtoEnum("State", RawImpressionUploadCorrectionCandidate.State::forNumber)
      decision =
        row.getProtoEnum("Decision", RawImpressionUploadCorrectionCandidate.Decision::forNumber)
      if (!row.isNull("SupersedingRawImpressionUploadCorrectionCandidateId")) {
        supersedingRawImpressionUploadCorrectionCandidateId =
          row.getString("SupersedingRawImpressionUploadCorrectionCandidateId")
      }
      if (!row.isNull("UploadHealingOperationId")) {
        uploadHealingOperationId = row.getString("UploadHealingOperationId")
      }
      expireTime = row.getTimestamp("ExpireTime").toProto()
      createTime = row.getTimestamp("CreateTime").toProto()
      this.updateTime = updateTime
      etag = ETags.computeETag(updateTime.toInstant())
    },
    createRequestId = row.getString("CreateRequestId"),
    advanceRequestIds = row.getStringList("AdvanceRequestIds"),
    advanceRequestFingerprints =
      row.getBytesList("AdvanceRequestFingerprints").map { it.toByteArray().toByteString() },
  )
}
