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

import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
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
import org.wfanet.measurement.internal.edpaggregator.CorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.ListCorrectionCandidatesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.correctionCandidate

data class CorrectionCandidateResult(
  val correctionCandidate: CorrectionCandidate,
  val createRequestId: String,
  val advanceRequestIds: List<String>,
  val advanceRequestFingerprints: List<ByteString>,
)

/** Reads a correction candidate by resource ID. */
suspend fun AsyncDatabaseClient.ReadContext.findCorrectionCandidate(
  dataProviderResourceId: String,
  correctionCandidateId: String,
): CorrectionCandidateResult? {
  val sql =
    """
    SELECT
      DataProviderResourceId,
      CorrectionCandidateId,
      RawImpressionUploadResourceId,
      CreateRequestId,
      Classification,
      PriorManifestDigest,
      CurrentManifestDigest,
      State,
      Decision,
      SupersedingCorrectionCandidateId,
      UploadHealingOperationId,
      ExpireTime,
      AdvanceRequestIds,
      AdvanceRequestFingerprints,
      CreateTime,
      UpdateTime,
    FROM CorrectionCandidate
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND CorrectionCandidateId = @correctionCandidateId
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("correctionCandidateId").to(correctionCandidateId)
      },
      Options.tag("action=findCorrectionCandidate"),
    )
    .singleOrNullIfEmpty()
    ?.let(::buildCorrectionCandidateResult)
}

/** Finds a correction candidate by create request ID. */
suspend fun AsyncDatabaseClient.ReadContext.findCorrectionCandidateByCreateRequestId(
  dataProviderResourceId: String,
  createRequestId: String,
): CorrectionCandidateResult? {
  val sql =
    """
    SELECT
      DataProviderResourceId,
      CorrectionCandidateId,
      RawImpressionUploadResourceId,
      CreateRequestId,
      Classification,
      PriorManifestDigest,
      CurrentManifestDigest,
      State,
      Decision,
      SupersedingCorrectionCandidateId,
      UploadHealingOperationId,
      ExpireTime,
      AdvanceRequestIds,
      AdvanceRequestFingerprints,
      CreateTime,
      UpdateTime,
    FROM CorrectionCandidate@{
      FORCE_INDEX=CorrectionCandidateByCreateRequestId,
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
      Options.tag("action=findCorrectionCandidateByCreateRequestId"),
    )
    .singleOrNullIfEmpty()
    ?.let(::buildCorrectionCandidateResult)
}

/** Finds a correction candidate by lifecycle request ID. */
suspend fun AsyncDatabaseClient.ReadContext.findCorrectionCandidateByAdvanceRequestId(
  dataProviderResourceId: String,
  requestId: String,
): CorrectionCandidateResult? {
  val sql =
    """
    SELECT
      DataProviderResourceId,
      CorrectionCandidateId,
      RawImpressionUploadResourceId,
      CreateRequestId,
      Classification,
      PriorManifestDigest,
      CurrentManifestDigest,
      State,
      Decision,
      SupersedingCorrectionCandidateId,
      UploadHealingOperationId,
      ExpireTime,
      AdvanceRequestIds,
      AdvanceRequestFingerprints,
      CreateTime,
      UpdateTime,
    FROM CorrectionCandidate
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
      Options.tag("action=findCorrectionCandidateByAdvanceRequestId"),
    )
    .singleOrNullIfEmpty()
    ?.let(::buildCorrectionCandidateResult)
}

/** Reads correction candidates in creation order. */
fun AsyncDatabaseClient.ReadContext.readCorrectionCandidates(
  dataProviderResourceId: String,
  filter: ListCorrectionCandidatesRequest.Filter,
  limit: Int,
  after: ListCorrectionCandidatesPageToken.After? = null,
): Flow<CorrectionCandidateResult> {
  val sql = buildString {
    appendLine(
      """
      SELECT
        DataProviderResourceId,
        CorrectionCandidateId,
        RawImpressionUploadResourceId,
        CreateRequestId,
        Classification,
        PriorManifestDigest,
        CurrentManifestDigest,
        State,
        Decision,
        SupersedingCorrectionCandidateId,
        UploadHealingOperationId,
        ExpireTime,
        AdvanceRequestIds,
        AdvanceRequestFingerprints,
        CreateTime,
        UpdateTime,
      FROM CorrectionCandidate
      """
        .trimIndent()
    )
    val conjuncts = mutableListOf("DataProviderResourceId = @dataProviderResourceId")
    if (filter.stateInList.isNotEmpty()) {
      conjuncts += "CAST(State AS INT64) IN UNNEST(@stateIn)"
    }
    if (after != null) {
      conjuncts +=
        "((CreateTime > @afterCreateTime) OR " +
          "(CreateTime = @afterCreateTime AND CorrectionCandidateId > @afterCandidateId))"
    }
    appendLine("WHERE " + conjuncts.joinToString(" AND "))
    appendLine("ORDER BY CreateTime, CorrectionCandidateId")
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
        bind("afterCandidateId").to(after.correctionCandidateId)
      }
    }
  return executeQuery(query, Options.tag("action=readCorrectionCandidates")).map {
    buildCorrectionCandidateResult(it)
  }
}

/** Buffers a correction candidate insert. */
fun AsyncDatabaseClient.TransactionContext.insertCorrectionCandidate(
  correctionCandidate: CorrectionCandidate,
  createRequestId: String,
) {
  bufferInsertMutation("CorrectionCandidate") {
    set("DataProviderResourceId").to(correctionCandidate.dataProviderResourceId)
    set("CorrectionCandidateId").to(correctionCandidate.correctionCandidateId)
    set("RawImpressionUploadResourceId").to(correctionCandidate.rawImpressionUploadResourceId)
    set("CreateRequestId").to(createRequestId)
    set("Classification").to(correctionCandidate.classification)
    set("PriorManifestDigest").to(correctionCandidate.priorManifestDigest.toGcloudByteArray())
    set("CurrentManifestDigest").to(correctionCandidate.currentManifestDigest.toGcloudByteArray())
    set("State").to(CorrectionCandidate.State.STATE_PENDING)
    set("Decision").to(CorrectionCandidate.Decision.DECISION_UNSPECIFIED)
    set("AdvanceRequestIds").toStringArray(emptyList())
    set("AdvanceRequestFingerprints").toBytesArray(emptyList())
    set("ExpireTime").to(correctionCandidate.expireTime.toGcloudTimestamp())
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers a correction candidate lifecycle update. */
fun AsyncDatabaseClient.TransactionContext.updateCorrectionCandidate(
  correctionCandidate: CorrectionCandidate,
  advanceRequestIds: List<String>,
  advanceRequestFingerprints: List<ByteString>,
) {
  bufferUpdateMutation("CorrectionCandidate") {
    set("DataProviderResourceId").to(correctionCandidate.dataProviderResourceId)
    set("CorrectionCandidateId").to(correctionCandidate.correctionCandidateId)
    set("State").to(correctionCandidate.state)
    set("Decision").to(correctionCandidate.decision)
    if (correctionCandidate.supersedingCorrectionCandidateId.isNotEmpty()) {
      set("SupersedingCorrectionCandidateId")
        .to(correctionCandidate.supersedingCorrectionCandidateId)
    }
    if (correctionCandidate.uploadHealingOperationId.isNotEmpty()) {
      set("UploadHealingOperationId").to(correctionCandidate.uploadHealingOperationId)
    }
    set("AdvanceRequestIds").toStringArray(advanceRequestIds)
    set("AdvanceRequestFingerprints")
      .toBytesArray(advanceRequestFingerprints.map { it.toGcloudByteArray() })
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

private fun buildCorrectionCandidateResult(row: Struct): CorrectionCandidateResult {
  val updateTime = row.getTimestamp("UpdateTime").toProto()
  return CorrectionCandidateResult(
    correctionCandidate {
      dataProviderResourceId = row.getString("DataProviderResourceId")
      correctionCandidateId = row.getString("CorrectionCandidateId")
      rawImpressionUploadResourceId = row.getString("RawImpressionUploadResourceId")
      classification =
        row.getProtoEnum("Classification", CorrectionCandidate.Classification::forNumber)
      priorManifestDigest = row.getBytes("PriorManifestDigest").toByteArray().toByteString()
      currentManifestDigest = row.getBytes("CurrentManifestDigest").toByteArray().toByteString()
      state = row.getProtoEnum("State", CorrectionCandidate.State::forNumber)
      decision = row.getProtoEnum("Decision", CorrectionCandidate.Decision::forNumber)
      if (!row.isNull("SupersedingCorrectionCandidateId")) {
        supersedingCorrectionCandidateId = row.getString("SupersedingCorrectionCandidateId")
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
