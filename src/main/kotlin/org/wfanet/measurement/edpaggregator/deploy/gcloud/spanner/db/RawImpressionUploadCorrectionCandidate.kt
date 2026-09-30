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
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate

data class RawImpressionUploadCorrectionCandidateResult(
  val rawImpressionUploadCorrectionCandidate: RawImpressionUploadCorrectionCandidate,
  val createRequestId: String,
  val advanceRequestIds: List<String>,
  val advanceRequestFingerprints: List<ByteString>,
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
