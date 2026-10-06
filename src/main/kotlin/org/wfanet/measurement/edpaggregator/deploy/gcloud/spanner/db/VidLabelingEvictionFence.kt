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
import com.google.cloud.spanner.Value
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toInt64Array
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState

data class VidLabelingEvictionFence(
  val evictionOperationId: String,
  val state: VidLabelingEvictionFenceState,
  val etag: String,
)

data class VidLabelingEvictionFenceMutation(
  val requestFingerprint: ByteString,
  val resultEtag: String,
  val newlyAcquired: Boolean,
)

/** Reads the VID-labeling eviction fence, if any. */
suspend fun AsyncDatabaseClient.ReadContext.getVidLabelingEvictionFence(
  dataProviderResourceId: String
): VidLabelingEvictionFence? {
  val row =
    readRow(
      "VidLabelingEvictionFence",
      Key.of(dataProviderResourceId),
      listOf("EvictionOperationId", "State", "Etag"),
    ) ?: return null
  return VidLabelingEvictionFence(
    evictionOperationId = row.getString("EvictionOperationId"),
    state =
      if (row.isNull("State")) {
        VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
      } else {
        row.getProtoEnum("State", VidLabelingEvictionFenceState::forNumber)
      },
    etag = row.getString("Etag"),
  )
}

/** Reads a recorded eviction-fence mutation. */
suspend fun AsyncDatabaseClient.ReadContext.findVidLabelingEvictionFenceMutation(
  dataProviderResourceId: String,
  requestId: String,
): VidLabelingEvictionFenceMutation? {
  val row =
    readRow(
      "VidLabelingEvictionFenceMutation",
      Key.of(dataProviderResourceId, requestId),
      listOf("RequestFingerprint", "ResultEtag", "NewlyAcquired"),
    ) ?: return null
  return VidLabelingEvictionFenceMutation(
    requestFingerprint = row.getBytes("RequestFingerprint").toByteArray().toByteString(),
    resultEtag = row.getString("ResultEtag"),
    newlyAcquired = row.getBoolean("NewlyAcquired"),
  )
}

/** Returns the operation ID holding the VID-labeling eviction fence, if any. */
suspend fun AsyncDatabaseClient.ReadContext.getVidLabelingEvictionOperationId(
  dataProviderResourceId: String
): String? = getVidLabelingEvictionFence(dataProviderResourceId)?.evictionOperationId

/** Returns whether any upload registration is incomplete for the data provider. */
suspend fun AsyncDatabaseClient.ReadContext.hasIncompleteRawImpressionUploadRegistration(
  dataProviderResourceId: String
): Boolean {
  val sql =
    """
    SELECT RawImpressionUploadId
    FROM RawImpressionUpload@{FORCE_INDEX=RawImpressionUploadByRegistrationComplete}
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RegistrationComplete = FALSE
      AND State = @createdState
    LIMIT 1
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("createdState")
          .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED))
      },
      Options.tag("action=hasIncompleteRawImpressionUploadRegistration"),
    )
    .singleOrNullIfEmpty() != null
}

/** Returns whether any model line is queued or processing for the data provider. */
suspend fun AsyncDatabaseClient.ReadContext.hasActiveRawImpressionUploadModelLine(
  dataProviderResourceId: String
): Boolean {
  val activeStates =
    listOf(
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED,
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING,
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING,
      RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING,
    )
  val sql =
    """
    SELECT RawImpressionUploadModelLineId
    FROM RawImpressionUploadModelLine@{FORCE_INDEX=RawImpressionUploadModelLineByState}
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND CAST(State AS INT64) IN UNNEST(@activeStates)
    LIMIT 1
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("activeStates").toInt64Array(activeStates.map { it.number.toLong() })
      },
      Options.tag("action=hasActiveRawImpressionUploadModelLine"),
    )
    .singleOrNullIfEmpty() != null
}

/** Returns whether an upload belongs to the specified eviction operation. */
suspend fun AsyncDatabaseClient.ReadContext.rawImpressionUploadHasEvictionOperation(
  dataProviderResourceId: String,
  rawImpressionUploadId: Long,
  evictionOperationId: String,
): Boolean {
  val sql =
    """
    SELECT RawImpressionUploadModelLineId
    FROM RawImpressionUploadModelLine
    WHERE DataProviderResourceId = @dataProviderResourceId
      AND RawImpressionUploadId = @rawImpressionUploadId
      AND EvictionOperationId = @evictionOperationId
    LIMIT 1
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("rawImpressionUploadId").to(rawImpressionUploadId)
        bind("evictionOperationId").to(evictionOperationId)
      },
      Options.tag("action=rawImpressionUploadHasEvictionOperation"),
    )
    .singleOrNullIfEmpty() != null
}

/** Buffers creation of the VID-labeling eviction fence. */
fun AsyncDatabaseClient.TransactionContext.insertVidLabelingEvictionFence(
  dataProviderResourceId: String,
  evictionOperationId: String,
  etag: String,
  state: VidLabelingEvictionFenceState =
    VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING,
) {
  bufferInsertMutation("VidLabelingEvictionFence") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("EvictionOperationId").to(evictionOperationId)
    set("State").to(state)
    set("Etag").to(etag)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers a VID-labeling eviction-fence state update. */
fun AsyncDatabaseClient.TransactionContext.updateVidLabelingEvictionFenceState(
  dataProviderResourceId: String,
  state: VidLabelingEvictionFenceState,
  etag: String,
) {
  bufferUpdateMutation("VidLabelingEvictionFence") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("State").to(state)
    set("Etag").to(etag)
  }
}

/** Buffers an eviction-fence mutation receipt. */
fun AsyncDatabaseClient.TransactionContext.insertVidLabelingEvictionFenceMutation(
  dataProviderResourceId: String,
  requestId: String,
  requestFingerprint: ByteString,
  resultEtag: String,
  newlyAcquired: Boolean = false,
) {
  bufferInsertMutation("VidLabelingEvictionFenceMutation") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("RequestId").to(requestId)
    set("RequestFingerprint").to(requestFingerprint.toGcloudByteArray())
    set("ResultEtag").to(resultEtag)
    set("NewlyAcquired").to(newlyAcquired)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Transfers the fence to the next pending correction plan. */
fun AsyncDatabaseClient.TransactionContext.transferVidLabelingEvictionFence(
  dataProviderResourceId: String,
  evictionOperationId: String,
) {
  bufferUpdateMutation("VidLabelingEvictionFence") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("EvictionOperationId").to(evictionOperationId)
    set("State")
      .to(VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING)
  }
}

/** Buffers deletion of the VID-labeling eviction fence. */
fun AsyncDatabaseClient.TransactionContext.deleteVidLabelingEvictionFence(
  dataProviderResourceId: String
) {
  buffer(Mutation.delete("VidLabelingEvictionFence", Key.of(dataProviderResourceId)))
}
