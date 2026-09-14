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
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState

/** Returns the operation ID holding the VID-labeling eviction fence, if any. */
suspend fun AsyncDatabaseClient.ReadContext.getVidLabelingEvictionOperationId(
  dataProviderResourceId: String
): String? {
  val row =
    readRow(
      "VidLabelingEvictionFence",
      Key.of(dataProviderResourceId),
      listOf("EvictionOperationId"),
    ) ?: return null
  return row.getString("EvictionOperationId")
}

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
    LIMIT 1
    """
      .trimIndent()
  return executeQuery(
      statement(sql) { bind("dataProviderResourceId").to(dataProviderResourceId) },
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
      AND State IN UNNEST(@activeStates)
    LIMIT 1
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("dataProviderResourceId").to(dataProviderResourceId)
        bind("activeStates")
          .toProtoEnumArray(activeStates, RawImpressionUploadModelLineState.getDescriptor())
      },
      Options.tag("action=hasActiveRawImpressionUploadModelLine"),
    )
    .singleOrNullIfEmpty() != null
}

/** Buffers creation of the VID-labeling eviction fence. */
fun AsyncDatabaseClient.TransactionContext.insertVidLabelingEvictionFence(
  dataProviderResourceId: String,
  evictionOperationId: String,
) {
  bufferInsertMutation("VidLabelingEvictionFence") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("EvictionOperationId").to(evictionOperationId)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers deletion of the VID-labeling eviction fence. */
fun AsyncDatabaseClient.TransactionContext.deleteVidLabelingEvictionFence(
  dataProviderResourceId: String
) {
  buffer(Mutation.delete("VidLabelingEvictionFence", Key.of(dataProviderResourceId)))
}
