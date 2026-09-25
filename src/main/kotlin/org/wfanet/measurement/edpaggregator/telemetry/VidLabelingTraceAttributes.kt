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

package org.wfanet.measurement.edpaggregator.telemetry

import io.opentelemetry.api.common.AttributeKey
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

/** Stable OpenTelemetry attributes for one VID-labeling pipeline execution. */
object VidLabelingTraceAttributes {
  data class GcsObjectIdentity(val pathHash: String, val generation: Long)

  /** Returns the stable, payload-free identity for one exact GCS object version. */
  fun gcsObjectIdentity(uri: String, generation: Long): GcsObjectIdentity =
    GcsObjectIdentity(gcsObjectPathHash(uri), generation)

  /** Returns a SHA-256 digest of a full `gs://bucket/key` URI. */
  fun gcsObjectPathHash(uri: String): String =
    MessageDigest.getInstance("SHA-256").digest(uri.toByteArray(UTF_8)).joinToString("") { byte ->
      (byte.toInt() and 0xff).toString(16).padStart(2, '0')
    }

  const val RAW_IMPRESSION_UPLOAD_METADATA_KEY = "xmm-raw-impression-upload"
  const val MODEL_LINE_METADATA_KEY = "xmm-model-line"
  const val VID_LABELING_JOB_METADATA_KEY = "xmm-vid-labeling-job"
  const val TRACEPARENT_METADATA_KEY = "xmm-traceparent"
  const val TRACESTATE_METADATA_KEY = "xmm-tracestate"
  const val TRACE_CONTEXT_SOURCE_METADATA_KEY = "xmm-trace-context-source"
  const val TRACE_CONTEXT_SOURCE_VID_LABELER = "vid-labeler"
  val PERSISTED_BOUNDARY_METADATA_KEYS: Set<String> =
    setOf(
      RAW_IMPRESSION_UPLOAD_METADATA_KEY,
      MODEL_LINE_METADATA_KEY,
      VID_LABELING_JOB_METADATA_KEY,
      TRACEPARENT_METADATA_KEY,
      TRACESTATE_METADATA_KEY,
      TRACE_CONTEXT_SOURCE_METADATA_KEY,
    )
  const val RAW_IMPRESSION_UPLOAD_HEADER = "X-Raw-Impression-Upload"
  const val MODEL_LINE_HEADER = "X-Model-Line"
  const val VID_LABELING_JOB_HEADER = "X-Vid-Labeling-Job"
  const val DATA_WATCHER_GENERATION_HEADER = "X-DataWatcher-Generation"
  const val DATA_PROVIDER_NAME_STRING = "xmm.data_provider.name"
  const val MODEL_LINE_NAME_STRING = "xmm.model_line.name"
  const val MODEL_LINE_NAMES_STRING = "xmm.model_line.names"
  const val RAW_IMPRESSION_UPLOAD_NAME_STRING = "xmm.edpa.raw_impression_upload.name"
  const val RAW_IMPRESSION_UPLOAD_MODEL_LINE_NAME_STRING =
    "xmm.edpa.raw_impression_upload_model_line.name"
  const val POOL_ASSIGNMENT_JOB_NAME_STRING = "xmm.edpa.pool_assignment_job.name"
  const val RANKER_JOB_NAME_STRING = "xmm.edpa.ranker_job.name"
  const val VID_LABELING_JOB_NAME_STRING = "xmm.edpa.vid_labeling_job.name"
  const val RANK_INDEX_BLOB_NAME_STRING = "xmm.edpa.rank_index_blob.name"
  const val RANK_INDEX_BLOB_TYPE_STRING = "xmm.edpa.rank_index_blob.type"
  const val IMPRESSION_METADATA_NAME_STRING = "xmm.edpa.impression_metadata.name"
  const val RECOVERY_WORK_ITEM_NAME_STRING = "xmm.edpa.recovery_work_item.name"
  const val PIPELINE_PHASE_STRING = "xmm.edpa.pipeline.phase"
  const val GCS_OBJECT_GENERATION_STRING = "xmm.gcs.object.generation"
  const val GCS_OBJECT_PATH_HASH_STRING = "xmm.gcs.object.path_hash"
  const val POOL_OFFSET_STRING = "xmm.edpa.pool_offset"
  const val SHARD_INDEX_STRING = "xmm.edpa.shard_index"
  const val RANK_ALLOCATED_STRING = "xmm.edpa.rank.allocated"
  const val RANK_RENEWED_STRING = "xmm.edpa.rank.renewed"
  const val RANK_OVERFLOW_STRING = "xmm.edpa.rank.overflow"
  const val RANK_FREED_STRING = "xmm.edpa.rank.freed"
  const val RANK_BACKFILL_REUSED_STRING = "xmm.edpa.rank.backfill_reused"
  const val RANK_BACKFILL_COLLISIONS_STRING = "xmm.edpa.rank.backfill_collisions"
  const val LABEL_ROUTE_STRING = "xmm.edpa.label.route"
  const val LABEL_INPUT_FILE_COUNT_STRING = "xmm.edpa.label.input_file_count"
  const val LABEL_OUTPUT_TYPE_STRING = "xmm.edpa.label.output_type"
  const val LABEL_EVENT_DATE_STRING = "xmm.edpa.label.event_date"
  const val LABEL_EXPECTED_FINALIZATIONS_STRING = "xmm.edpa.label.expected_finalizations"
  const val LABEL_DONE_OBJECTS_WRITTEN_STRING = "xmm.edpa.label.done_objects_written"
  const val LABEL_PARENTS_COMPLETED_STRING = "xmm.edpa.label.parents_completed"
  const val IMPRESSION_METADATA_ACTION_STRING = "xmm.edpa.impression_metadata.action"
  const val AVAILABILITY_INTERVAL_START_STRING = "xmm.edpa.availability.interval_start"
  const val AVAILABILITY_INTERVAL_END_STRING = "xmm.edpa.availability.interval_end"

  val DATA_PROVIDER_NAME: AttributeKey<String> = AttributeKey.stringKey(DATA_PROVIDER_NAME_STRING)
  val MODEL_LINE_NAME: AttributeKey<String> = AttributeKey.stringKey(MODEL_LINE_NAME_STRING)
  val MODEL_LINE_NAMES: AttributeKey<List<String>> =
    AttributeKey.stringArrayKey(MODEL_LINE_NAMES_STRING)
  val RAW_IMPRESSION_UPLOAD_NAME: AttributeKey<String> =
    AttributeKey.stringKey(RAW_IMPRESSION_UPLOAD_NAME_STRING)
  val RAW_IMPRESSION_UPLOAD_MODEL_LINE_NAME: AttributeKey<String> =
    AttributeKey.stringKey(RAW_IMPRESSION_UPLOAD_MODEL_LINE_NAME_STRING)
  val POOL_ASSIGNMENT_JOB_NAME: AttributeKey<String> =
    AttributeKey.stringKey(POOL_ASSIGNMENT_JOB_NAME_STRING)
  val RANKER_JOB_NAME: AttributeKey<String> = AttributeKey.stringKey(RANKER_JOB_NAME_STRING)
  val VID_LABELING_JOB_NAME: AttributeKey<String> =
    AttributeKey.stringKey(VID_LABELING_JOB_NAME_STRING)
  val RANK_INDEX_BLOB_NAME: AttributeKey<String> =
    AttributeKey.stringKey(RANK_INDEX_BLOB_NAME_STRING)
  val RANK_INDEX_BLOB_TYPE: AttributeKey<String> =
    AttributeKey.stringKey(RANK_INDEX_BLOB_TYPE_STRING)
  val IMPRESSION_METADATA_NAME: AttributeKey<String> =
    AttributeKey.stringKey(IMPRESSION_METADATA_NAME_STRING)
  val RECOVERY_WORK_ITEM_NAME: AttributeKey<String> =
    AttributeKey.stringKey(RECOVERY_WORK_ITEM_NAME_STRING)
  val PIPELINE_PHASE: AttributeKey<String> = AttributeKey.stringKey(PIPELINE_PHASE_STRING)
  val GCS_OBJECT_GENERATION: AttributeKey<Long> = AttributeKey.longKey(GCS_OBJECT_GENERATION_STRING)
  val GCS_OBJECT_PATH_HASH: AttributeKey<String> =
    AttributeKey.stringKey(GCS_OBJECT_PATH_HASH_STRING)
  val POOL_OFFSET: AttributeKey<Long> = AttributeKey.longKey(POOL_OFFSET_STRING)
  val SHARD_INDEX: AttributeKey<Long> = AttributeKey.longKey(SHARD_INDEX_STRING)
  val LABEL_ROUTE: AttributeKey<String> = AttributeKey.stringKey(LABEL_ROUTE_STRING)
  val LABEL_INPUT_FILE_COUNT: AttributeKey<Long> =
    AttributeKey.longKey(LABEL_INPUT_FILE_COUNT_STRING)
  val LABEL_OUTPUT_TYPE: AttributeKey<String> = AttributeKey.stringKey(LABEL_OUTPUT_TYPE_STRING)
  val LABEL_EVENT_DATE: AttributeKey<String> = AttributeKey.stringKey(LABEL_EVENT_DATE_STRING)
  val LABEL_EXPECTED_FINALIZATIONS: AttributeKey<Long> =
    AttributeKey.longKey(LABEL_EXPECTED_FINALIZATIONS_STRING)
  val LABEL_DONE_OBJECTS_WRITTEN: AttributeKey<Long> =
    AttributeKey.longKey(LABEL_DONE_OBJECTS_WRITTEN_STRING)
  val LABEL_PARENTS_COMPLETED: AttributeKey<Long> =
    AttributeKey.longKey(LABEL_PARENTS_COMPLETED_STRING)

  val SAFE_LOG_FIELD_NAMES: Set<String> =
    setOf(
      DATA_PROVIDER_NAME_STRING,
      MODEL_LINE_NAME_STRING,
      RAW_IMPRESSION_UPLOAD_NAME_STRING,
      RAW_IMPRESSION_UPLOAD_MODEL_LINE_NAME_STRING,
      POOL_ASSIGNMENT_JOB_NAME_STRING,
      RANKER_JOB_NAME_STRING,
      VID_LABELING_JOB_NAME_STRING,
      RANK_INDEX_BLOB_NAME_STRING,
      RANK_INDEX_BLOB_TYPE_STRING,
      IMPRESSION_METADATA_NAME_STRING,
      RECOVERY_WORK_ITEM_NAME_STRING,
      PIPELINE_PHASE_STRING,
      GCS_OBJECT_GENERATION_STRING,
      GCS_OBJECT_PATH_HASH_STRING,
      POOL_OFFSET_STRING,
      SHARD_INDEX_STRING,
      RANK_ALLOCATED_STRING,
      RANK_RENEWED_STRING,
      RANK_OVERFLOW_STRING,
      RANK_FREED_STRING,
      RANK_BACKFILL_REUSED_STRING,
      RANK_BACKFILL_COLLISIONS_STRING,
      LABEL_ROUTE_STRING,
      LABEL_INPUT_FILE_COUNT_STRING,
      LABEL_OUTPUT_TYPE_STRING,
      LABEL_EVENT_DATE_STRING,
      LABEL_EXPECTED_FINALIZATIONS_STRING,
      LABEL_DONE_OBJECTS_WRITTEN_STRING,
      LABEL_PARENTS_COMPLETED_STRING,
      IMPRESSION_METADATA_ACTION_STRING,
      AVAILABILITY_INTERVAL_START_STRING,
      AVAILABILITY_INTERVAL_END_STRING,
    )
}
