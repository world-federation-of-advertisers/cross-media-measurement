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

/** Stable OpenTelemetry attributes for one VID-labeling pipeline execution. */
object VidLabelingTraceAttributes {
  const val DATA_PROVIDER_NAME_STRING = "xmm.data_provider.name"
  const val MODEL_LINE_NAME_STRING = "xmm.model_line.name"
  const val RAW_IMPRESSION_UPLOAD_NAME_STRING = "xmm.edpa.raw_impression_upload.name"
  const val RAW_IMPRESSION_UPLOAD_MODEL_LINE_NAME_STRING =
    "xmm.edpa.raw_impression_upload_model_line.name"
  const val POOL_ASSIGNMENT_JOB_NAME_STRING = "xmm.edpa.pool_assignment_job.name"
  const val RANKER_JOB_NAME_STRING = "xmm.edpa.ranker_job.name"
  const val VID_LABELING_JOB_NAME_STRING = "xmm.edpa.vid_labeling_job.name"
  const val RANK_INDEX_BLOB_NAME_STRING = "xmm.edpa.rank_index_blob.name"
  const val IMPRESSION_METADATA_NAME_STRING = "xmm.edpa.impression_metadata.name"
  const val PIPELINE_PHASE_STRING = "xmm.edpa.pipeline.phase"
  const val GCS_OBJECT_GENERATION_STRING = "xmm.gcs.object.generation"

  val DATA_PROVIDER_NAME: AttributeKey<String> = AttributeKey.stringKey(DATA_PROVIDER_NAME_STRING)
  val MODEL_LINE_NAME: AttributeKey<String> = AttributeKey.stringKey(MODEL_LINE_NAME_STRING)
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
  val IMPRESSION_METADATA_NAME: AttributeKey<String> =
    AttributeKey.stringKey(IMPRESSION_METADATA_NAME_STRING)
  val PIPELINE_PHASE: AttributeKey<String> = AttributeKey.stringKey(PIPELINE_PHASE_STRING)
  val GCS_OBJECT_GENERATION: AttributeKey<Long> = AttributeKey.longKey(GCS_OBJECT_GENERATION_STRING)

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
      IMPRESSION_METADATA_NAME_STRING,
      PIPELINE_PHASE_STRING,
      GCS_OBJECT_GENERATION_STRING,
    )
}
