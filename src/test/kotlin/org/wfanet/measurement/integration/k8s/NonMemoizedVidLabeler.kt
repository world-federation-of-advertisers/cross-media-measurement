/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.protobuf.ByteString
import java.time.Instant
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.vidlabeler.VirtualPeopleVidAssigner
import org.wfanet.measurement.loadtest.edpaggregator.testing.RawImpressionColumns
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.parquetValue

/**
 * Runs the deployed **non-memoized (hash-only)** VID labeler in-process to reproduce the VID the
 * pipeline assigns to a synthetic impression.
 *
 * The cloud test pins its measurements to a non-memoized model line. That line's 03-21 impressions
 * are labeled by the pipeline, while the out-of-band days (edp7 03-15..20 and all of edpa_meta) are
 * pre-staged by the test. To make the same person carry the SAME VID across every day and both
 * publishers, the pre-staged impressions and the expected-result computation must use the same VID
 * the pipeline assigns on 03-21 — so both call this labeler instead of writing the raw synthetic
 * VID.
 *
 * This is only sound for the **non-memoized** path: that VID is a pure function of the person
 * identity (`user_id`) + demographics through the static model, so it is deterministically
 * reproducible offline (unlike the memoized rank-index path, whose rank depends on pipeline-runtime
 * ordering). The columns built here MUST match those `RawImpressionsWriter` writes for the pipeline
 * (see [RawImpressionColumns]); `event_id`/`event_time_usec` are supplied because the mapping
 * requires them, but they do not affect the non-memoized VID.
 */
class NonMemoizedVidLabeler(
  private val assigner: VirtualPeopleVidAssigner,
  labelerInputFieldMapping: List<LabelerInputFieldMapping>,
) {
  private val mapper = LabelerInputMapper(labelerInputFieldMapping)

  /**
   * Assigns the model VID for one impression, projecting it onto the same raw-impression columns
   * the pipeline reads and running the same [LabelerInputMapper] + [VirtualPeopleVidAssigner].
   *
   * @param vid the synthetic VID; used only to derive the `person_id` (`person-<vid>`) the model
   *   routes on — the returned VID is the model's assignment, not this value.
   */
  fun assignVid(vid: Long, personGender: String, personAgeGroup: String, timestamp: Instant): Long {
    val row: Map<String, ParquetValue> =
      mapOf(
        RawImpressionColumns.EVENT_ID to parquetValue { stringValue = "offline-$vid" },
        RawImpressionColumns.EVENT_TIME_USEC to
          parquetValue {
            int64Value = timestamp.epochSecond * 1_000_000L + timestamp.nano / 1_000L
          },
        RawImpressionColumns.PERSON_ID to parquetValue { stringValue = "person-$vid" },
        RawImpressionColumns.PERSON_GENDER to parquetValue { stringValue = personGender },
        RawImpressionColumns.PERSON_AGE_GROUP to parquetValue { stringValue = personAgeGroup },
      )
    return assigner.assign(mapper.project(row)).getPeople(0).virtualPersonId
  }

  companion object {
    /**
     * Loads the compiled non-memoized model blob from [modelBlobUri] (a `gs://` URI read with the
     * runner's ADC) and builds a labeler using [labelerInputFieldMapping] (the same mapping the
     * deployed pipeline uses, from `VidLabelerModelResourcesRule`).
     */
    suspend fun create(
      modelBlobUri: String,
      gcsProjectId: String,
      labelerInputFieldMapping: List<LabelerInputFieldMapping>,
    ): NonMemoizedVidLabeler {
      val blobUri = SelectedStorageClient.parseBlobUri(modelBlobUri)
      val blob =
        SelectedStorageClient(blobUri, rootDirectory = null, projectId = gcsProjectId)
          .getBlob(blobUri.key) ?: error("Non-memoized model blob not found: $modelBlobUri")
      val bytes: ByteString = blob.read().flatten()
      return NonMemoizedVidLabeler(
        VirtualPeopleVidAssigner.fromCompiledNodeBlob(bytes),
        labelerInputFieldMapping,
      )
    }
  }
}
