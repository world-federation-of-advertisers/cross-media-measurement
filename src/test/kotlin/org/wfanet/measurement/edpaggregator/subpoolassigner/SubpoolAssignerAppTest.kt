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

package org.wfanet.measurement.edpaggregator.subpoolassigner

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.subpoolAssignerParams

private const val DATA_PROVIDER = "dataProviders/dp"
private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/up1"
private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"

/**
 * Unit tests for [SubpoolAssignerApp.buildVidRankBuilderParamsTemplate] — the Phase-0 -> Phase-1
 * pass-through mapper. Verifies that the static `VidRankBuilderParams` fields (including the
 * Phase-2 `max_file_batch_size_bytes` bin-packing cap) are forwarded verbatim from the incoming
 * `SubpoolAssignerParams`.
 */
@RunWith(JUnit4::class)
class SubpoolAssignerAppTest {

  private fun storageParams(prefix: String) =
    SubpoolAssignerParamsKt.storageParams {
      gcsProjectId = "test-project"
      blobPrefix = prefix
    }

  private fun validParams(): SubpoolAssignerParams = subpoolAssignerParams {
    dataProvider = DATA_PROVIDER
    rawImpressionStorageParams = storageParams("raw-impressions")
    vidLabeledImpressionsStorageParams = storageParams("vid-labeled")
    subpoolMapStorageParams = storageParams("subpool-map")
    vidRankMapStorageParams = storageParams("vid-rank-map")
    rawImpressionUpload = UPLOAD
    modelLine = MODEL_LINE
    modelBlobPath = "model/blob"
    labelerInputFieldMapping.put("event_id.id", "raw_event_id")
    eventTemplateFieldMapping.put("banner_ad.viewable", "raw_viewable")
    eventTemplateDescriptorBlobUri = "descriptor/event-template.pb"
    eventTemplateType = "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
    requiredEntityKeyFieldMapping.put("household", "hh_col")
    optionalEntityKeyFieldMapping.put("creative", "cr_col")
    totalShards = 4
    maxFileBatchSizeBytes = 777
    activeStartTime = timestamp { seconds = 1000 }
    activeEndTime = timestamp { seconds = 2000 }
  }

  @Test
  fun `forwards max_file_batch_size_bytes verbatim into the template`() {
    val template = SubpoolAssignerApp.buildVidRankBuilderParamsTemplate(validParams())

    assertThat(template.maxFileBatchSizeBytes).isEqualTo(777)
  }

  @Test
  fun `requires max_file_batch_size_bytes to be set`() {
    // REQUIRED contract: the dispatcher must set it; an unset (0) value fails at this boundary
    // rather than being forwarded for Phase-1 to silently default.
    val params = validParams().toBuilder().clearMaxFileBatchSizeBytes().build()

    assertFailsWith<IllegalArgumentException> {
      SubpoolAssignerApp.buildVidRankBuilderParamsTemplate(params)
    }
  }

  @Test
  fun `maps the static pass-through fields`() {
    val template = SubpoolAssignerApp.buildVidRankBuilderParamsTemplate(validParams())

    assertThat(template.dataProvider).isEqualTo(DATA_PROVIDER)
    assertThat(template.rawImpressionUpload).isEqualTo(UPLOAD)
    assertThat(template.modelLine).isEqualTo(MODEL_LINE)
    assertThat(template.modelBlobPath).isEqualTo("model/blob")
    assertThat(template.totalShards).isEqualTo(4)
    assertThat(template.labelerInputFieldMappingMap).containsExactly("event_id.id", "raw_event_id")
    assertThat(template.eventTemplateFieldMappingMap)
      .containsExactly("banner_ad.viewable", "raw_viewable")
    // Event-template descriptor forwarded so the Phase-1 last-out can stamp it on Phase-2, which
    // requires it to build the labeled output.
    assertThat(template.eventTemplateDescriptorBlobUri).isEqualTo("descriptor/event-template.pb")
    assertThat(template.eventTemplateType)
      .isEqualTo("wfa.measurement.api.v2alpha.event_templates.testing.TestEvent")
    // Per-impression entity-key columns forwarded for the Phase-2 fan-out.
    assertThat(template.requiredEntityKeyFieldMappingMap).containsExactly("household", "hh_col")
    assertThat(template.optionalEntityKeyFieldMappingMap).containsExactly("creative", "cr_col")
    // Active-window timestamps forwarded verbatim from Phase-0 for the Phase-2 fan-out.
    assertThat(template.activeStartTime).isEqualTo(timestamp { seconds = 1000 })
    assertThat(template.activeEndTime).isEqualTo(timestamp { seconds = 2000 })
    // Storage params copy the {gcs_project_id, blob_prefix} shape across the two protos.
    assertThat(template.rawImpressionStorageParams.gcsProjectId).isEqualTo("test-project")
    assertThat(template.rawImpressionStorageParams.blobPrefix).isEqualTo("raw-impressions")
    assertThat(template.vidLabeledImpressionsStorageParams.blobPrefix).isEqualTo("vid-labeled")
    assertThat(template.subpoolMapStorageParams.blobPrefix).isEqualTo("subpool-map")
    assertThat(template.vidRankMapStorageParams.blobPrefix).isEqualTo("vid-rank-map")
  }

  @Test
  fun `requires vid_labeled_impressions_storage_params`() {
    val params = validParams().toBuilder().clearVidLabeledImpressionsStorageParams().build()

    assertFailsWith<IllegalArgumentException> {
      SubpoolAssignerApp.buildVidRankBuilderParamsTemplate(params)
    }
  }

  @Test
  fun `requires vid_rank_map_storage_params`() {
    val params = validParams().toBuilder().clearVidRankMapStorageParams().build()

    assertFailsWith<IllegalArgumentException> {
      SubpoolAssignerApp.buildVidRankBuilderParamsTemplate(params)
    }
  }
}
