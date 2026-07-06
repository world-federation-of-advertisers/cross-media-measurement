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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class WorkItemIdsTest {

  @Test
  fun `forSubpoolAssigner builds the per-shard id from the upload, model line, and shard`() {
    assertThat(WorkItemIds.forSubpoolAssigner("upload-1", MODEL_LINE, 3))
      .isEqualTo("subpool-assigner-upload-1-ml1-shard-3")
  }

  @Test
  fun `forVidRankBuilder derives the id from the ranker job resource id`() {
    assertThat(WorkItemIds.forVidRankBuilder("$UPLOAD/rankerJobs/rj1"))
      .isEqualTo("vid-rank-builder-rj1")
  }

  @Test
  fun `forVidLabeler derives the id from the vid labeling job resource id`() {
    assertThat(WorkItemIds.forVidLabeler("$UPLOAD/vidLabelingJobs/vj1"))
      .isEqualTo("vid-labeler-vj1")
  }

  @Test
  fun `ids are stable across calls`() {
    assertThat(WorkItemIds.forSubpoolAssigner("upload-1", MODEL_LINE, 3))
      .isEqualTo(WorkItemIds.forSubpoolAssigner("upload-1", MODEL_LINE, 3))
    assertThat(WorkItemIds.forVidRankBuilder("$UPLOAD/rankerJobs/rj1"))
      .isEqualTo(WorkItemIds.forVidRankBuilder("$UPLOAD/rankerJobs/rj1"))
    assertThat(WorkItemIds.forVidLabeler("$UPLOAD/vidLabelingJobs/vj1"))
      .isEqualTo(WorkItemIds.forVidLabeler("$UPLOAD/vidLabelingJobs/vj1"))
  }

  @Test
  fun `phase prefixes keep the three WorkItem kinds distinct for the same job id`() {
    // A ranker job and a vid-labeling job could share a trailing resource id; the per-phase prefix
    // must keep their WorkItem ids distinct so re-publishing never targets the wrong queue.
    assertThat(WorkItemIds.forVidRankBuilder("$UPLOAD/rankerJobs/x"))
      .isNotEqualTo(WorkItemIds.forVidLabeler("$UPLOAD/vidLabelingJobs/x"))
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp123"
    private const val UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/upload-1"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
  }
}
