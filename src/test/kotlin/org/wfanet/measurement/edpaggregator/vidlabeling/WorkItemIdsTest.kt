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
import org.wfanet.measurement.common.api.ResourceIds

@RunWith(JUnit4::class)
class WorkItemIdsTest {

  @Test
  fun `forSubpoolAssigner is a stable RFC-1034 id with the sa- prefix`() {
    val id = WorkItemIds.forSubpoolAssigner(UPLOAD, MODEL_LINE, 3)
    assertThat(id).startsWith("sa-")
    assertThat(ResourceIds.RFC_1034_REGEX.matches(id)).isTrue()
    assertThat(id).isEqualTo(WorkItemIds.forSubpoolAssigner(UPLOAD, MODEL_LINE, 3))
  }

  @Test
  fun `forVidRankBuilder is a stable RFC-1034 id with the vrb- prefix`() {
    val id = WorkItemIds.forVidRankBuilder("$UPLOAD/rankerJobs/rj1")
    assertThat(id).startsWith("vrb-")
    assertThat(ResourceIds.RFC_1034_REGEX.matches(id)).isTrue()
    assertThat(id).isEqualTo(WorkItemIds.forVidRankBuilder("$UPLOAD/rankerJobs/rj1"))
  }

  @Test
  fun `forVidLabeler is a stable RFC-1034 id with the vl- prefix`() {
    val id = WorkItemIds.forVidLabeler("$UPLOAD/vidLabelingJobs/vj1")
    assertThat(id).startsWith("vl-")
    assertThat(ResourceIds.RFC_1034_REGEX.matches(id)).isTrue()
    assertThat(id).isEqualTo(WorkItemIds.forVidLabeler("$UPLOAD/vidLabelingJobs/vj1"))
  }

  @Test
  fun `ids stay RFC-1034 valid when the driving names contain '_' and are long`() {
    // Kingdom resource ids are base64url (may contain '_') and long; hashing keeps the id bounded
    // and free of invalid characters where a raw concatenation would not be.
    val id =
      WorkItemIds.forSubpoolAssigner(
        "$UPLOAD/rawImpressionUploads/Vq3nB_2aK9s_a_long_upload_id_0000000000",
        "modelProviders/mp/modelSuites/ms/modelLines/AbC_1dEf_gHi_long_id",
        7,
      )
    assertThat(ResourceIds.RFC_1034_REGEX.matches(id)).isTrue()
  }

  @Test
  fun `phase prefixes keep the three kinds distinct for the same trailing id`() {
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
