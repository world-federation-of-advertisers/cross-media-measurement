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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.file.Paths
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.virtualpeople.common.eventId
import org.wfanet.virtualpeople.common.labelerInput

@RunWith(JUnit4::class)
class VirtualPeopleVidAssignerTest {
  @Test
  fun `fromCompiledNodeBlob reads a Riegeli CompiledNode list and assigns the expected VID`() {
    val modelBlob: ByteString =
      MODEL_BLOB_PATH.toFile().inputStream().use { ByteString.readFrom(it) }

    val assigner = VirtualPeopleVidAssigner.fromCompiledNodeBlob(modelBlob)
    val output = assigner.assign(labelerInput { eventId = eventId { id = "any-event" } })

    // single_id_model assigns virtual person id 10000 to every input (see fixture provenance
    // below).
    assertThat(output.peopleList).hasSize(1)
    assertThat(output.getPeople(0).virtualPersonId).isEqualTo(10000L)
  }

  companion object {
    /**
     * Riegeli-encoded `CompiledNode` list, vendored from `virtual-people-core-serving` v0.3.1
     * (`src/main/resources/testing/labeler/single_id_model_riegeli_list`). The model assigns
     * virtual person id 10000 to every input, so it exercises the Riegeli-list read path in
     * [VirtualPeopleVidAssigner.fromCompiledNodeBlob] with a deterministic golden VID.
     */
    private val MODEL_BLOB_PATH =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "test",
          "kotlin",
          "org",
          "wfanet",
          "measurement",
          "edpaggregator",
          "vidlabeler",
          "testdata",
          "single_id_model_riegeli_list",
        )
      )!!
  }
}
