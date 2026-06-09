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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import java.nio.file.Paths
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpec
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto

@RunWith(JUnit4::class)
class ReferenceVidDataGenerationTest {

  @Test
  fun `generate produces one record per reference VID`() {
    val records: List<ReferenceVidRecord> =
      ReferenceVidDataGeneration.generate(loadDataSpec()).toList()

    assertThat(records).hasSize(300)
  }

  @Test
  fun `generate assigns correct demographics from spec`() {
    val records: List<ReferenceVidRecord> =
      ReferenceVidDataGeneration.generate(loadDataSpec()).toList()

    val m1634: List<ReferenceVidRecord> = records.filter { it.referenceVid in 0L until 100L }
    assertThat(m1634).hasSize(100)
    m1634.forEach {
      assertThat(it.gender).isEqualTo(1)
      assertThat(it.minAge).isEqualTo(16)
      assertThat(it.maxAge).isEqualTo(34)
    }

    val f3554: List<ReferenceVidRecord> = records.filter { it.referenceVid in 100L until 200L }
    assertThat(f3554).hasSize(100)
    f3554.forEach {
      assertThat(it.gender).isEqualTo(2)
      assertThat(it.minAge).isEqualTo(35)
      assertThat(it.maxAge).isEqualTo(54)
    }

    val m55plus: List<ReferenceVidRecord> = records.filter { it.referenceVid in 200L until 300L }
    assertThat(m55plus).hasSize(100)
    m55plus.forEach {
      assertThat(it.gender).isEqualTo(1)
      assertThat(it.minAge).isEqualTo(55)
      assertThat(it.maxAge).isEqualTo(99)
    }
  }

  @Test
  fun `generate assigns per-distribution frequency`() {
    val records: List<ReferenceVidRecord> =
      ReferenceVidDataGeneration.generate(loadDataSpec()).toList()

    val m1634Freq: List<Long> =
      records.filter { it.referenceVid in 0L until 100L }.map { it.frequency }.distinct()
    assertThat(m1634Freq).containsExactly(2L)

    val f3554Freq: List<Long> =
      records.filter { it.referenceVid in 100L until 200L }.map { it.frequency }.distinct()
    assertThat(f3554Freq).containsExactly(1L)

    val m55Freq: List<Long> =
      records.filter { it.referenceVid in 200L until 300L }.map { it.frequency }.distinct()
    assertThat(m55Freq).containsExactly(3L)
  }

  @Test
  fun `generate preserves non-population field values`() {
    val records: List<ReferenceVidRecord> =
      ReferenceVidDataGeneration.generate(loadDataSpec()).toList()

    records.forEach {
      assertThat(it.nonPopulationFieldValues).containsKey("video.completed_50_percent_plus")
    }
  }

  @Test
  fun `generate produces sequential reference VIDs`() {
    val records: List<ReferenceVidRecord> =
      ReferenceVidDataGeneration.generate(loadDataSpec()).toList()

    val referenceVids: List<Long> = records.map { it.referenceVid }
    assertThat(referenceVids).isEqualTo((0L until 300L).toList())
  }

  private fun loadDataSpec(): ReferenceVidEventGroupSpec {
    val path =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "main",
          "proto",
          "wfa",
          "measurement",
          "loadtest",
          "dataprovider",
          "reference_vid_data_spec.textproto",
        )
      )!!
    return parseTextProto(path.toFile(), ReferenceVidEventGroupSpec.getDefaultInstance())
  }
}
