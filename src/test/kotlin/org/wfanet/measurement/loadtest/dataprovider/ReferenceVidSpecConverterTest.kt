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
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.TypeRegistry
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.market.v1.Common
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.core.labeler.Labeler

@RunWith(JUnit4::class)
class ReferenceVidSpecConverterTest {

  @Test
  fun `convert produces SyntheticEventGroupSpec with correct date range`() {
    val converted = convertDefault()

    assertThat(converted.syntheticEventGroupSpec.dateSpecsCount).isEqualTo(1)
    val dateSpec = converted.syntheticEventGroupSpec.getDateSpecs(0)
    assertThat(dateSpec.dateRange.start.year).isEqualTo(2024)
    assertThat(dateSpec.dateRange.start.month).isEqualTo(1)
    assertThat(dateSpec.dateRange.start.day).isEqualTo(1)
    assertThat(dateSpec.dateRange.endExclusive.year).isEqualTo(2024)
    assertThat(dateSpec.dateRange.endExclusive.month).isEqualTo(1)
    assertThat(dateSpec.dateRange.endExclusive.day).isEqualTo(4)
  }

  @Test
  fun `convert preserves non-population field values`() {
    val converted = convertDefault()

    val dateSpec = converted.syntheticEventGroupSpec.getDateSpecs(0)
    for (freqSpec in dateSpec.frequencySpecsList) {
      for (vidRangeSpec in freqSpec.vidRangeSpecsList) {
        assertThat(vidRangeSpec.nonPopulationFieldValuesMap)
          .containsKey("video.completed_50_percent_plus")
      }
    }
  }

  @Test
  fun `convert produces VIDs within PopulationSpec ranges`() {
    val converted = convertDefault()
    val populationSpec = converted.populationSpec

    val allVidRanges: List<LongRange> =
      populationSpec.subpopulationsList.flatMap { sub ->
        sub.vidRangesList.map { it.startVid..it.endVidInclusive }
      }

    val dateSpec = converted.syntheticEventGroupSpec.getDateSpecs(0)
    for (freqSpec in dateSpec.frequencySpecsList) {
      for (vidRangeSpec in freqSpec.vidRangeSpecsList) {
        for (vid in vidRangeSpec.vidRange.start until vidRangeSpec.vidRange.endExclusive) {
          val inRange = allVidRanges.any { vid in it }
          assertThat(inRange).isTrue()
        }
      }
    }
  }

  @Test
  fun `convert produces multiple frequency groups when collisions exist`() {
    val converted = convertDefault()

    val dateSpec = converted.syntheticEventGroupSpec.getDateSpecs(0)
    val frequencies: List<Long> = dateSpec.frequencySpecsList.map { it.frequency }

    assertThat(frequencies.size).isGreaterThan(1)
    assertThat(frequencies).isInStrictOrder()
  }

  @Test
  fun `convert preserves PopulationSpec attributes`() {
    val converted = convertDefault()
    val sourcePopSpec = loadPopulationSpec()

    for (subPop in converted.populationSpec.subpopulationsList) {
      assertThat(subPop.attributesList).isNotEmpty()
      val matchingSource =
        sourcePopSpec.subpopulationsList.firstOrNull { source ->
          source.attributesList == subPop.attributesList
        }
      assertThat(matchingSource).isNotNull()
    }
  }

  @Test
  fun `convert throws when VidRangeSpec count exceeds threshold`() {
    assertFailsWith<IllegalArgumentException> {
      ReferenceVidSpecConverter.convert(
        buildLabeler(),
        loadDataSpec(),
        loadPopulationSpec(),
        maxVidRangeSpecs = 1,
      )
    }
  }

  @Test
  fun `convert produces PopulationSpec with merged adjacent VID ranges`() {
    val converted = convertDefault()

    for (subPop in converted.populationSpec.subpopulationsList) {
      for (i in 0 until subPop.vidRangesCount - 1) {
        val current = subPop.getVidRanges(i)
        val next = subPop.getVidRanges(i + 1)
        assertThat(current.endVidInclusive).isLessThan(next.startVid - 1)
      }
    }
  }

  private fun convertDefault(): ReferenceVidSpecConverter.ConvertedSpecs =
    ReferenceVidSpecConverter.convert(buildLabeler(), loadDataSpec(), loadPopulationSpec())

  private fun buildLabeler(): Labeler {
    val modelPath =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "main",
          "resources",
          "testing",
          "labeler",
          "reference_test_model.textproto",
        )
      )!!
    return Labeler.build(parseTextProto(modelPath.toFile(), CompiledNode.getDefaultInstance()))
  }

  private fun loadPopulationSpec(): PopulationSpec {
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
          "reference_vid_population_spec.textproto",
        )
      )!!
    val typeRegistry = TypeRegistry.newBuilder().add(Common.getDescriptor()).build()
    return parseTextProto(path.toFile(), PopulationSpec.getDefaultInstance(), typeRegistry)
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

  companion object {
    @BeforeClass
    @JvmStatic
    fun registerTink() {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }
  }
}
