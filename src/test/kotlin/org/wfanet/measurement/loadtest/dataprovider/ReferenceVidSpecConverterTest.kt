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
import com.google.protobuf.TypeRegistry
import com.google.type.date
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpecKt.DateSpecKt.dateRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpecKt.dateSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpecKt.demographicDistribution
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpecKt.idRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.referenceVidEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.market.v1.Common
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.core.labeler.Labeler

@RunWith(JUnit4::class)
class ReferenceVidSpecConverterTest {

  @Test
  fun `convert produces expected SyntheticEventGroupSpec for known inputs`() {
    val converted =
      ReferenceVidSpecConverter.convert(
        buildLabeler(),
        buildGoldenInputSpec(),
        loadPopulationSpec(),
      )

    val expectedSyntheticSpec: SyntheticEventGroupSpec =
      parseTextProto(
        loadTestDataFile("expected_converted_synthetic_spec.textproto"),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )
    val expectedPopulationSpec: PopulationSpec =
      parseTextProto(
        loadTestDataFile("expected_converted_population_spec.textproto"),
        PopulationSpec.getDefaultInstance(),
        TypeRegistry.newBuilder().add(Common.getDescriptor()).build(),
      )

    assertThat(converted.syntheticEventGroupSpec).isEqualTo(expectedSyntheticSpec)
    assertThat(converted.populationSpec).isEqualTo(expectedPopulationSpec)
  }

  @Test
  fun `convert preserves non-population field values`() {
    val converted =
      ReferenceVidSpecConverter.convert(buildLabeler(), loadDataSpec(), loadPopulationSpec())

    val dateSpec = converted.syntheticEventGroupSpec.getDateSpecs(0)
    for (freqSpec in dateSpec.frequencySpecsList) {
      for (vidRangeSpec in freqSpec.vidRangeSpecsList) {
        assertThat(vidRangeSpec.nonPopulationFieldValuesMap)
          .containsKey("video.completed_50_percent_plus")
      }
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
  fun `convert passes when VidRangeSpec count equals threshold`() {
    val converted =
      ReferenceVidSpecConverter.convert(
        buildLabeler(),
        buildGoldenInputSpec(),
        loadPopulationSpec(),
      )
    val actualCount: Int =
      converted.syntheticEventGroupSpec.dateSpecsList.sumOf { ds ->
        ds.frequencySpecsList.sumOf { it.vidRangeSpecsCount }
      }

    val result =
      ReferenceVidSpecConverter.convert(
        buildLabeler(),
        buildGoldenInputSpec(),
        loadPopulationSpec(),
        maxVidRangeSpecs = actualCount,
      )
    assertThat(result.syntheticEventGroupSpec).isEqualTo(converted.syntheticEventGroupSpec)
  }

  @Test
  fun `convert throws when VidRangeSpec count is one over threshold`() {
    val converted =
      ReferenceVidSpecConverter.convert(
        buildLabeler(),
        buildGoldenInputSpec(),
        loadPopulationSpec(),
      )
    val actualCount: Int =
      converted.syntheticEventGroupSpec.dateSpecsList.sumOf { ds ->
        ds.frequencySpecsList.sumOf { it.vidRangeSpecsCount }
      }

    assertFailsWith<IllegalArgumentException> {
      ReferenceVidSpecConverter.convert(
        buildLabeler(),
        buildGoldenInputSpec(),
        loadPopulationSpec(),
        maxVidRangeSpecs = actualCount - 1,
      )
    }
  }

  private fun buildGoldenInputSpec(): ReferenceVidEventGroupSpec = referenceVidEventGroupSpec {
    dateSpecs += dateSpec {
      this.dateRange = dateRange {
        start = date {
          year = 2024
          month = 1
          day = 1
        }
        endExclusive = date {
          year = 2024
          month = 1
          day = 2
        }
      }
      demographicDistributions += demographicDistribution {
        idRange = idRange {
          start = 0
          endExclusive = 20
        }
        gender = 1
        minAge = 16
        maxAge = 34
        frequency = 2
      }
      demographicDistributions += demographicDistribution {
        idRange = idRange {
          start = 20
          endExclusive = 30
        }
        gender = 2
        minAge = 55
        maxAge = 99
        frequency = 1
      }
    }
  }

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

  private fun loadTestDataFile(name: String): java.io.File {
    val path =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "test",
          "kotlin",
          "org",
          "wfanet",
          "measurement",
          "loadtest",
          "dataprovider",
          name,
        )
      )!!
    return path.toFile()
  }
}
