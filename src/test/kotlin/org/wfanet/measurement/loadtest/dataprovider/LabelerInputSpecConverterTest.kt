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
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpecKt.DateSpecKt.dateRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpecKt.dateSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpecKt.demographicDistribution
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpecKt.idRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.fieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.labelerInputEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.market.v1.Common
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.demoBucket
import org.wfanet.virtualpeople.common.labelerOutput
import org.wfanet.virtualpeople.common.virtualPersonActivity
import org.wfanet.virtualpeople.core.labeler.Labeler

@RunWith(JUnit4::class)
class LabelerInputSpecConverterTest {

  @Test
  fun `convert produces expected SyntheticEventGroupSpec for known inputs`() {
    val converted =
      LabelerInputSpecConverter.convert(
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
      LabelerInputSpecConverter.convert(buildLabeler(), loadDataSpec(), loadPopulationSpec())

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
      LabelerInputSpecConverter.convert(
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
      LabelerInputSpecConverter.convert(
        buildLabeler(),
        buildGoldenInputSpec(),
        loadPopulationSpec(),
      )
    val actualCount: Int =
      converted.syntheticEventGroupSpec.dateSpecsList.sumOf { ds ->
        ds.frequencySpecsList.sumOf { it.vidRangeSpecsCount }
      }

    val result =
      LabelerInputSpecConverter.convert(
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
      LabelerInputSpecConverter.convert(
        buildLabeler(),
        buildGoldenInputSpec(),
        loadPopulationSpec(),
      )
    val actualCount: Int =
      converted.syntheticEventGroupSpec.dateSpecsList.sumOf { ds ->
        ds.frequencySpecsList.sumOf { it.vidRangeSpecsCount }
      }

    assertFailsWith<IllegalArgumentException> {
      LabelerInputSpecConverter.convert(
        buildLabeler(),
        buildGoldenInputSpec(),
        loadPopulationSpec(),
        maxVidRangeSpecs = actualCount - 1,
      )
    }
  }

  private fun buildGoldenInputSpec(): LabelerInputEventGroupSpec = labelerInputEventGroupSpec {
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
        demoBucket = demoBucket {
          gender = Gender.GENDER_MALE
          age = ageRange {
            minAge = 16
            maxAge = 34
          }
        }
        frequency = 2
      }
      demographicDistributions += demographicDistribution {
        idRange = idRange {
          start = 20
          endExclusive = 30
        }
        demoBucket = demoBucket {
          gender = Gender.GENDER_FEMALE
          age = ageRange {
            minAge = 55
            maxAge = 99
          }
        }
        frequency = 1
      }
    }
  }

  @Test
  fun `convert produces independent output per date spec`() {
    val spec = labelerInputEventGroupSpec {
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
            endExclusive = 5
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
        }
      }
      dateSpecs += dateSpec {
        this.dateRange = dateRange {
          start = date {
            year = 2024
            month = 2
            day = 1
          }
          endExclusive = date {
            year = 2024
            month = 2
            day = 2
          }
        }
        demographicDistributions += demographicDistribution {
          idRange = idRange {
            start = 100
            endExclusive = 105
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_FEMALE
            age = ageRange {
              minAge = 55
              maxAge = 99
            }
          }
          frequency = 2
        }
      }
    }

    val converted = LabelerInputSpecConverter.convert(buildLabeler(), spec, loadPopulationSpec())

    assertThat(converted.syntheticEventGroupSpec.dateSpecsCount).isEqualTo(2)

    val dateSpec1 = converted.syntheticEventGroupSpec.getDateSpecs(0)
    val dateSpec2 = converted.syntheticEventGroupSpec.getDateSpecs(1)

    val vids1: Set<Long> =
      dateSpec1.frequencySpecsList
        .flatMap { it.vidRangeSpecsList }
        .flatMap { (it.vidRange.start until it.vidRange.endExclusive).toList() }
        .toSet()
    val vids2: Set<Long> =
      dateSpec2.frequencySpecsList
        .flatMap { it.vidRangeSpecsList }
        .flatMap { (it.vidRange.start until it.vidRange.endExclusive).toList() }
        .toSet()

    assertThat(vids1).isNotEmpty()
    assertThat(vids2).isNotEmpty()
    assertThat(vids1.intersect(vids2)).isEmpty()

    val freq1: List<Long> = dateSpec1.frequencySpecsList.map { it.frequency }
    val freq2: List<Long> = dateSpec2.frequencySpecsList.map { it.frequency }
    assertThat(freq1).containsExactly(1L)
    assertThat(freq2).containsExactly(2L)
  }

  @Test
  fun `convert keeps adjacent VIDs with different fields in separate VidRangeSpecs`() {
    val spec = labelerInputEventGroupSpec {
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
            endExclusive = 1
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
          nonPopulationFieldValues.put(
            "video.completed_50_percent_plus",
            fieldValue { boolValue = true },
          )
        }
        demographicDistributions += demographicDistribution {
          idRange = idRange {
            start = 1
            endExclusive = 2
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
          nonPopulationFieldValues.put(
            "display.viewable_100_percent",
            fieldValue { boolValue = true },
          )
        }
      }
    }

    // Pin labeler input 0 -> VID 10005, input 1 -> VID 10006 — adjacent VIDs, same frequency,
    // different non_population_field_values. Without per-(frequency,fields) grouping the merge
    // step collapses both into one VidRangeSpec and the second VID silently loses its fields.
    val fakeLabel: (LabelerInput) -> LabelerOutput = { input ->
      val vid = if (input.profileInfo.proprietaryIdSpace1UserInfo.userId == "0") 10005L else 10006L
      labelerOutput { people += virtualPersonActivity { virtualPersonId = vid } }
    }

    val converted = LabelerInputSpecConverter.convert(fakeLabel, spec, loadPopulationSpec())

    val vidToFieldKeys: Map<Long, Set<String>> =
      converted.syntheticEventGroupSpec.dateSpecsList
        .flatMap { it.frequencySpecsList }
        .flatMap { it.vidRangeSpecsList }
        .flatMap { vrs ->
          (vrs.vidRange.start until vrs.vidRange.endExclusive).map {
            it to vrs.nonPopulationFieldValuesMap.keys
          }
        }
        .toMap()

    assertThat(vidToFieldKeys[10005L]).containsExactly("video.completed_50_percent_plus")
    assertThat(vidToFieldKeys[10006L]).containsExactly("display.viewable_100_percent")
  }

  @Test
  fun `convert preserves different non-population field values for same frequency`() {
    val spec = labelerInputEventGroupSpec {
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
            endExclusive = 5
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
          nonPopulationFieldValues.put(
            "video.completed_50_percent_plus",
            fieldValue { boolValue = true },
          )
        }
        demographicDistributions += demographicDistribution {
          idRange = idRange {
            start = 5
            endExclusive = 10
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
          nonPopulationFieldValues.put(
            "display.viewable_100_percent",
            fieldValue { boolValue = true },
          )
        }
      }
    }

    val converted = LabelerInputSpecConverter.convert(buildLabeler(), spec, loadPopulationSpec())
    val dateSpec = converted.syntheticEventGroupSpec.getDateSpecs(0)

    val allFieldKeys: Set<String> =
      dateSpec.frequencySpecsList
        .flatMap { it.vidRangeSpecsList }
        .flatMap { it.nonPopulationFieldValuesMap.keys }
        .toSet()

    assertThat(allFieldKeys)
      .containsAtLeast("video.completed_50_percent_plus", "display.viewable_100_percent")
  }

  @Test
  fun `convert throws when input labeler input ID count exceeds maximum`() {
    val hugeSpec = labelerInputEventGroupSpec {
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
            endExclusive = LabelerInputSpecConverter.MAX_INPUT_LABELER_INPUT_IDS + 1
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
        }
      }
    }

    val failure =
      assertFailsWith<IllegalArgumentException> {
        LabelerInputSpecConverter.convert(buildLabeler(), hugeSpec, loadPopulationSpec())
      }
    assertThat(failure).hasMessageThat().contains("labeler input IDs, exceeding maximum")
  }

  @Test
  fun `convert throws when demographic distribution frequency is non-positive`() {
    val spec = labelerInputEventGroupSpec {
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
            endExclusive = 5
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 0
        }
      }
    }

    val failure =
      assertFailsWith<IllegalArgumentException> {
        LabelerInputSpecConverter.convert(buildLabeler(), spec, loadPopulationSpec())
      }
    assertThat(failure).hasMessageThat().contains("frequency must be positive (got 0)")
  }

  @Test
  fun `convert throws when id range is empty or inverted`() {
    val spec = labelerInputEventGroupSpec {
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
            start = 10
            endExclusive = 5
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
        }
      }
    }

    val failure =
      assertFailsWith<IllegalArgumentException> {
        LabelerInputSpecConverter.convert(buildLabeler(), spec, loadPopulationSpec())
      }
    assertThat(failure)
      .hasMessageThat()
      .contains("id_range.end_exclusive (5) must be greater than start (10)")
  }

  @Test
  fun `convert throws when date range is empty or inverted`() {
    val spec = labelerInputEventGroupSpec {
      dateSpecs += dateSpec {
        this.dateRange = dateRange {
          start = date {
            year = 2024
            month = 1
            day = 2
          }
          endExclusive = date {
            year = 2024
            month = 1
            day = 1
          }
        }
        demographicDistributions += demographicDistribution {
          idRange = idRange {
            start = 0
            endExclusive = 5
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
        }
      }
    }

    assertFailsWith<IllegalArgumentException> {
      LabelerInputSpecConverter.convert(buildLabeler(), spec, loadPopulationSpec())
    }
  }

  @Test
  fun `convert throws when id ranges overlap within a date spec`() {
    val spec = labelerInputEventGroupSpec {
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
            endExclusive = 10
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
        }
        // Overlaps with the first distribution's range [0, 10).
        demographicDistributions += demographicDistribution {
          idRange = idRange {
            start = 5
            endExclusive = 15
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_FEMALE
            age = ageRange {
              minAge = 55
              maxAge = 99
            }
          }
          frequency = 2
        }
      }
    }

    val failure =
      assertFailsWith<IllegalArgumentException> {
        LabelerInputSpecConverter.convert(buildLabeler(), spec, loadPopulationSpec())
      }
    assertThat(failure).hasMessageThat().contains("(0 until 10) overlaps with [1] (5 until 15)")
  }

  @Test
  fun `convert throws when labeled VID is not in any PopulationSpec subpopulation`() {
    val spec = labelerInputEventGroupSpec {
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
            endExclusive = 1
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
        }
      }
    }

    // VID 99999 is outside every subpopulation range in labeler_input_population_spec.textproto.
    val fakeLabel: (LabelerInput) -> LabelerOutput = {
      labelerOutput { people += virtualPersonActivity { virtualPersonId = 99_999L } }
    }

    assertFailsWith<IllegalArgumentException> {
      LabelerInputSpecConverter.convert(fakeLabel, spec, loadPopulationSpec())
    }
  }

  @Test
  fun `convert emits one entry per labeler-returned person`() {
    val spec = labelerInputEventGroupSpec {
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
            endExclusive = 1
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 7
        }
      }
    }

    // Fake labeler emits two valid VIDs (both within the MALE 16-34 subpopulation) plus one
    // impression-counting-only entry (virtual_person_id = 0) that the converter should skip.
    val fakeLabel: (LabelerInput) -> LabelerOutput = {
      labelerOutput {
        people += virtualPersonActivity { virtualPersonId = 10_000L }
        people += virtualPersonActivity { virtualPersonId = 10_001L }
        people += virtualPersonActivity {}
      }
    }

    val converted = LabelerInputSpecConverter.convert(fakeLabel, spec, loadPopulationSpec())

    // Both VIDs appear, each with the input's full frequency (no split).
    val emittedVids: List<Pair<Long, Long>> =
      converted.syntheticEventGroupSpec.dateSpecsList
        .flatMap { it.frequencySpecsList }
        .flatMap { freqSpec ->
          freqSpec.vidRangeSpecsList.flatMap { vrs ->
            (vrs.vidRange.start until vrs.vidRange.endExclusive).map { it to freqSpec.frequency }
          }
        }
    assertThat(emittedVids).containsExactly(10_000L to 7L, 10_001L to 7L)
  }

  @Test
  fun `convert throws when labeler returns no people with virtual_person_id`() {
    val spec = labelerInputEventGroupSpec {
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
            endExclusive = 1
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 16
              maxAge = 34
            }
          }
          frequency = 1
        }
      }
    }

    // Labeler emits only impression-counting-only entries (virtual_person_id unset).
    val fakeLabel: (LabelerInput) -> LabelerOutput = {
      labelerOutput {
        people += virtualPersonActivity {}
        people += virtualPersonActivity {}
      }
    }

    assertFailsWith<IllegalStateException> {
      LabelerInputSpecConverter.convert(fakeLabel, spec, loadPopulationSpec())
    }
  }

  @Test
  fun `convert returns empty specs for empty input`() {
    val converted =
      LabelerInputSpecConverter.convert(
        buildLabeler(),
        labelerInputEventGroupSpec {},
        loadPopulationSpec(),
      )

    assertThat(converted.syntheticEventGroupSpec.dateSpecsList).isEmpty()
    assertThat(converted.populationSpec.subpopulationsList).isEmpty()
  }

  @Test
  fun `convert is deterministic for the same input`() {
    val spec = buildGoldenInputSpec()

    val first = LabelerInputSpecConverter.convert(buildLabeler(), spec, loadPopulationSpec())
    val second = LabelerInputSpecConverter.convert(buildLabeler(), spec, loadPopulationSpec())

    assertThat(second.syntheticEventGroupSpec).isEqualTo(first.syntheticEventGroupSpec)
    assertThat(second.populationSpec).isEqualTo(first.populationSpec)
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
          "labeler_input_population_spec.textproto",
        )
      )!!
    val typeRegistry = TypeRegistry.newBuilder().add(Common.getDescriptor()).build()
    return parseTextProto(path.toFile(), PopulationSpec.getDefaultInstance(), typeRegistry)
  }

  private fun loadDataSpec(): LabelerInputEventGroupSpec {
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
          "labeler_input_data_spec.textproto",
        )
      )!!
    return parseTextProto(path.toFile(), LabelerInputEventGroupSpec.getDefaultInstance())
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
