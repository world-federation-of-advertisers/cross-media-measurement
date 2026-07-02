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
import com.google.type.date
import java.nio.file.Paths
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpecKt.DateSpecKt.dateRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpecKt.dateSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpecKt.demographicDistribution
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpecKt.idRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.fieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.labelerInputEventGroupSpec
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.demoBucket

@RunWith(JUnit4::class)
class LabelerInputDataGenerationTest {

  @Test
  fun `generate produces one record per labeler input ID`() {
    val records: List<LabelerInputRecord> =
      LabelerInputDataGeneration.generate(loadDataSpec()).toList()

    assertThat(records).hasSize(300)
  }

  @Test
  fun `generate assigns correct demographics from spec`() {
    val records: List<LabelerInputRecord> =
      LabelerInputDataGeneration.generate(loadDataSpec()).toList()

    val m1634: List<LabelerInputRecord> = records.filter { it.labelerInputId in 0L until 100L }
    assertThat(m1634).hasSize(100)
    m1634.forEach {
      assertThat(it.demoBucket.gender).isEqualTo(Gender.GENDER_MALE)
      assertThat(it.demoBucket.age.minAge).isEqualTo(16)
      assertThat(it.demoBucket.age.maxAge).isEqualTo(34)
    }

    val f3554: List<LabelerInputRecord> = records.filter { it.labelerInputId in 100L until 200L }
    assertThat(f3554).hasSize(100)
    f3554.forEach {
      assertThat(it.demoBucket.gender).isEqualTo(Gender.GENDER_FEMALE)
      assertThat(it.demoBucket.age.minAge).isEqualTo(35)
      assertThat(it.demoBucket.age.maxAge).isEqualTo(54)
    }

    val m55plus: List<LabelerInputRecord> = records.filter { it.labelerInputId in 200L until 300L }
    assertThat(m55plus).hasSize(100)
    m55plus.forEach {
      assertThat(it.demoBucket.gender).isEqualTo(Gender.GENDER_MALE)
      assertThat(it.demoBucket.age.minAge).isEqualTo(55)
      assertThat(it.demoBucket.age.maxAge).isEqualTo(99)
    }
  }

  @Test
  fun `generate assigns per-distribution frequency`() {
    val records: List<LabelerInputRecord> =
      LabelerInputDataGeneration.generate(loadDataSpec()).toList()

    val m1634Freq: List<Long> =
      records.filter { it.labelerInputId in 0L until 100L }.map { it.frequency }.distinct()
    assertThat(m1634Freq).containsExactly(2L)

    val f3554Freq: List<Long> =
      records.filter { it.labelerInputId in 100L until 200L }.map { it.frequency }.distinct()
    assertThat(f3554Freq).containsExactly(1L)

    val m55Freq: List<Long> =
      records.filter { it.labelerInputId in 200L until 300L }.map { it.frequency }.distinct()
    assertThat(m55Freq).containsExactly(3L)
  }

  @Test
  fun `generate preserves non-population field values per distribution`() {
    val records: List<LabelerInputRecord> =
      LabelerInputDataGeneration.generate(loadDataSpec()).toList()

    records.forEach {
      assertThat(it.nonPopulationFieldValues).containsKey("video.completed_50_percent_plus")
    }
  }

  @Test
  fun `generate produces sequential labeler input IDs`() {
    val records: List<LabelerInputRecord> =
      LabelerInputDataGeneration.generate(loadDataSpec()).toList()

    val labelerInputIds: List<Long> = records.map { it.labelerInputId }
    assertThat(labelerInputIds).isEqualTo((0L until 300L).toList())
  }

  @Test
  fun `generate returns empty sequence for empty spec`() {
    val emptySpec: LabelerInputEventGroupSpec = labelerInputEventGroupSpec {}

    val records: List<LabelerInputRecord> = LabelerInputDataGeneration.generate(emptySpec).toList()

    assertThat(records).isEmpty()
  }

  @Test
  fun `generate emits records for multiple date specs`() {
    val spec: LabelerInputEventGroupSpec = labelerInputEventGroupSpec {
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
            start = 10
            endExclusive = 13
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_FEMALE
            age = ageRange {
              minAge = 55
              maxAge = 99
            }
          }
          frequency = 3
        }
      }
    }

    val records: List<LabelerInputRecord> = LabelerInputDataGeneration.generate(spec).toList()

    assertThat(records).hasSize(8)
    assertThat(records.filter { it.labelerInputId in 0L until 5L }).hasSize(5)
    assertThat(records.filter { it.labelerInputId in 10L until 13L }).hasSize(3)
    assertThat(records.filter { it.frequency == 3L }).hasSize(3)
  }

  @Test
  fun `generate assigns different non-population field values per distribution`() {
    val spec: LabelerInputEventGroupSpec = labelerInputEventGroupSpec {
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
            endExclusive = 3
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
            start = 3
            endExclusive = 6
          }
          demoBucket = demoBucket {
            gender = Gender.GENDER_FEMALE
            age = ageRange {
              minAge = 55
              maxAge = 99
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

    val records: List<LabelerInputRecord> = LabelerInputDataGeneration.generate(spec).toList()

    val videoRecords: List<LabelerInputRecord> = records.filter { it.labelerInputId in 0L until 3L }
    videoRecords.forEach {
      assertThat(it.nonPopulationFieldValues).containsKey("video.completed_50_percent_plus")
      assertThat(it.nonPopulationFieldValues).doesNotContainKey("display.viewable_100_percent")
    }

    val displayRecords: List<LabelerInputRecord> =
      records.filter { it.labelerInputId in 3L until 6L }
    displayRecords.forEach {
      assertThat(it.nonPopulationFieldValues).containsKey("display.viewable_100_percent")
      assertThat(it.nonPopulationFieldValues).doesNotContainKey("video.completed_50_percent_plus")
    }
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
}
