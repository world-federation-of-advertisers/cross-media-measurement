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

package org.wfanet.measurement.duchy.mill.trustee.v2

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any as ProtoAny
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.internal.duchy.PopulationSpec as InternalPopulationSpec
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2Kt

@RunWith(JUnit4::class)
class HashRankSubpopulationIndexTest {
  @Test
  fun `subpopulationOf matches DataProvider frequency vectors over the whole population`() {
    assertMatchesDataProviderVectors(samplingStart = 0.0f, samplingWidth = 1.0f)
  }

  @Test
  fun `subpopulationOf matches DataProvider frequency vectors over a sampling interval`() {
    assertMatchesDataProviderVectors(samplingStart = 0.25f, samplingWidth = 0.5f)
  }

  @Test
  fun `subpopulationOf matches DataProvider frequency vectors over a wrapping interval`() {
    assertMatchesDataProviderVectors(samplingStart = 0.8f, samplingWidth = 0.5f)
  }

  @Test
  fun `subpopulationOf matches DataProvider frequency vectors over an inexact interval`() {
    assertMatchesDataProviderVectors(samplingStart = 0.1f, samplingWidth = 0.33f)
  }

  @Test
  fun `attributeValue returns the attributes of the subpopulation`() {
    val index = buildIndex(samplingStart = 0.0f, samplingWidth = 1.0f)

    assertThat(index.subpopulationCount).isEqualTo(2)
    assertThat(index.attributeValue(0, "person.age_group")).isEqualTo("YEARS_18_TO_34")
    assertThat(index.attributeValue(1, "person.age_group")).isEqualTo("YEARS_35_TO_54")
  }

  @Test
  fun `subpopulationOf throws for a position outside the vector`() {
    val index = buildIndex(samplingStart = 0.0f, samplingWidth = 0.5f)

    assertFailsWith<IllegalArgumentException> { index.subpopulationOf(index.vectorSize) }
  }

  @Test
  fun `build throws when VID ranges overlap`() {
    val overlapping = populationSpec {
      subpopulations += subPopulation(Person.AgeGroup.YEARS_18_TO_34, 1L..10L)
      subpopulations += subPopulation(Person.AgeGroup.YEARS_35_TO_54, 10L..20L)
    }

    assertFailsWith<IllegalArgumentException> {
      HashRankSubpopulationIndex.build(
        overlapping.toInternal(),
        attributesOf(overlapping),
        samplingStart = 0.0f,
        samplingWidth = 1.0f,
      )
    }
  }

  @Test
  fun `build throws when attributes do not match the PopulationSpec`() {
    val oneSubpopulation = populationSpec {
      subpopulations += subPopulation(Person.AgeGroup.YEARS_18_TO_34, 1L..10L)
    }

    assertFailsWith<IllegalArgumentException> {
      HashRankSubpopulationIndex.build(
        POPULATION_SPEC.toInternal(),
        attributesOf(oneSubpopulation),
        samplingStart = 0.0f,
        samplingWidth = 1.0f,
      )
    }
  }

  @Test
  fun `build throws for a zero sampling width`() {
    assertFailsWith<IllegalArgumentException> {
      buildIndex(samplingStart = 0.0f, samplingWidth = 0.0f)
    }
  }

  companion object {
    /**
     * Two subpopulations, the first split across two VID ranges around the second, listed out of
     * VID order.
     */
    private val POPULATION_SPEC: PopulationSpec = populationSpec {
      subpopulations += subPopulation(Person.AgeGroup.YEARS_18_TO_34, 81L..100L, 1L..40L)
      subpopulations += subPopulation(Person.AgeGroup.YEARS_35_TO_54, 41L..80L)
    }

    private fun subPopulation(
      ageGroup: Person.AgeGroup,
      vararg vidRanges: LongRange,
    ): PopulationSpec.SubPopulation =
      PopulationSpecKt.subPopulation {
        attributes += ProtoAny.pack(person { this.ageGroup = ageGroup }).toByteString()
        for (range in vidRanges) {
          this.vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = range.first
              endVidInclusive = range.last
            }
        }
      }

    private fun PopulationSpec.toInternal(): InternalPopulationSpec =
      InternalPopulationSpec.parseFrom(toByteString())

    private fun attributesOf(populationSpec: PopulationSpec): SubpopulationAttributes =
      SubpopulationAttributes.build(
        populationSpec.toInternal(),
        TrusTeeV2Kt.eventDescriptor {
          descriptorSet = ProtoReflection.buildFileDescriptorSet(TestEvent.getDescriptor())
          eventMessageType = TestEvent.getDescriptor().fullName
        },
      )

    private fun buildIndex(samplingStart: Float, samplingWidth: Float): HashRankSubpopulationIndex =
      HashRankSubpopulationIndex.build(
        POPULATION_SPEC.toInternal(),
        attributesOf(POPULATION_SPEC),
        samplingStart,
        samplingWidth,
      )

    /**
     * Asserts that, for each subpopulation, a DataProvider's frequency vector over the interval
     * reaching every VID of that subpopulation is nonzero exactly at the positions the index
     * assigns to it.
     */
    private fun assertMatchesDataProviderVectors(samplingStart: Float, samplingWidth: Float) {
      val index = buildIndex(samplingStart, samplingWidth)
      val vidIndexMap = InMemoryVidIndexMap.build(POPULATION_SPEC)
      val measurementSpec = measurementSpec {
        reach = MeasurementSpecKt.reach {}
        vidSamplingInterval =
          MeasurementSpecKt.vidSamplingInterval {
            start = samplingStart
            width = samplingWidth
          }
      }

      for ((subpopulation, spec) in POPULATION_SPEC.subpopulationsList.withIndex()) {
        val builder =
          FrequencyVectorBuilder(
            POPULATION_SPEC,
            measurementSpec,
            strict = false,
            overrideImpressionMaxFrequencyPerUser = null,
          )
        for (range in spec.vidRangesList) {
          for (vid in range.startVid..range.endVidInclusive) {
            builder.increment(vidIndexMap[vid])
          }
        }
        val vector: IntArray = builder.frequencyDataArray

        assertThat(vector.size).isEqualTo(index.vectorSize)
        val expected = vector.indices.filter { vector[it] > 0 }
        val actual = vector.indices.filter { index.subpopulationOf(it) == subpopulation }
        assertThat(actual).isEqualTo(expected)
      }
    }
  }
}
