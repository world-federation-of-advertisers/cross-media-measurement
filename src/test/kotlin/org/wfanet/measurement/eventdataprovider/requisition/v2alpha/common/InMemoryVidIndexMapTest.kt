// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.eventdataprovider.shareshuffle.VidIndexMapEntryKt.value
import org.wfanet.measurement.eventdataprovider.shareshuffle.vidIndexMapEntry

@RunWith(JUnit4::class)
class InMemoryVidIndexMapTest : AbstractVidIndexMapTest() {

  override fun build(populationSpec: PopulationSpec): VidIndexMap =
    InMemoryVidIndexMap.build(populationSpec)

  override fun buildInternal(
    populationSpec: PopulationSpec,
    hashFunction: (Long, ByteString) -> Long,
  ): VidIndexMap = InMemoryVidIndexMap.buildInternal(populationSpec, hashFunction)

  @Test
  fun `build throws when PopulationSpec does not contain the indexMap`() =
    runBlocking<Unit> {
      val testPopulationSpec = populationSpec {
        subpopulations += subPopulation {
          vidRanges += vidRange {
            startVid = 1
            endVidInclusive = 1
          }
        }
      }
      assertFailsWith<InconsistentIndexMapAndPopulationSpecException>("Expected exception") {
        InMemoryVidIndexMap.build(
          testPopulationSpec,
          flowOf(
            vidIndexMapEntry {
              key = 1L
              value = value { index = 3 }
            },
            vidIndexMapEntry {
              key = 2L
              value = value { index = 6 }
            },
          ),
        )
      }
    }

  @Test
  fun `build throws when the indexMap does not contain the PopulationSpec`() =
    runBlocking<Unit> {
      val testPopulationSpec = populationSpec {
        subpopulations += subPopulation {
          vidRanges += vidRange {
            startVid = 1
            endVidInclusive = 1
          }
        }
      }
      assertFailsWith<InconsistentIndexMapAndPopulationSpecException>("Expected exception") {
        InMemoryVidIndexMap.build(testPopulationSpec, flowOf())
      }
    }

  @Test
  fun `build an InMemoryVidIndexMap with a consistent populationSpec and indexMap`() =
    runBlocking<Unit> {
      val testPopulationSpec = populationSpec {
        subpopulations += subPopulation {
          vidRanges += vidRange {
            startVid = 1
            endVidInclusive = 1
          }
        }
      }
      val vidIndexMap =
        InMemoryVidIndexMap.build(
          testPopulationSpec,
          flowOf(
            vidIndexMapEntry {
              key = 1L
              value = value { index = 2 }
            }
          ),
        )

      assertThat(vidIndexMap.size).isEqualTo(1)
      assertThat(vidIndexMap.get(1L)).isEqualTo(2)
      assertFailsWith<VidNotFoundException> { vidIndexMap.get(2L) }
    }
}
