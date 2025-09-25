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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.populationSpec

@RunWith(JUnit4::class)
class ParallelInMemoryVidIndexMapTest : AbstractVidIndexMapTest() {

  override fun build(populationSpec: PopulationSpec): VidIndexMap =
    ParallelInMemoryVidIndexMap.build(populationSpec)

  override fun buildInternal(
    populationSpec: PopulationSpec,
    hashFunction: (Long, ByteString) -> Long,
  ): VidIndexMap = ParallelInMemoryVidIndexMap.buildInternal(populationSpec, hashFunction)

  @Test
  fun `parallel map build fails when VID missing`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 2
        }
      }
    }

    assertFailsWith<VidNotFoundException> {
      val map = ParallelInMemoryVidIndexMap.build(populationSpec)
      map.get(3L)
    }
  }

  @Test
  fun `parallel map matches sequential for single contiguous population`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 32
        }
      }
    }

    assertParallelMatchesSequential(
      populationSpec = populationSpec,
      hashFunction = { vid: Long, _: ByteString -> vid * 5 },
      partitionCount = 4,
    )
  }

  @Test
  fun `parallel map matches sequential for disjoint ranges`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 3
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 10
          endVidInclusive = 12
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 100
          endVidInclusive = 102
        }
      }
    }

    assertParallelMatchesSequential(
      populationSpec = populationSpec,
      hashFunction = { vid: Long, salt: ByteString -> vid * 37 + salt.hashCode().toLong() },
      partitionCount = 3,
    )
  }

  @Test
  fun `parallel map matches sequential for dense multi-range population`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 64
        }
        vidRanges += vidRange {
          startVid = 200
          endVidInclusive = 220
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1000
          endVidInclusive = 1016
        }
      }
    }

    assertParallelMatchesSequential(
      populationSpec = populationSpec,
      hashFunction = { vid: Long, _: ByteString -> vid * vid + 13 },
      partitionCount = 8,
    )
  }

  private fun assertParallelMatchesSequential(
    populationSpec: PopulationSpec,
    hashFunction: (Long, ByteString) -> Long,
    partitionCount: Int = ParallelInMemoryVidIndexMap.DEFAULT_PARTITION_COUNT,
  ) {
    val sequentialHashes =
      InMemoryVidIndexMap.generateHashes(populationSpec, hashFunction).sortedBy { it.vid }
    val parallelHashes =
      ParallelInMemoryVidIndexMap.generateHashes(
          populationSpec,
          hashFunction = hashFunction,
          partitionCount = partitionCount,
        )
        .sortedBy { it.vid }

    assertThat(parallelHashes.map { it.vid })
      .containsExactlyElementsIn(sequentialHashes.map { it.vid })
      .inOrder()
    assertThat(parallelHashes.map { it.hash })
      .containsExactlyElementsIn(sequentialHashes.map { it.hash })
      .inOrder()

    val sequentialMap = InMemoryVidIndexMap.buildInternal(populationSpec, hashFunction)
    val parallelMap =
      ParallelInMemoryVidIndexMap.buildInternal(populationSpec, hashFunction, partitionCount)

    assertThat(parallelMap.size).isEqualTo(sequentialMap.size)
    assertThat(parallelMap.populationSpec).isEqualTo(sequentialMap.populationSpec)

    sequentialHashes
      .map { it.vid.toLong() }
      .forEach { vid -> assertThat(parallelMap.get(vid)).isEqualTo(sequentialMap.get(vid)) }

    if (sequentialHashes.isNotEmpty()) {
      val missingVid = sequentialHashes.last().vid.toLong() + 1
      assertFailsWith<VidNotFoundException> { sequentialMap.get(missingVid) }
      assertFailsWith<VidNotFoundException> { parallelMap.get(missingVid) }
    }

    val sequentialEntries = sequentialMap.iterator().asSequence().sortedBy { it.key }.toList()
    val parallelEntries = parallelMap.iterator().asSequence().sortedBy { it.key }.toList()

    assertThat(parallelEntries).containsExactlyElementsIn(sequentialEntries).inOrder()
  }
}
