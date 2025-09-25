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
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import java.nio.ByteOrder
import java.util.Collections
import kotlin.sequences.asSequence
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException.EndVidInclusiveLessThanVidStartDetail
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException.VidRangeIndex
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.toLong
import org.wfanet.measurement.eventdataprovider.shareshuffle.VidIndexMapEntryKt.value
import org.wfanet.measurement.eventdataprovider.shareshuffle.vidIndexMapEntry
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.ParallelInMemoryVidIndexMap

@RunWith(JUnit4::class)
class InMemoryVidIndexMapTest {

  private fun assertParallelMatchesSequential(
    populationSpec: PopulationSpec,
    hashFunction: (Long, ByteString) -> Long,
    partitionCount: Int = ParallelInMemoryVidIndexMap.DEFAULT_PARTITION_COUNT,
  ) {
    val sequentialHashes =
      InMemoryVidIndexMap.generateHashesList(populationSpec, hashFunction).sortedBy { it.vid }
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

    for (vid in sequentialHashes.map { it.vid.toLong() }) {
      assertThat(parallelMap.get(vid)).isEqualTo(sequentialMap.get(vid))
    }

    if (sequentialHashes.isNotEmpty()) {
      val missingVid = sequentialHashes.last().vid.toLong() + 1
      assertFailsWith<VidNotFoundException> { sequentialMap.get(missingVid) }
      assertFailsWith<VidNotFoundException> { parallelMap.get(missingVid) }
    }

    val sequentialEntries = sequentialMap.iterator().asSequence().sortedBy { it.key }.toList()
    val parallelEntries = parallelMap.iterator().asSequence().sortedBy { it.key }.toList()

    assertThat(parallelEntries)
      .containsExactlyElementsIn(sequentialEntries)
      .inOrder()
  }

  // The intent isn't to test the validator here, but to just ensure that any validation
  // error results in an exception being thrown.
  @Test
  fun `build throws when PopulationSpec has a VidRange with start greater than end`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 2
          endVidInclusive = 1
        }
      }
    }
    val exception =
      assertFailsWith<PopulationSpecValidationException>("Expected exception") {
        InMemoryVidIndexMap.build(testPopulationSpec)
      }
    val details = exception.details[0] as EndVidInclusiveLessThanVidStartDetail
    assertThat(details.index).isEqualTo(VidRangeIndex(0, 0))
  }

  @Test
  fun `build a InMemoryVidIndexMap of size zero with an empty PopulationSpec`() {
    val emptyPopulationSpec = populationSpec {}
    val vidIndexMap = InMemoryVidIndexMap.build(emptyPopulationSpec)
    assertThat(vidIndexMap.size).isEqualTo(0)
  }

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

  @Test
  fun `create a InMemoryVidIndexMap of size one`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 1
        }
      }
    }
    val vidIndexMap = InMemoryVidIndexMap.build(testPopulationSpec)
    assertThat(vidIndexMap.size).isEqualTo(1)
    assertThat(vidIndexMap[1]).isEqualTo(0)
  }

  @Test
  fun `create a ParallelInMemoryVidIndexMap of size one`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 1
        }
      }
    }
    val vidIndexMap = ParallelInMemoryVidIndexMap.build(testPopulationSpec)
    assertThat(vidIndexMap.size).isEqualTo(1)
    assertThat(vidIndexMap[1L]).isEqualTo(0)
  }

  @Test
  fun `create an InMemoryVidIndexMap with a custom hash function and multiple subpopulations`() {
    val vidCount = 10L
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 1
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 2
          endVidInclusive = vidCount
        }
      }
    }

    val hashFunction = { _: Long, salt: ByteString -> salt.toLong(ByteOrder.BIG_ENDIAN) }
    val vidIndexMap = InMemoryVidIndexMap.buildInternal(testPopulationSpec, hashFunction)

    assertThat(vidIndexMap.size).isEqualTo(vidCount)
    for (entry in vidIndexMap) {
      assertThat(entry.value.index).isEqualTo(entry.key - 1)
      assertThat(entry.value.unitIntervalValue)
        .isEqualTo(entry.value.index.toDouble() / vidIndexMap.size)
    }
  }

  @Test
  fun `create a ParallelInMemoryVidIndexMap with a custom hash function`() {
    val vidCount = 10
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = vidCount.toLong()
        }
      }
    }

    val hashFunction = { vid: Long, _: ByteString -> vid }
    val vidIndexMap = ParallelInMemoryVidIndexMap.buildInternal(testPopulationSpec, hashFunction)

    assertThat(vidIndexMap.size).isEqualTo(vidCount.toLong())
    for (vid in 1..vidCount) {
      assertThat(vidIndexMap.get(vid.toLong())).isEqualTo(vid - 1)
    }
  }

  @Test
  fun `parallel map matches sequential map with single subpopulation`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 32
        }
      }
    }

    val hashFunction = { vid: Long, _: ByteString -> vid * 5 }
    assertParallelMatchesSequential(populationSpec, hashFunction, partitionCount = 4)
  }

  @Test
  fun `parallel map matches sequential map with disjoint subpopulations`() {
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

    val hashFunction = { vid: Long, salt: ByteString -> vid * 37 + salt.toLong(ByteOrder.BIG_ENDIAN) }
    assertParallelMatchesSequential(populationSpec, hashFunction, partitionCount = 3)
  }

  @Test
  fun `parallel map matches sequential map with dense subpopulations`() {
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

    val hashFunction = { vid: Long, _: ByteString -> vid * vid + 13 }
    assertParallelMatchesSequential(populationSpec, hashFunction, partitionCount = 8)
  }

  @Test
  fun `applyPartitioned executes task for each partition`() {
    val segments = Collections.synchronizedList(mutableListOf<List<Int>>())
    ParallelInMemoryVidIndexMap.applyPartitioned(10, 3) { bounds ->
      segments += (bounds.startIndex until bounds.endIndexExclusive).toList()
    }

    val flattened = segments.flatten().sorted()
    assertThat(flattened).containsExactlyElementsIn((0 until 10).toList()).inOrder()
    assertThat(segments.count { it.isNotEmpty() }).isEqualTo(3)
  }

  @Test
  fun `ParallelInMemoryVidIndexMap generateHashes hashes every vid`() {
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
    }

    val hashes =
      ParallelInMemoryVidIndexMap.generateHashes(
        populationSpec,
        hashFunction = { vid: Long, _: ByteString -> vid * 2 },
      )
    val sortedHashes = hashes.sortedBy { it.vid }

    assertThat(sortedHashes.map { it.vid }).containsExactly(1, 2, 3, 10, 11, 12).inOrder()
    assertThat(sortedHashes.map { it.hash }).containsExactly(2L, 4L, 6L, 20L, 22L, 24L).inOrder()
  }

  @Test
  fun `populateIndexMap merges partial results`() {
    val hashes = arrayOf(
      ParallelInMemoryVidIndexMap.VidAndHash(1, 99L),
      ParallelInMemoryVidIndexMap.VidAndHash(2, 98L),
      ParallelInMemoryVidIndexMap.VidAndHash(3, 97L),
      ParallelInMemoryVidIndexMap.VidAndHash(10, 90L),
      ParallelInMemoryVidIndexMap.VidAndHash(11, 89L),
      ParallelInMemoryVidIndexMap.VidAndHash(12, 88L)
    )
    val indexMap = Int2IntOpenHashMap().apply { defaultReturnValue(-1) }

    val sortedHashes = hashes.sortedArray()
    ParallelInMemoryVidIndexMap.populateIndexMap(sortedHashes, indexMap, partitionCount = 2)

    for ((index, vidAndHash) in sortedHashes.withIndex()) {
      assertThat(indexMap.get(vidAndHash.vid)).isEqualTo(index)
    }
  }

}
