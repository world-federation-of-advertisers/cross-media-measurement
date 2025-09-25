/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.ByteOrder
import java.util.Collections
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException.EndVidInclusiveLessThanVidStartDetail
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException.VidRangeIndex
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.toLong
import org.wfanet.measurement.eventdataprovider.shareshuffle.VidIndexMapEntryKt.value
import org.wfanet.measurement.eventdataprovider.shareshuffle.vidIndexMapEntry

@RunWith(JUnit4::class)
class InMemoryVidIndexMapTest {
  @Test
  fun `build throws when PopulationSpec has a VidRange with start greater than end`() {
    // The intent isn't to test the validator here, but to just ensure that any validation
    // error results in an exception being thrown.
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
    listOf<Boolean>(false, true).forEach { useParallelSorting ->
      val vidIndexMap = InMemoryVidIndexMap.build(testPopulationSpec, useParallelSorting)
      assertThat(vidIndexMap.size).isEqualTo(1)
      assertThat(vidIndexMap[1]).isEqualTo(0)
    }
  }

  @Test
  fun `create a InMemoryVidIndexMap with a custom hash function and multiple subpopulations`() {
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

    // The value of the hash function is constant and equal to the salt.
    // Thus, because the secondary sort key is the VID itself, the
    // ordering of the indexes of the VIDs follows the ordering of
    // the VIDs themselves.
    val hashFunction = { _: Long, salt: ByteString -> salt.toLong(ByteOrder.BIG_ENDIAN) }
    listOf<Boolean>(false, true).forEach { useParallelSorting ->
      val vidIndexMap =
        InMemoryVidIndexMap.buildInternal(testPopulationSpec, hashFunction, useParallelSorting)

      assertThat(vidIndexMap.size).isEqualTo(vidCount)
      for (entry in vidIndexMap) {
        assertThat(entry.value.index).isEqualTo(entry.key - 1)
        assertThat(entry.value.unitIntervalValue)
          .isEqualTo(entry.value.index.toDouble() / vidIndexMap.size)
      }
    }
  }

  @Test
  fun `buildInternal with parallel sorting assigns contiguous indexes`() {
    val vidCount = 1_024
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = vidCount.toLong()
        }
      }
    }
    val hashFunction = { vid: Long, _: ByteString -> vid }

    val vidIndexMap = InMemoryVidIndexMap.buildInternal(testPopulationSpec, hashFunction, true)

    assertThat(vidIndexMap.size).isEqualTo(vidCount.toLong())
    for (vid in 1..vidCount) {
      assertThat(vidIndexMap.get(vid.toLong())).isEqualTo(vid - 1)
    }
  }

  @Test
  fun `parallel hash generation matches sequential for contiguous ranges`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 10
        }
      }
    }

    val hashFunction = { vid: Long, _: ByteString -> vid * 17 }

    val parallel =
      InMemoryVidIndexMap.generateHashesParallel(populationSpec, hashFunction = hashFunction)
    val sequential = InMemoryVidIndexMap.generateHashesSequential(populationSpec, hashFunction)

    assertThat(parallel.toList()).containsExactlyElementsIn(sequential.toList()).inOrder()
  }

  @Test
  fun `parallel hash generation matches sequential for disjoint ranges`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 100
          endVidInclusive = 105
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1_000
          endVidInclusive = 1_005
        }
      }
    }

    val hashFunction =
      { vid: Long, salt: ByteString -> vid.xor(salt.toLong(ByteOrder.BIG_ENDIAN)) }

    val parallel =
      InMemoryVidIndexMap.generateHashesParallel(populationSpec, hashFunction = hashFunction)
    val sequential = InMemoryVidIndexMap.generateHashesSequential(populationSpec, hashFunction)

    assertThat(parallel.toList()).containsExactlyElementsIn(sequential.toList()).inOrder()
  }

  @Test
  fun `parallel hash generation honors custom partition count`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 32
        }
      }
    }

    val hashFunction = { vid: Long, _: ByteString -> vid * vid }

    val defaultParallel =
      InMemoryVidIndexMap.generateHashesParallel(populationSpec, hashFunction = hashFunction)
    val sequential = InMemoryVidIndexMap.generateHashesSequential(populationSpec, hashFunction)
    val forcedParallel =
      InMemoryVidIndexMap.generateHashesParallel(
        populationSpec,
        hashFunction = hashFunction,
        partitionCount = 4,
      )

    assertThat(defaultParallel.toList()).containsExactlyElementsIn(sequential.toList()).inOrder()
    assertThat(forcedParallel.toList()).containsExactlyElementsIn(sequential.toList()).inOrder()
  }

  @Test
  fun `applyPartitioned executes task for each partition`() {
    // `segments` is mutated from multiple coroutine workers; guard it to avoid lost writes.
    val segments = Collections.synchronizedList(mutableListOf<List<Int>>())
    InMemoryVidIndexMap.applyPartitioned(10, 3) { bounds ->
      segments += (bounds.startIndex until bounds.endIndexExclusive).toList()
    }

    // Partition execution order is nondeterministic; sort so we assert on coverage only.
    val flattened = segments.flatten().sorted()
    assertThat(flattened).containsExactlyElementsIn((0 until 10).toList()).inOrder()
    assertThat(segments.count { it.isNotEmpty() }).isEqualTo(3)
  }

  @Test
  fun `applyPartitioned returns empty when there is no work`() {
    var invocationCount = 0
    InMemoryVidIndexMap.applyPartitioned(0, 4) { invocationCount++ }

    assertThat(invocationCount).isEqualTo(0)
  }

  @Test
  fun `generateHashesParallel hashes every vid`() {
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
      InMemoryVidIndexMap.generateHashesParallel(
        populationSpec,
        hashFunction = { vid: Long, _: ByteString -> vid * 2 },
      )

    assertThat(hashes.map { it.vid }).containsExactly(1, 2, 3, 10, 11, 12).inOrder()
    assertThat(hashes.map { it.hash }).containsExactly(2L, 4L, 6L, 20L, 22L, 24L).inOrder()
  }

  @Test
  fun `generateHashesSequential hashes every vid`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 5
          endVidInclusive = 7
        }
      }
    }

    val hashes =
      InMemoryVidIndexMap.generateHashesSequential(
        populationSpec,
        hashFunction = { vid: Long, _: ByteString -> vid * 3 },
      )

    assertThat(hashes.map { it.vid }).containsExactly(5, 6, 7).inOrder()
    assertThat(hashes.map { it.hash }).containsExactly(15L, 18L, 21L).inOrder()
  }

  @Test
  fun `populateIndexMapPartitionMerge merges partial results`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 6
        }
      }
    }

    val hashes =
      InMemoryVidIndexMap.generateHashesParallel(
        populationSpec,
        hashFunction = { vid: Long, _: ByteString -> 100 - vid },
      )
    val sorted = hashes.copyOf().also { it.sort() }
    val indexMap = Int2IntOpenHashMap().apply { defaultReturnValue(-1) }

    InMemoryVidIndexMap.populateIndexMapPartitionMerge(sorted, indexMap, partitionCount = 2)

    for ((index, vidAndHash) in sorted.withIndex()) {
      assertThat(indexMap.get(vidAndHash.vid)).isEqualTo(index)
    }
  }
}
