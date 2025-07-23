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

package org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.ByteOrder
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
    val vidIndexMap = InMemoryVidIndexMap.buildInternal(testPopulationSpec, hashFunction)

    assertThat(vidIndexMap.size).isEqualTo(vidCount)
    for (entry in vidIndexMap) {
      assertThat(entry.value.index).isEqualTo(entry.key - 1)
      assertThat(entry.value.unitIntervalValue)
        .isEqualTo(entry.value.index.toDouble() / vidIndexMap.size)
    }
  }

  @Test
  fun `buildParallel produces same results as sequential build`() = runBlocking {
    // Create a population spec with multiple subpopulations and ranges
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 1000
        }
        vidRanges += vidRange {
          startVid = 2001
          endVidInclusive = 3000
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1001
          endVidInclusive = 2000
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 5001
          endVidInclusive = 6000
        }
        vidRanges += vidRange {
          startVid = 7001
          endVidInclusive = 8000
        }
      }
    }

    // Build using sequential method
    val sequentialMap = InMemoryVidIndexMap.build(testPopulationSpec)
    
    // Build using parallel method with different parallelism levels
    val parallelMap2 = InMemoryVidIndexMap.buildParallel(testPopulationSpec, parallelism = 2)
    val parallelMap4 = InMemoryVidIndexMap.buildParallel(testPopulationSpec, parallelism = 4)
    val parallelMap8 = InMemoryVidIndexMap.buildParallel(testPopulationSpec, parallelism = 8)
    
    // Verify sizes are the same
    assertThat(parallelMap2.size).isEqualTo(sequentialMap.size)
    assertThat(parallelMap4.size).isEqualTo(sequentialMap.size)
    assertThat(parallelMap8.size).isEqualTo(sequentialMap.size)
    
    // Verify all VIDs have the same indices
    val testVids = listOf(1L, 500L, 1000L, 1001L, 1500L, 2000L, 2001L, 2500L, 3000L, 
                          5001L, 5500L, 6000L, 7001L, 7500L, 8000L)
    
    for (vid in testVids) {
      assertThat(parallelMap2[vid]).isEqualTo(sequentialMap[vid])
      assertThat(parallelMap4[vid]).isEqualTo(sequentialMap[vid])
      assertThat(parallelMap8[vid]).isEqualTo(sequentialMap[vid])
    }
  }

  @Test
  fun `buildParallel handles single VID range correctly`() = runBlocking {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 100
        }
      }
    }

    val sequentialMap = InMemoryVidIndexMap.build(testPopulationSpec)
    val parallelMap = InMemoryVidIndexMap.buildParallel(testPopulationSpec, parallelism = 4)
    
    assertThat(parallelMap.size).isEqualTo(sequentialMap.size)
    
    // Check all VIDs
    for (vid in 1L..100L) {
      assertThat(parallelMap[vid]).isEqualTo(sequentialMap[vid])
    }
  }

  @Test
  fun `buildParallel shows progress for large populations`() = runBlocking {
    // Create a population spec with 50,000 VIDs (above the MIN_VIDS_FOR_PROGRESS_TRACKING threshold)
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 50000
        }
      }
    }

    // Build using parallel method - this should trigger progress logging
    val parallelMap = InMemoryVidIndexMap.buildParallel(testPopulationSpec, parallelism = 4)
    
    assertThat(parallelMap.size).isEqualTo(50000)
    
    // Verify a few sample VIDs work correctly
    val sampleVids = listOf(1L, 1000L, 25000L, 50000L)
    for (vid in sampleVids) {
      assertThat(parallelMap[vid]).isAtLeast(0)
      assertThat(parallelMap[vid]).isLessThan(50000)
    }
  }

}
