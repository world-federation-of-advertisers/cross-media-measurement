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
package org.wfanet.measurement.eventdataprovider.shareshuffle

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.ByteOrder
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException.EndVidInclusiveLessThanVidStartDetail
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException.VidRangeIndex
import org.wfanet.measurement.api.v2alpha.populationSpec

@RunWith(JUnit4::class)
class VidIndexMapTest {
  @Test
  fun `construction throws when PopulationSpec has a VidRange with start greater than end`() {
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
        InMemoryVidIndexMap(testPopulationSpec)
      }
    val details = exception.details[0] as EndVidInclusiveLessThanVidStartDetail
    assertThat(details.index).isEqualTo(VidRangeIndex(0, 0))
  }

  @Test
  fun `create a InMemoryVidIndexMap of size zero with an empty PopulationSpec`() {
    val emptyPopulationSpec = populationSpec {}
    val vidIndexMap = InMemoryVidIndexMap(emptyPopulationSpec)
    assertThat(vidIndexMap.size).isEqualTo(0)
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
    val vidIndexMap = InMemoryVidIndexMap(testPopulationSpec)
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
    val hashFunction = { _: Long, salt: Long -> salt}
    val vidIndexMap = InMemoryVidIndexMap(testPopulationSpec, hashFunction)

    assertThat(vidIndexMap.size).isEqualTo(vidCount)
    for (vid in 1..vidCount) {
      val index = vidIndexMap[vid]
      assertThat(index).isEqualTo(vid - 1)
    }
  }
}
