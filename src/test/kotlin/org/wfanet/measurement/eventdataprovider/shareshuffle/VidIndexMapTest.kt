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
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.toByteString

@RunWith(JUnit4::class)
class VidIndexMapTest {
  @Test
  fun `throws when PopulationSpec contains a VidRange with startVid greater than endVidInclusive`() {
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
      assertFailsWith<PopulationSpecValidationException>(
        message = "Expected exception",
        block = { VidIndexMap(testPopulationSpec) },
      )
    assertThat(exception.toString())
      .contains(
        "The endVidInclusive of the range at 'SubpopulationIndex: 0 VidRangeIndex: 0' " +
          "must be greater than or equal to the startVid."
      )
  }

  @Test
  fun `create a VidIndexMap of size zero with an empty PopulationSpec`() {
    val emptyPopulationSpec = populationSpec {}
    val vidIndexMap = VidIndexMap(emptyPopulationSpec)
    assertThat(vidIndexMap.size).isEqualTo(0)
  }

  @Test
  fun `create a VidIndexMap of size one`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 1
        }
      }
    }
    val vidIndexMap = VidIndexMap(testPopulationSpec)
    assertThat(vidIndexMap.size).isEqualTo(1)
    assertThat(vidIndexMap[1]).isEqualTo(0)
  }

  @Test
  fun `create a VidIndexMap with multiple subpopulations`() {
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
          endVidInclusive = 10
        }
      }
    }

    // We'll pass a salt which we'll confirm is the value we expect.
    // The output of the hash is just the VID.
    val salt = 100L
    val hashFunction = { vid: Long, localSalt: ByteString ->
      assertThat(localSalt).isEqualTo(salt.toByteString(ByteOrder.BIG_ENDIAN))
      vid.toByteString(ByteOrder.BIG_ENDIAN)
    }

    val vidIndexMap =
      VidIndexMap(testPopulationSpec, salt.toByteString(ByteOrder.BIG_ENDIAN), hashFunction)

    val vidCount = 10L
    assertThat(vidIndexMap.size).isEqualTo(vidCount)
    for (i in 1..vidCount) {
      assertThat(vidIndexMap[i]).isEqualTo(i - 1)
    }
  }
}
