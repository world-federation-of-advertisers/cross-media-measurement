// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.virtualpeople.core.model

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.BranchNodeKt.attributesUpdater
import org.wfanet.virtualpeople.common.labelerEvent
import org.wfanet.virtualpeople.common.updateMatrix

private const val SEED_NUMBER = 10000

@RunWith(JUnit4::class)
class UpdateMatrixImplTest {

  @Test
  fun `no rows should throw`() {
    val config = attributesUpdater {
      updateMatrix = updateMatrix {
        columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
        columns.add(labelerEvent { personCountryCode = "COUNTRY_2" })
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("No row exists in UpdateMatrix"))
  }

  @Test
  fun `no columns should throw`() {
    val config = attributesUpdater {
      updateMatrix = updateMatrix {
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("No column exists in UpdateMatrix"))
  }

  @Test
  fun `probabilities count not match should throw`() {
    val config = attributesUpdater {
      updateMatrix = updateMatrix {
        columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
        columns.add(labelerEvent { personCountryCode = "COUNTRY_2" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
        probabilities.add(0.8f)
        probabilities.add(0.2f)
        probabilities.add(0.2f)
        probabilities.add(0.8f)
        probabilities.add(0.8f)
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("Probabilities count must equal to row * column"))
  }

  @Test
  fun `invalid probability should throw`() {
    val config = attributesUpdater {
      updateMatrix = updateMatrix {
        columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
        columns.add(labelerEvent { personCountryCode = "COUNTRY_2" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
        probabilities.add(-1.0f)
        probabilities.add(0.2f)
        probabilities.add(2.0f)
        probabilities.add(0.8f)
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("Negative probability"))
  }

  @Test
  fun `output distribition should match`() {
    /**
     * ```
     * Matrix:
     *                     "COUNTRY_1" "COUNTRY_2"
     * "UPDATED_COUNTRY_1"    0.8         0.2
     * "UPDATED_COUNTRY_2"    0.2         0.8
     * ```
     */
    val config = attributesUpdater {
      updateMatrix = updateMatrix {
        columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
        columns.add(labelerEvent { personCountryCode = "COUNTRY_2" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
        probabilities.add(0.8f)
        probabilities.add(0.2f)
        probabilities.add(0.2f)
        probabilities.add(0.8f)
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    /**
     * ```
     *  When the input person_country_code is "COUNTRY_1", the output probability distribution is
     *  person_country_code   probability
     *  "UPDATED_COUNTRY_1"      0.8
     *  "UPDATED_COUNTRY_2"      0.2
     *
     * ```
     */
    val outputCounts1 = mutableMapOf<String, Int>()
    (0 until SEED_NUMBER).forEach { seed ->
      val event =
        labelerEvent {
            personCountryCode = "COUNTRY_1"
            actingFingerprint = seed.toLong()
          }
          .toBuilder()
      updater.update(event)
      outputCounts1[event.personCountryCode] =
        outputCounts1.getOrDefault(event.personCountryCode, 0) + 1
    }
    assertEquals(2, outputCounts1.size)
    /**
     * Compares to the exact values to make sure the Kotlin and C++ implementation behave the same.
     */
    assertEquals(7993, outputCounts1["UPDATED_COUNTRY_1"])
    assertEquals(2007, outputCounts1["UPDATED_COUNTRY_2"])

    /**
     * ```
     *  When the input person_country_code is "COUNTRY_2", the output probability distribution is
     *  person_country_code   probability
     *  "UPDATED_COUNTRY_1"      0.2
     *  "UPDATED_COUNTRY_2"      0.8
     *
     * ```
     */
    val outputCounts2 = mutableMapOf<String, Int>()
    (0 until SEED_NUMBER).forEach { seed ->
      val event =
        labelerEvent {
            personCountryCode = "COUNTRY_2"
            actingFingerprint = seed.toLong()
          }
          .toBuilder()
      updater.update(event)
      outputCounts2[event.personCountryCode] =
        outputCounts2.getOrDefault(event.personCountryCode, 0) + 1
    }
    assertEquals(2, outputCounts2.size)
    /**
     * Compares to the exact values to make sure the Kotlin and C++ implementation behave the same.
     */
    assertEquals(1944, outputCounts2["UPDATED_COUNTRY_1"])
    assertEquals(8056, outputCounts2["UPDATED_COUNTRY_2"])
  }

  @Test
  fun `probabilities not normalized should throw`() {
    val config = attributesUpdater {
      updateMatrix = updateMatrix {
        columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
        columns.add(labelerEvent { personCountryCode = "COUNTRY_2" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
        probabilities.add(1.6f)
        probabilities.add(0.2f)
        probabilities.add(0.4f)
        probabilities.add(0.8f)
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("Probabilities do not sum to 1"))
  }

  @Test
  fun `no matching not pass should throw`() {
    val config = attributesUpdater {
      updateMatrix = updateMatrix {
        columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
        columns.add(labelerEvent { personCountryCode = "COUNTRY_2" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
        probabilities.add(0.8f)
        probabilities.add(0.2f)
        probabilities.add(0.2f)
        probabilities.add(0.8f)
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)
    val event =
      labelerEvent {
          personCountryCode = "COUNTRY_3"
          actingFingerprint = 0L
        }
        .toBuilder()
    val error = assertFailsWith<IllegalStateException> { updater.update(event) }
    assertTrue(error.message!!.contains("No column matching for event"))
    assertEquals("COUNTRY_3", event.personCountryCode)
  }

  @Test
  fun `no matching pass should do nothing`() {
    val config = attributesUpdater {
      updateMatrix = updateMatrix {
        columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
        columns.add(labelerEvent { personCountryCode = "COUNTRY_2" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
        rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
        probabilities.add(0.8f)
        probabilities.add(0.2f)
        probabilities.add(0.2f)
        probabilities.add(0.8f)
        passThroughNonMatches = true
        randomSeed = "TestSeed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)
    val event =
      labelerEvent {
          personCountryCode = "COUNTRY_3"
          actingFingerprint = 0L
        }
        .toBuilder()

    updater.update(event)
    assertEquals("COUNTRY_3", event.personCountryCode)
  }
}
