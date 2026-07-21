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

import com.google.protobuf.fieldMask
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.BranchNodeKt.attributesUpdater
import org.wfanet.virtualpeople.common.SparseUpdateMatrixKt.column
import org.wfanet.virtualpeople.common.labelerEvent
import org.wfanet.virtualpeople.common.sparseUpdateMatrix

private const val SEED_NUMBER = 10000

@RunWith(JUnit4::class)
class SparseUpdateMatrixImplTest {

  @Test
  fun `no columns should throw`() {
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("No column exists in SparseUpdateMatrix"))
  }

  @Test
  fun `no columnAttrs should throw`() {
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(
          column {
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            probabilities.add(1.0f)
          }
        )
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("No column_attrs in the column in SparseUpdateMatrix"))
  }

  @Test
  fun `no rows should throw`() {
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(column { columnAttrs = labelerEvent { personCountryCode = "COUNTRY_1" } })
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("No row exists in the column in SparseUpdateMatrix"))
  }

  @Test
  fun `no rows in second column should throw`() {
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(
          column {
            columnAttrs = labelerEvent { personCountryCode = "COUNTRY_1" }
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            probabilities.add(1.0f)
          }
        )
        columns.add(column { columnAttrs = labelerEvent { personCountryCode = "COUNTRY_2" } })
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("No row exists in the column in SparseUpdateMatrix"))
  }

  @Test
  fun `probabilities count not match should throw`() {
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(
          column {
            columnAttrs = labelerEvent { personCountryCode = "COUNTRY_1" }
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            probabilities.add(1.0f)
            probabilities.add(0.0f)
          }
        )
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("Rows and probabilities are not aligned"))
  }

  @Test
  fun `invalid probability should throw`() {
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(
          column {
            columnAttrs = labelerEvent { personCountryCode = "COUNTRY_1" }
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            probabilities.add(-1.0f)
          }
        )
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
     * "UPDATED_COUNTRY_2"    0.2         0.4
     * "UPDATED_COUNTRY_3"    0.0         0.4
     * ```
     */
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(
          column {
            columnAttrs = labelerEvent { personCountryCode = "COUNTRY_1" }
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
            probabilities.add(0.8f)
            probabilities.add(0.2f)
          }
        )
        columns.add(
          column {
            columnAttrs = labelerEvent { personCountryCode = "COUNTRY_2" }
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_3" })
            probabilities.add(0.2f)
            probabilities.add(0.4f)
            probabilities.add(0.4f)
          }
        )
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
     *  "UPDATED_COUNTRY_2"      0.4
     *  "UPDATED_COUNTRY_3"      0.4
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
    assertEquals(3, outputCounts2.size)
    /**
     * Compares to the exact values to make sure the Kotlin and C++ implementation behave the same.
     */
    assertEquals(2037, outputCounts2["UPDATED_COUNTRY_1"])
    assertEquals(4025, outputCounts2["UPDATED_COUNTRY_2"])
    assertEquals(3938, outputCounts2["UPDATED_COUNTRY_3"])
  }

  @Test
  fun `probabilities not normalized should throw`() {
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(
          column {
            columnAttrs = labelerEvent { personCountryCode = "COUNTRY_1" }
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
            probabilities.add(1.6f)
            probabilities.add(0.4f)
          }
        )
        columns.add(
          column {
            columnAttrs = labelerEvent { personCountryCode = "COUNTRY_2" }
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_3" })
            probabilities.add(0.2f)
            probabilities.add(0.4f)
            probabilities.add(0.4f)
          }
        )
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
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(
          column {
            columnAttrs = labelerEvent { personCountryCode = "COUNTRY_1" }
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            probabilities.add(1.0f)
          }
        )
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)
    val event =
      labelerEvent {
          personCountryCode = "COUNTRY_2"
          actingFingerprint = 0L
        }
        .toBuilder()
    val error = assertFailsWith<IllegalStateException> { updater.update(event) }
    assertTrue(error.message!!.contains("No column matching for event"))
    assertEquals("COUNTRY_2", event.personCountryCode)
  }

  @Test
  fun `no matching pass should do nothing`() {
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(
          column {
            columnAttrs = labelerEvent { personCountryCode = "COUNTRY_1" }
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            probabilities.add(1.0f)
          }
        )
        passThroughNonMatches = true
        randomSeed = "TestSeed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)
    val event =
      labelerEvent {
          personCountryCode = "COUNTRY_2"
          actingFingerprint = 0L
        }
        .toBuilder()

    updater.update(event)
    assertEquals("COUNTRY_2", event.personCountryCode)
  }

  @Test
  fun `hash field mask should work`() {
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(
          column {
            columnAttrs = labelerEvent {
              personCountryCode = "COUNTRY_1"
              personRegionCode = "REGION_1"
            }
            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
            probabilities.add(1.0f)
          }
        )
        hashFieldMask = fieldMask { paths.add("person_country_code") }
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    /** personCountryCode matches. personRegionCode is ignored as not in hashFieldMask. */
    val event1 =
      labelerEvent {
          personCountryCode = "COUNTRY_1"
          personRegionCode = "REGION_2"
          actingFingerprint = 0L
        }
        .toBuilder()
    updater.update(event1)
    assertEquals("UPDATED_COUNTRY_1", event1.personCountryCode)

    /** personCountryCode matches. personRegionCode is ignored as not in hashFieldMask. */
    val event2 =
      labelerEvent {
          personCountryCode = "COUNTRY_1"
          actingFingerprint = 0L
        }
        .toBuilder()
    updater.update(event2)
    assertEquals("UPDATED_COUNTRY_1", event2.personCountryCode)

    /** No match. */
    val event3 =
      labelerEvent {
          personCountryCode = "COUNTRY_2"
          personRegionCode = "REGION_2"
          actingFingerprint = 0L
        }
        .toBuilder()
    val error = assertFailsWith<IllegalStateException> { updater.update(event3) }
    assertTrue(error.message!!.contains("No column matching for event"))
    assertEquals("COUNTRY_2", event3.personCountryCode)
  }

  @Test
  fun `hash field mask not set should fail`() {
    val config = attributesUpdater {
      sparseUpdateMatrix = sparseUpdateMatrix {
        columns.add(
          column {
            columnAttrs = labelerEvent { personRegionCode = "REGION_1" }
            rows.add(
              labelerEvent {
                personCountryCode = "UPDATED_COUNTRY_1"
                personRegionCode = "UPDATED_REGION_1"
              }
            )
            probabilities.add(1.0f)
          }
        )
        hashFieldMask = fieldMask {
          paths.add("person_country_code")
          paths.add("person_region_code")
        }
        passThroughNonMatches = false
        randomSeed = "TestSeed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    /** person_country_code matches as not set. person_region_code matches. */
    val event1 =
      labelerEvent {
          personRegionCode = "REGION_1"
          actingFingerprint = 0L
        }
        .toBuilder()
    updater.update(event1)
    assertEquals("UPDATED_COUNTRY_1", event1.personCountryCode)
    assertEquals("UPDATED_REGION_1", event1.personRegionCode)

    /** person_country_code does not match, should be not set. */
    val event2 =
      labelerEvent {
          personCountryCode = "COUNTRY_1"
          personRegionCode = "REGION_1"
          actingFingerprint = 0L
        }
        .toBuilder()
    val error = assertFailsWith<IllegalStateException> { updater.update(event2) }
    assertTrue(error.message!!.contains("No column matching for event"))
    assertEquals("COUNTRY_1", event2.personCountryCode)
    assertEquals("REGION_1", event2.personRegionCode)
  }
}
