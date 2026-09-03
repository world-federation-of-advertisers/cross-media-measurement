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
import org.wfanet.virtualpeople.common.labelerEvent
import org.wfanet.virtualpeople.common.multiplicity

private const val EVENT_COUNT = 10000

@RunWith(JUnit4::class)
class MultiplicityImplTest {

  @Test
  fun `multiplicity ref not set should throw`() {
    val config = multiplicity {
      maxValue = 1.2
      capAtMax = true
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val error = assertFailsWith<IllegalStateException> { MultiplicityImpl.build(config) }
    assertTrue(error.message!!.contains("Multiplicity must set multiplicity_ref"))
  }

  @Test
  fun `invalid multiplicity field name should throw`() {
    val config = multiplicity {
      expectedMultiplicityField = "bad field name"
      maxValue = 1.2
      capAtMax = true
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val error = assertFailsWith<IllegalStateException> { MultiplicityImpl.build(config) }
    assertTrue(error.message!!.contains("The field name is invalid"))
  }

  @Test
  fun `invalid multiplicity field type should throw`() {
    val config = multiplicity {
      expectedMultiplicityField = "corrected_demo"
      maxValue = 1.2
      capAtMax = true
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val error = assertFailsWith<IllegalStateException> { MultiplicityImpl.build(config) }
    assertTrue(error.message!!.contains("Unsupported field type"))
  }

  @Test
  fun `person index field not set should throw`() {
    val config = multiplicity {
      expectedMultiplicityField = "expected_multiplicity"
      maxValue = 1.2
      capAtMax = true
      randomSeed = "test multiplicity"
    }
    val error = assertFailsWith<IllegalStateException> { MultiplicityImpl.build(config) }
    assertTrue(error.message!!.contains("Multiplicity must set person_index_field"))
  }

  @Test
  fun `invalid person index field name should throw`() {
    val config = multiplicity {
      expectedMultiplicityField = "expected_multiplicity"
      maxValue = 1.2
      capAtMax = true
      personIndexField = "bad field name"
      randomSeed = "test multiplicity"
    }
    val error = assertFailsWith<IllegalStateException> { MultiplicityImpl.build(config) }
    assertTrue(error.message!!.contains("The field name is invalid"))
  }

  @Test
  fun `invalid person index field type should throw`() {
    val config = multiplicity {
      expectedMultiplicityField = "expected_multiplicity"
      maxValue = 1.2
      capAtMax = true
      personIndexField = "expected_multiplicity"
      randomSeed = "test multiplicity"
    }
    val error = assertFailsWith<IllegalStateException> { MultiplicityImpl.build(config) }
    assertTrue(error.message!!.contains("Invalid type for person_index_field"))
  }

  @Test
  fun `max value not set should throw`() {
    val config = multiplicity {
      expectedMultiplicityField = "expected_multiplicity"
      capAtMax = true
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val error = assertFailsWith<IllegalStateException> { MultiplicityImpl.build(config) }
    assertTrue(error.message!!.contains("Multiplicity must set max_value"))
  }

  @Test
  fun `cap at max not set should throw`() {
    val config = multiplicity {
      expectedMultiplicityField = "expected_multiplicity"
      maxValue = 1.2
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val error = assertFailsWith<IllegalStateException> { MultiplicityImpl.build(config) }
    assertTrue(error.message!!.contains("Multiplicity must set cap_at_max"))
  }

  @Test
  fun `random seed not set should throw`() {
    val config = multiplicity {
      expectedMultiplicityField = "expected_multiplicity"
      maxValue = 1.2
      capAtMax = true
      personIndexField = "multiplicity_person_index"
    }
    val error = assertFailsWith<IllegalStateException> { MultiplicityImpl.build(config) }
    assertTrue(error.message!!.contains("Multiplicity must set random_seed"))
  }

  @Test
  fun `explicit multiplicity and cap at max true`() {
    val config = multiplicity {
      expectedMultiplicity = 2.5
      maxValue = 1.3
      capAtMax = true
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val multiplicity = MultiplicityImpl.build(config)

    var personTotal = 0
    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent { actingFingerprint = it.toLong() }
      val cloneCount = multiplicity.computeEventMultiplicity(input)
      assertTrue { cloneCount in listOf(1, 2) }
      personTotal += cloneCount
    }
    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * result should be around 1.3 * EVENT_COUNT = 13000
     */
    assertEquals(12947, personTotal)
  }

  @Test
  fun `explicit multiplicity and cap at max false`() {
    val config = multiplicity {
      expectedMultiplicity = 2.5
      maxValue = 1.3
      capAtMax = false
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val multiplicity = MultiplicityImpl.build(config)

    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent { actingFingerprint = it.toLong() }
      val error =
        assertFailsWith<IllegalStateException> { multiplicity.computeEventMultiplicity(input) }
      assertTrue(error.message!!.contains("exceeds the specified max value"))
    }
  }

  @Test
  fun `explicit multiplicity is negative should throw`() {
    val config = multiplicity {
      expectedMultiplicity = -1.2
      maxValue = 1.3
      capAtMax = false
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val multiplicity = MultiplicityImpl.build(config)

    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent { actingFingerprint = it.toLong() }
      val error =
        assertFailsWith<IllegalStateException> { multiplicity.computeEventMultiplicity(input) }
      assertTrue(error.message!!.contains("multiplicity must >= 0"))
    }
  }

  @Test
  fun `multiplicity field and cap at max true`() {
    val config = multiplicity {
      expectedMultiplicityField = "expected_multiplicity"
      maxValue = 1.3
      capAtMax = true
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val multiplicity = MultiplicityImpl.build(config)

    /** multiplicity field is not set, throws error. */
    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent { actingFingerprint = it.toLong() }
      val error =
        assertFailsWith<IllegalStateException> { multiplicity.computeEventMultiplicity(input) }
      assertTrue(error.message!!.contains("The multiplicity field is not set"))
    }

    /** multiplicity > max_value, cap at max_value. */
    var personTotal = 0
    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent {
        actingFingerprint = it.toLong()
        expectedMultiplicity = 2.0
      }
      val cloneCount = multiplicity.computeEventMultiplicity(input)
      assertTrue { cloneCount in listOf(1, 2) }
      personTotal += cloneCount
    }
    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * result should be around 1.3 * EVENT_COUNT = 13000
     */
    assertEquals(12947, personTotal)

    /** multiplicity < max_value */
    personTotal = 0
    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent {
        actingFingerprint = it.toLong()
        expectedMultiplicity = 1.2
      }
      val cloneCount = multiplicity.computeEventMultiplicity(input)
      assertTrue { cloneCount in listOf(1, 2) }
      personTotal += cloneCount
    }
    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * result should be around 1.2 * EVENT_COUNT = 12000
     */
    assertEquals(11949, personTotal)
  }

  @Test
  fun `multiplicity field and cap at max false`() {
    val config = multiplicity {
      expectedMultiplicityField = "expected_multiplicity"
      maxValue = 1.3
      capAtMax = false
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val multiplicity = MultiplicityImpl.build(config)

    /** multiplicity field is not set, throws error. */
    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent { actingFingerprint = it.toLong() }
      val error =
        assertFailsWith<IllegalStateException> { multiplicity.computeEventMultiplicity(input) }
      assertTrue(error.message!!.contains("The multiplicity field is not set"))
    }

    /** multiplicity > max_value, return error. */
    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent {
        actingFingerprint = it.toLong()
        expectedMultiplicity = 2.0
      }
      val error =
        assertFailsWith<IllegalStateException> { multiplicity.computeEventMultiplicity(input) }
      assertTrue(error.message!!.contains("exceeds the specified max value"))
    }

    /** multiplicity < max_value */
    var personTotal = 0
    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent {
        actingFingerprint = it.toLong()
        expectedMultiplicity = 1.2
      }
      val cloneCount = multiplicity.computeEventMultiplicity(input)
      assertTrue { cloneCount in listOf(1, 2) }
      personTotal += cloneCount
    }
    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * result should be around 1.2 * EVENT_COUNT = 12000
     */
    assertEquals(11949, personTotal)
  }

  @Test
  fun `multiplicity field is negative should throw`() {
    val config = multiplicity {
      expectedMultiplicityField = "expected_multiplicity"
      maxValue = 1.3
      capAtMax = false
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val multiplicity = MultiplicityImpl.build(config)

    /** return error if multiplicity < 0. */
    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent {
        actingFingerprint = it.toLong()
        expectedMultiplicity = -1.2
      }
      val error =
        assertFailsWith<IllegalStateException> { multiplicity.computeEventMultiplicity(input) }
      assertTrue(error.message!!.contains("multiplicity must >= 0"))
    }
  }

  @Test
  fun `multiplicity less than one`() {
    val config = multiplicity {
      expectedMultiplicity = 0.3
      maxValue = 1.3
      capAtMax = true
      personIndexField = "multiplicity_person_index"
      randomSeed = "test multiplicity"
    }
    val multiplicity = MultiplicityImpl.build(config)

    var personTotal = 0
    (0 until EVENT_COUNT).forEach {
      val input = labelerEvent { actingFingerprint = it.toLong() }
      val cloneCount = multiplicity.computeEventMultiplicity(input)
      assertTrue { cloneCount in listOf(0, 1) }
      personTotal += cloneCount
    }
    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * result should be around 0.3 * EVENT_COUNT = 3000
     */
    assertEquals(2947, personTotal)
  }
}
