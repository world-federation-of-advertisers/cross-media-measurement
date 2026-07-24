// Copyright 2024 The Cross-Media Measurement Authors
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
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.BranchNodeKt.attributesUpdater
import org.wfanet.virtualpeople.common.eventId
import org.wfanet.virtualpeople.common.geometricShredder
import org.wfanet.virtualpeople.common.labelerEvent
import org.wfanet.virtualpeople.common.labelerInput

/** randomness_field value is from 1 to RANDOMNESS_KEY_NUMBER. */
private const val RANDOMNESS_KEY_NUMBER = 10000

/** target_field value is from 0 to TARGET_KEY_NUMBER. */
private const val TARGET_KEY_NUMBER = 100

@RunWith(JUnit4::class)
class GeometricShredderImplTest {

  @Test
  fun `negative psi should throw`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = -0.01f
        randomnessField = "labeler_input.event_id.id_fingerprint"
        targetField = "acting_fingerprint"
        randomSeed = "seed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("Psi is not in [0, 1] in GeometricShredder:"))
  }

  @Test
  fun `psi larger than one should throw`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 1.01f
        randomnessField = "labeler_input.event_id.id_fingerprint"
        targetField = "acting_fingerprint"
        randomSeed = "seed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("Psi is not in [0, 1] in GeometricShredder:"))
  }

  @Test
  fun `invalid randomness field should throw`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 0.5f
        randomnessField = "labeler_input.event_id.__INVALID_FIELD__"
        targetField = "acting_fingerprint"
        randomSeed = "seed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("The field name is invalid:"))
  }

  @Test
  fun `non-uint64 randomness field should throw`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 0.5f
        randomnessField = "labeler_input.event_id.id"
        targetField = "acting_fingerprint"
        randomSeed = "seed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(
      error.message!!.contains("randomness_field type is not uint64 in GeometricShredder:")
    )
  }

  @Test
  fun `invalid target field should throw`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 0.5f
        randomnessField = "labeler_input.event_id.id_fingerprint"
        targetField = "__INVALID_FIELD__"
        randomSeed = "seed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("The field name is invalid:"))
  }

  @Test
  fun `non-uint64 target field should throw`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 0.5f
        randomnessField = "labeler_input.event_id.id_fingerprint"
        targetField = "acting_demo_id_space"
        randomSeed = "seed"
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("target_field type is not uint64 in GeometricShredder:"))
  }

  @Test
  fun `randomness field not set should throw`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 1f
        randomnessField = "labeler_input.event_id.id_fingerprint"
        targetField = "acting_fingerprint"
        randomSeed = "seed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)
    val event = labelerEvent { actingFingerprint = 1 }.toBuilder()
    val error = assertFailsWith<IllegalStateException> { updater.update(event) }
    assertTrue(error.message!!.contains("The randomness field is not set in the event."))
  }

  @Test
  fun `target field not set should throw`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 1f
        randomnessField = "labeler_input.event_id.id_fingerprint"
        targetField = "acting_fingerprint"
        randomSeed = "seed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)
    val event =
      labelerEvent { labelerInput = labelerInput { eventId = eventId { idFingerprint = 1L } } }
        .toBuilder()
    val error = assertFailsWith<IllegalStateException> { updater.update(event) }
    assertTrue(error.message!!.contains("The target field is not set in the event."))
  }

  @Test
  fun `no shred with psi as zero`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 0f
        randomnessField = "labeler_input.event_id.id_fingerprint"
        targetField = "acting_fingerprint"
        randomSeed = "seed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    (0 until TARGET_KEY_NUMBER).forEach { actingFp ->
      (1 until RANDOMNESS_KEY_NUMBER + 1).forEach { eventIdFp ->
        val event =
          labelerEvent {
              labelerInput = labelerInput {
                eventId = eventId { idFingerprint = eventIdFp.toLong() }
              }
              actingFingerprint = actingFp.toLong()
            }
            .toBuilder()
        updater.update(event)
        assertEquals(actingFp.toLong(), event.actingFingerprint)
      }
    }
  }

  @Test
  fun `always shred with psi as one`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 1f
        randomnessField = "labeler_input.event_id.id_fingerprint"
        targetField = "acting_fingerprint"
        randomSeed = "seed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    (0 until TARGET_KEY_NUMBER).forEach { actingFp ->
      (1 until RANDOMNESS_KEY_NUMBER + 1).forEach { eventIdFp ->
        val event =
          labelerEvent {
              labelerInput = labelerInput {
                eventId = eventId { idFingerprint = eventIdFp.toLong() }
              }
              actingFingerprint = actingFp.toLong()
            }
            .toBuilder()
        updater.update(event)
        assertNotEquals(actingFp.toLong(), event.actingFingerprint)
      }
    }
  }

  @Test
  fun `no shred with psi as one randomness value as zero`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 1f
        randomnessField = "labeler_input.event_id.id_fingerprint"
        targetField = "acting_fingerprint"
        randomSeed = "seed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    (0 until TARGET_KEY_NUMBER).forEach { actingFp ->
      val event =
        labelerEvent {
            labelerInput = labelerInput { eventId = eventId { idFingerprint = 0L } }
            actingFingerprint = actingFp.toLong()
          }
          .toBuilder()
      updater.update(event)
      assertEquals(actingFp.toLong(), event.actingFingerprint)
    }
  }

  @Test
  fun `shred with probability should match`() {
    val config = attributesUpdater {
      geometricShredder = geometricShredder {
        psi = 0.65f
        randomnessField = "labeler_input.event_id.id_fingerprint"
        targetField = "acting_fingerprint"
        randomSeed = "seed"
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    (0 until TARGET_KEY_NUMBER).forEach { actingFp ->
      var shredCount = 0
      (1 until RANDOMNESS_KEY_NUMBER + 1).forEach { eventIdFp ->
        val event =
          labelerEvent {
              labelerInput = labelerInput {
                eventId = eventId { idFingerprint = eventIdFp.toLong() }
              }
              actingFingerprint = actingFp.toLong()
            }
            .toBuilder()
        updater.update(event)
        if (event.actingFingerprint != actingFp.toLong()) {
          ++shredCount
        }
      }
      /**
       * Compares to the exact values to make sure the Kotlin and C++ implementation behave the
       * same.
       */
      assertEquals(6533, shredCount)
    }

    /** Test again with different value in randomness field(idFingerprint). */
    (0 until TARGET_KEY_NUMBER).forEach { actingFp ->
      var shredCount = 0
      (1 until RANDOMNESS_KEY_NUMBER + 1).forEach { eventIdFp ->
        val event =
          labelerEvent {
              labelerInput = labelerInput {
                eventId = eventId { idFingerprint = eventIdFp.toLong() + 20000 }
              }
              actingFingerprint = actingFp.toLong()
            }
            .toBuilder()
        updater.update(event)
        if (event.actingFingerprint != actingFp.toLong()) {
          ++shredCount
        }
      }
      /**
       * Compares to the exact values to make sure the Kotlin and C++ implementation behave the
       * same.
       */
      assertEquals(6442, shredCount)
    }
  }
}
