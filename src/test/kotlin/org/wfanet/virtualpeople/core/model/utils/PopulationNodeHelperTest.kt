// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.virtualpeople.core.model.utils

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.PersonLabelAttributes
import org.wfanet.virtualpeople.common.QuantumLabel

@RunWith(JUnit4::class)
class PopulationNodeHelperTest {

  @Test
  fun `computeVidSeed is deterministic`() {
    val seed1 = PopulationNodeHelper.computeVidSeed("test-seed", 42L)
    val seed2 = PopulationNodeHelper.computeVidSeed("test-seed", 42L)
    assertEquals(seed1, seed2)
  }

  @Test
  fun `computeVidSeed differs for different seeds`() {
    val seed1 = PopulationNodeHelper.computeVidSeed("seed-a", 42L)
    val seed2 = PopulationNodeHelper.computeVidSeed("seed-b", 42L)
    assertNotEquals(seed1, seed2)
  }

  @Test
  fun `computeVidSeed differs for different fingerprints`() {
    val seed1 = PopulationNodeHelper.computeVidSeed("test-seed", 1L)
    val seed2 = PopulationNodeHelper.computeVidSeed("test-seed", 2L)
    assertNotEquals(seed1, seed2)
  }

  @Test
  fun `collapseQuantumLabel selects label by probability`() {
    val quantumLabel =
      QuantumLabel.newBuilder()
        .addLabels(PersonLabelAttributes.getDefaultInstance())
        .addLabels(PersonLabelAttributes.getDefaultInstance())
        .addProbabilities(1.0)
        .addProbabilities(0.0)
        .setSeed("test-seed")
        .build()

    val output = PersonLabelAttributes.newBuilder()
    PopulationNodeHelper.collapseQuantumLabel(quantumLabel, "suffix", output)
    assertTrue(output.isInitialized)
  }

  @Test
  fun `collapseQuantumLabel errors on empty labels`() {
    val quantumLabel = QuantumLabel.newBuilder().setSeed("test-seed").build()
    val output = PersonLabelAttributes.newBuilder()
    assertFailsWith<IllegalStateException> {
      PopulationNodeHelper.collapseQuantumLabel(quantumLabel, "suffix", output)
    }
  }

  @Test
  fun `collapseQuantumLabel errors on mismatched sizes`() {
    val quantumLabel =
      QuantumLabel.newBuilder()
        .addLabels(PersonLabelAttributes.getDefaultInstance())
        .addProbabilities(0.5)
        .addProbabilities(0.5)
        .setSeed("test-seed")
        .build()

    val output = PersonLabelAttributes.newBuilder()
    assertFailsWith<IllegalStateException> {
      PopulationNodeHelper.collapseQuantumLabel(quantumLabel, "suffix", output)
    }
  }
}
