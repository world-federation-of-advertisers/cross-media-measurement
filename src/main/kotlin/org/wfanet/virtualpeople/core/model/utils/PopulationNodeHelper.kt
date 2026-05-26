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

import com.google.common.hash.Hashing
import java.nio.charset.StandardCharsets
import org.wfanet.virtualpeople.common.PersonLabelAttributes
import org.wfanet.virtualpeople.common.QuantumLabel

/**
 * Shared utilities for PopulationNodeImpl and RankedPopulationNodeImpl.
 *
 * Keeps seed computation and quantum label collapsing in sync across both node types.
 */
object PopulationNodeHelper {

  /** Compute the VID selection seed from a random seed and acting fingerprint. */
  fun computeVidSeed(randomSeed: String, actingFingerprint: Long): ULong {
    return Hashing.farmHashFingerprint64()
      .hashString("$randomSeed${actingFingerprint.toULong()}", StandardCharsets.UTF_8)
      .asLong()
      .toULong()
  }

  /**
   * Collapse the [quantumLabel] to a single label based on the probabilities, and merge to
   * [outputLabel].
   */
  fun collapseQuantumLabel(
    quantumLabel: QuantumLabel,
    seedSuffix: String,
    outputLabel: PersonLabelAttributes.Builder
  ) {
    if (quantumLabel.labelsCount == 0) {
      error("Empty quantum label.")
    }
    if (quantumLabel.labelsCount != quantumLabel.probabilitiesCount) {
      error("The sizes of labels and probabilities are different in quantum label. $quantumLabel")
    }
    val distribution =
      quantumLabel.probabilitiesList.mapIndexed { index, it -> DistributionChoice(index, it) }
    val hashing = DistributedConsistentHashing(distribution)
    val index = hashing.hash("quantum-label-collapse-${quantumLabel.seed}$seedSuffix")
    outputLabel.mergeFrom(quantumLabel.getLabels(index))
  }
}
