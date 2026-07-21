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
import kotlin.math.ceil
import kotlin.math.sqrt

object Feistel {
  /**
   * Bijective permutation on [0, [domainSize]).
   *
   * Returns a unique output for each unique input — zero collisions by construction. Uses a 4-round
   * Feistel network with FarmHash64 as the round function and iterative cycle-walking for
   * non-power-of-2 domains.
   */
  fun permute(value: ULong, domainSize: ULong, seed: String): ULong {
    if (domainSize <= 1uL) return 0uL

    val half = ceil(sqrt(domainSize.toDouble())).toULong()
    var current = value

    do {
      var left = current / half
      var right = current % half

      repeat(4) { round ->
        val roundHash =
          Hashing.farmHashFingerprint64()
            .hashString("$seed-feistel-$round-$right", StandardCharsets.UTF_8)
            .asLong()
            .toULong()
        val newRight = (left + (roundHash % half)) % half
        left = right
        right = newRight
      }

      current = left * half + right
    } while (current >= domainSize)

    return current
  }
}
