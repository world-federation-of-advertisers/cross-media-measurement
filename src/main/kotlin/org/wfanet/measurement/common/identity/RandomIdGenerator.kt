// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.common.identity

import java.time.Clock
import kotlin.math.abs
import kotlin.random.Random

class RandomIdGenerator(
  private val clock: Clock = Clock.systemUTC(),
  private val random: Random = Random.Default
) : IdGenerator {
  override fun generateInternalId(): InternalId = InternalId(generateLong())

  override fun generateExternalId(): ExternalId = ExternalId(generateLong())

  private fun generateLong(): Long =
    abs((random.nextLong() shl 32) or (clock.millis() and 0xFFFFFFFF))
}
