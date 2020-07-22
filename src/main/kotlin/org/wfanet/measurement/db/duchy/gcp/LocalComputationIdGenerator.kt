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

package org.wfanet.measurement.db.duchy.gcp

import java.time.Clock

/** Knows how to make local identifiers for computations to be used in a Spanner database. */
interface LocalComputationIdGenerator {
  /** Generates a local identifier for a computation.*/
  fun localId(globalId: Long): Long
}

/** Combines the upper 32 bits of the global id with the lower 32 bits of the current time.  */
class HalfOfGlobalBitsAndTimeStampIdGenerator(private val clock: Clock = Clock.systemUTC()) :
  LocalComputationIdGenerator {
  override fun localId(globalId: Long): Long {
    // TODO: Reverse the bits.
    return (globalId ushr 32) or (clock.millis() shl 32)
  }
}
