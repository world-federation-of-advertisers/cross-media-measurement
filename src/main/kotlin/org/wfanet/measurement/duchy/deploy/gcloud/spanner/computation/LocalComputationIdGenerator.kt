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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import java.lang.Long.reverse
import java.time.Clock

/** Knows how to make local identifiers for computations to be used in a Spanner database. */
interface LocalComputationIdGenerator {
  /** Generates a local identifier for a computation.*/
  fun localId(globalId: String): Long
}

/** An IdGenerator using the hashCode of the globalId and the timestamp. */
class GlobalBitsPlusTimeStampIdGenerator(private val clock: Clock = Clock.systemUTC()) :
  LocalComputationIdGenerator {
  override fun localId(globalId: String): Long {
    return reverse(clock.millis()) or globalId.hashCode().toLong()
  }
}
