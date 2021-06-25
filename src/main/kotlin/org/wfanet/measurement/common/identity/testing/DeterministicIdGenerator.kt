// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.identity.testing

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId

/** A [IdGenerator] that outputs new but deterministic ids for each call of generate methods */
class DeterministicIdGenerator(val idSeed: Long = 123456789, var iter: Int = 0) : IdGenerator {
  override fun generateInternalId() = InternalId(getNextSeed())
  override fun generateExternalId() = ExternalId(getNextSeed())

  private fun getNextSeed(): Long {
    this.iter += 1
    return idSeed + iter
  }
}

fun DeterministicIdGenerator.copy(): DeterministicIdGenerator {
  return DeterministicIdGenerator(idSeed, iter)
}
