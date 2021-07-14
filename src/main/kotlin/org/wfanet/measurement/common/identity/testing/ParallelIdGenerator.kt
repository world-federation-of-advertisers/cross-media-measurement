// Copyright 2020 The Cross-Media Measurement Authors
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

/**
 * An [IdGenerator] that saves the generated ids from the [inputIdGenerator] and exposes them in the
 * same order they are genereated. Used for testing.
 */
class ParallelIdGenerator(val inputIdGenerator: IdGenerator) : IdGenerator {
  private val internalIds = mutableListOf<InternalId>()
  private val externalIds = mutableListOf<ExternalId>()
  private var internalIdIndex = 0
  private var externalIdIndex = 0

  override fun generateInternalId(): InternalId {
    val internalId = inputIdGenerator.generateInternalId()
    internalIds.add(internalId)
    return internalId
  }

  override fun generateExternalId(): ExternalId {
    val externalId = inputIdGenerator.generateExternalId()
    externalIds.add(externalId)
    return externalId
  }

  fun getNextGeneratedInternalId(): InternalId {
    val internalId = internalIds.get(internalIdIndex)
    this.internalIdIndex++
    return internalId
  }

  fun getNextGeneratedExternalId(): ExternalId {
    val externalId = externalIds.get(externalIdIndex)
    this.externalIdIndex++
    return externalId
  }
}
