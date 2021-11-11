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

package org.wfanet.measurement.loadtest.dataprovider

import kotlin.random.Random

data class SketchGenerationParams(
  val reach: Int,
  val universeSize: Int,
)

/** Fulfill the query with randomly generated ids. */
class RandomEventQuery(private val sketchGenerationParams: SketchGenerationParams) : EventQuery() {

  /** Generate Ids using random values. The parameter is ignored. */
  override fun getUserVirtualIds(parameter: QueryParameter): Sequence<Long> {
    return sequence {
      for (i in 1..sketchGenerationParams.reach) {
        yield(Random.nextInt(1, sketchGenerationParams.universeSize + 1).toLong())
      }
    }
  }
}
