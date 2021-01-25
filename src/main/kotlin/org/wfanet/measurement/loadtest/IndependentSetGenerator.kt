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

package org.wfanet.measurement.loadtest

import kotlin.random.Random

fun generateIndependentSets(
  universeSize: Long,
  setSize: Int,
  random: Random = Random
): Sequence<Set<Long>> = sequence {
  require(setSize > 0 && setSize <= universeSize) {
    "Each set size must be between 1 and universe size!"
  }
  while (true) {
    // Use Robert Floyd algorithm for sampling without replacement
    val independentSet = mutableSetOf<Long>()
    for (j in universeSize - setSize until universeSize) {
      val t = random.nextLong(j + 1)
      if (independentSet.contains(t)) {
        independentSet.add(j)
      } else {
        independentSet.add(t)
      }
    }
    yield(independentSet.toSet())
  }
}
