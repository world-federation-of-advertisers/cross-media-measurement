// Copyright 2022 The Cross-Media Measurement Authors
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

private const val SEED: ULong = 2862933555777941757UL
private const val NUMERATOR: Double = (1L shl 31).toDouble()

/**
 * Applies consistent hashing, to map the input key to one of the buckets. Each bucket is
 * represented by an index with range [0, num_buckets - 1]. The output is the index of the selected
 * bucket.
 *
 * The consistent hashing algorithm is from the published paper: https://arxiv.org/pdf/1406.2294.pdf
 */
fun jumpConsistentHash(key: ULong, numBuckets: Int): Int {
  var b: Int = -1
  var j: Long = 0
  var nextKey: ULong = key
  /** Use Long for j to handle Int overflow. */
  while (j < numBuckets) {
    b = j.toInt()
    nextKey = nextKey * SEED + 1UL
    j = ((j + 1) * (NUMERATOR / ((nextKey shr 33) + 1UL).toDouble())).toLong()
  }
  return b
}
