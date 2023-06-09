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

package org.wfanet.panelmatch.common.beam

import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList

/**
 * Finds items in a PCollection which do not exist in another PCollection. Keep this as a PTransform
 * for Scala compatibility.
 */
class Minus<T : Any> : PTransform<PCollectionList<T>, PCollection<T>>() {

  override fun expand(input: PCollectionList<T>): PCollection<T> {
    val items: List<PCollection<T>> = input.all
    require(items.size == 2) {
      "Found ${items.size} elements. Only PCollection with 2 elements is allowed."
    }
    val firstItems = items[0]
    val otherItems = items[1]
    return firstItems
      .map("MapLeft") { kvOf(it, 1) }
      .join(otherItems.map("MapRight") { kvOf(it, 2) }, "Join") {
        key: T,
        lefts: Iterable<Int>,
        rights: Iterable<Int> ->
        if (lefts.iterator().hasNext() && !rights.iterator().hasNext()) yield(key)
      }
  }
}
