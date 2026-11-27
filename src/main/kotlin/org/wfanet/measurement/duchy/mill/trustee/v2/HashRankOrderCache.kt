/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.duchy.mill.trustee.v2

import com.google.protobuf.ByteString
import java.security.MessageDigest
import org.wfanet.measurement.internal.duchy.PopulationSpec

/**
 * A least-recently-used cache of the [HashRankOrder] of each [PopulationSpec].
 *
 * Each order holds an `Int` per VID of its [PopulationSpec], so [maxSize] bounds the memory it
 * keeps between computations. An order is built at most once while cached, outside the cache's
 * lock, so that a build does not block lookups of other orders.
 *
 * @param maxSize the most orders kept
 */
class HashRankOrderCache(private val maxSize: Int) {
  init {
    require(maxSize >= 1) { "maxSize must be at least 1, got $maxSize" }
  }

  private val orders =
    object : LinkedHashMap<ByteString, Lazy<HashRankOrder>>(maxSize + 1, 1f, true) {
      override fun removeEldestEntry(
        eldest: MutableMap.MutableEntry<ByteString, Lazy<HashRankOrder>>
      ): Boolean = size > maxSize
    }

  /** Returns the [HashRankOrder] of [populationSpec], building it if it is not cached. */
  fun get(populationSpec: PopulationSpec): HashRankOrder {
    val key: ByteString = fingerprint(populationSpec)
    val order: Lazy<HashRankOrder> =
      synchronized(orders) { orders.getOrPut(key) { lazy { HashRankOrder.build(populationSpec) } } }
    return order.value
  }

  companion object {
    private const val FINGERPRINT_ALGORITHM = "SHA-256"

    private fun fingerprint(populationSpec: PopulationSpec): ByteString =
      ByteString.copyFrom(
        MessageDigest.getInstance(FINGERPRINT_ALGORITHM).digest(populationSpec.toByteArray())
      )
  }
}
