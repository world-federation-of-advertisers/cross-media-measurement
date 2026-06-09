/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.vidlabeler.utils

import com.google.common.truth.Truth.assertThat
import kotlin.random.Random
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class Bytes12IntMapTest {

  // -------- Constructor validation --------

  @Test
  fun `constructor rejects non-positive initialCapacity`() {
    assertThrows(IllegalArgumentException::class.java) { Bytes12IntMap(initialCapacity = 0L) }
    assertThrows(IllegalArgumentException::class.java) { Bytes12IntMap(initialCapacity = -1L) }
  }

  @Test
  fun `constructor rejects loadFactor out of range`() {
    assertThrows(IllegalArgumentException::class.java) { Bytes12IntMap(loadFactor = 0f) }
    assertThrows(IllegalArgumentException::class.java) { Bytes12IntMap(loadFactor = 1f) }
    assertThrows(IllegalArgumentException::class.java) { Bytes12IntMap(loadFactor = -0.5f) }
    assertThrows(IllegalArgumentException::class.java) { Bytes12IntMap(loadFactor = 1.5f) }
  }

  @Test
  fun `constructor rejects chunkShift out of range`() {
    assertThrows(IllegalArgumentException::class.java) { Bytes12IntMap(maxChunkShift = 0) }
    assertThrows(IllegalArgumentException::class.java) { Bytes12IntMap(maxChunkShift = -1) }
    assertThrows(IllegalArgumentException::class.java) { Bytes12IntMap(maxChunkShift = 31) }
  }

  @Test
  fun `constructor floors capacity at MIN_CAPACITY`() {
    val table = Bytes12IntMap(initialCapacity = 1L)
    assertThat(table.capacity()).isAtLeast(Bytes12IntMap.MIN_CAPACITY)
  }

  @Test
  fun `constructor rounds capacity up to next power of two`() {
    assertThat(Bytes12IntMap(initialCapacity = 17L).capacity()).isEqualTo(32L)
    assertThat(Bytes12IntMap(initialCapacity = 17L, maxChunkShift = 4).capacity()).isEqualTo(32L)
    assertThat(Bytes12IntMap(initialCapacity = 100L, maxChunkShift = 4).capacity()).isEqualTo(128L)
    assertThat(Bytes12IntMap(initialCapacity = 16L, maxChunkShift = 4).capacity()).isEqualTo(16L)
    assertThat(Bytes12IntMap(initialCapacity = 1024L, maxChunkShift = 4).numChunks())
      .isEqualTo(1024 / 16)
  }

  @Test
  fun `numChunks scales with capacity`() {
    val table = Bytes12IntMap(initialCapacity = 256L, maxChunkShift = 4)
    assertThat(table.capacity()).isEqualTo(256L)
    assertThat(table.numChunks()).isEqualTo(256 / 16)
  }

  // -------- Basic put / get / containsKey --------

  @Test
  fun `put returns NOT_PRESENT for a new key`() {
    val table = Bytes12IntMap()
    assertThat(table.put(1L, 2, 100)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
    assertThat(table.size).isEqualTo(1L)
  }

  @Test
  fun `put returns the previous value when updating`() {
    val table = Bytes12IntMap()
    table.put(1L, 2, 100)
    assertThat(table.put(1L, 2, 200)).isEqualTo(100)
    assertThat(table.get(1L, 2)).isEqualTo(200)
    assertThat(table.size).isEqualTo(1L)
  }

  @Test
  fun `get returns NOT_PRESENT for a missing key`() {
    val table = Bytes12IntMap()
    assertThat(table.get(1L, 2)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
  }

  @Test
  fun `containsKey distinguishes present from missing`() {
    val table = Bytes12IntMap()
    assertThat(table.containsKey(1L, 2)).isFalse()
    table.put(1L, 2, 100)
    assertThat(table.containsKey(1L, 2)).isTrue()
    assertThat(table.containsKey(2L, 1)).isFalse()
  }

  @Test
  fun `value zero is distinguished from absence`() {
    val table = Bytes12IntMap()
    table.put(1L, 2, 0)
    assertThat(table.containsKey(1L, 2)).isTrue()
    assertThat(table.get(1L, 2)).isEqualTo(0)
    assertThat(table.get(2L, 1)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
  }

  @Test
  fun `containsKey returns true even when stored value equals NOT_PRESENT`() {
    // Regression guard: containsKey MUST probe the key arrays directly and
    // not depend on the stored value. If a future change reverts the impl
    // to `get(...) != NOT_PRESENT`, this test fails — because get() would
    // return NOT_PRESENT both for absent keys AND for keys present with
    // value -1, making containsKey unable to distinguish them.
    val table = Bytes12IntMap()
    val sentinel = Bytes12IntMap.NOT_PRESENT // -1

    // Non-zero key with value == NOT_PRESENT: must report present.
    table.put(1L, 2, sentinel)
    assertThat(table.containsKey(1L, 2)).isTrue()
    assertThat(table.get(1L, 2)).isEqualTo(sentinel)

    // Zero key with value == NOT_PRESENT: exercises the out-of-band
    // zero-key fast path on the containsKey side too.
    table.put(0L, 0, sentinel)
    assertThat(table.containsKey(0L, 0)).isTrue()
    assertThat(table.get(0L, 0)).isEqualTo(sentinel)

    // A truly absent key must still report missing.
    assertThat(table.containsKey(99L, 99)).isFalse()

    // After remove, the key must report missing again.
    table.remove(1L, 2)
    assertThat(table.containsKey(1L, 2)).isFalse()
  }

  // -------- remove --------

  @Test
  fun `remove returns NOT_PRESENT for a missing key`() {
    val table = Bytes12IntMap()
    assertThat(table.remove(1L, 2)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
  }

  @Test
  fun `remove returns the previous value and clears the entry`() {
    val table = Bytes12IntMap()
    table.put(1L, 2, 100)
    assertThat(table.remove(1L, 2)).isEqualTo(100)
    assertThat(table.containsKey(1L, 2)).isFalse()
    assertThat(table.size).isEqualTo(0L)
  }

  @Test
  fun `repeated remove is idempotent`() {
    val table = Bytes12IntMap()
    table.put(1L, 2, 100)
    assertThat(table.remove(1L, 2)).isEqualTo(100)
    assertThat(table.remove(1L, 2)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
    assertThat(table.remove(1L, 2)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
    assertThat(table.size).isEqualTo(0L)
  }

  // -------- Zero key handling --------

  @Test
  fun `zero key can be stored and retrieved`() {
    val table = Bytes12IntMap()
    assertThat(table.put(0L, 0, 42)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
    assertThat(table.containsKey(0L, 0)).isTrue()
    assertThat(table.get(0L, 0)).isEqualTo(42)
    assertThat(table.size).isEqualTo(1L)
  }

  @Test
  fun `zero key update returns previous value`() {
    val table = Bytes12IntMap()
    table.put(0L, 0, 42)
    assertThat(table.put(0L, 0, 99)).isEqualTo(42)
    assertThat(table.get(0L, 0)).isEqualTo(99)
    assertThat(table.size).isEqualTo(1L)
  }

  @Test
  fun `zero key remove returns previous value`() {
    val table = Bytes12IntMap()
    table.put(0L, 0, 42)
    assertThat(table.remove(0L, 0)).isEqualTo(42)
    assertThat(table.containsKey(0L, 0)).isFalse()
    assertThat(table.remove(0L, 0)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
    assertThat(table.size).isEqualTo(0L)
  }

  @Test
  fun `zero key coexists with non-zero keys`() {
    val table = Bytes12IntMap()
    table.put(0L, 0, 1)
    table.put(1L, 2, 2)
    table.put(3L, 4, 3)
    assertThat(table.size).isEqualTo(3L)
    assertThat(table.get(0L, 0)).isEqualTo(1)
    assertThat(table.get(1L, 2)).isEqualTo(2)
    assertThat(table.get(3L, 4)).isEqualTo(3)
  }

  @Test
  fun `keys that differ only in zero-ness are distinct`() {
    val table = Bytes12IntMap()
    table.put(0L, 0, 1)
    table.put(0L, 1, 2)
    table.put(1L, 0, 3)
    assertThat(table.size).isEqualTo(3L)
    assertThat(table.get(0L, 0)).isEqualTo(1)
    assertThat(table.get(0L, 1)).isEqualTo(2)
    assertThat(table.get(1L, 0)).isEqualTo(3)
  }

  // -------- clear --------

  @Test
  fun `clear empties the table including the zero key`() {
    val table = Bytes12IntMap()
    table.put(1L, 2, 100)
    table.put(3L, 4, 200)
    table.put(0L, 0, 42)
    table.clear()
    assertThat(table.size).isEqualTo(0L)
    assertThat(table.isEmpty()).isTrue()
    assertThat(table.containsKey(1L, 2)).isFalse()
    assertThat(table.containsKey(0L, 0)).isFalse()
    table.put(5L, 6, 300)
    assertThat(table.get(5L, 6)).isEqualTo(300)
  }

  @Test
  fun `clear on empty table is a no-op`() {
    val table = Bytes12IntMap()
    table.clear()
    assertThat(table.size).isEqualTo(0L)
    assertThat(table.isEmpty()).isTrue()
  }

  // -------- forEach --------

  @Test
  fun `forEach visits every entry exactly once`() {
    val table = Bytes12IntMap()
    val expected = mapOf((1L to 2) to 10, (3L to 4) to 20, (5L to 6) to 30, (0L to 0) to 40)
    for ((k, v) in expected) {
      table.put(k.first, k.second, v)
    }
    val seen = mutableMapOf<Pair<Long, Int>, Int>()
    table.forEach { keyHi, keyLo, value -> seen[keyHi to keyLo] = value }
    assertThat(seen).isEqualTo(expected)
  }

  @Test
  fun `forEach on empty table does not invoke action`() {
    val table = Bytes12IntMap()
    var calls = 0
    table.forEach { _, _, _ -> calls++ }
    assertThat(calls).isEqualTo(0)
  }

  // -------- Resize behavior --------

  @Test
  fun `entries survive resize`() {
    val table = Bytes12IntMap(initialCapacity = 32L, loadFactor = 0.5f, maxChunkShift = 4)
    val initialCap = table.capacity()
    val n = 5_000
    for (i in 0 until n) {
      table.put(i.toLong() * 17L, i * 31, i)
    }
    assertThat(table.size).isEqualTo(n.toLong())
    assertThat(table.capacity()).isGreaterThan(initialCap)
    for (i in 0 until n) {
      assertThat(table.get(i.toLong() * 17L, i * 31)).isEqualTo(i)
    }
  }

  @Test
  fun `single-chunk table survives transition to multi-chunk on resize`() {
    // Start with exactly one chunk (initialCapacity == chunk size cap).
    // Inserting past the load threshold must force numChunks to grow
    // from 1 to >1, which activates the chunk-index decomposition in
    // the probing hot path for re-inserted entries for the first time.
    val table = Bytes12IntMap(initialCapacity = 16L, loadFactor = 0.5f, maxChunkShift = 4)
    assertThat(table.numChunks()).isEqualTo(1)
    val initialCap = table.capacity()

    // Threshold at load factor 0.5 over capacity 16 is 8; insert past it.
    val n = 20
    for (i in 0 until n) {
      table.put(i.toLong() * 17L + 1L, i * 31 + 1, i)
    }

    assertThat(table.size).isEqualTo(n.toLong())
    assertThat(table.capacity()).isGreaterThan(initialCap)
    assertThat(table.numChunks()).isGreaterThan(1)
    for (i in 0 until n) {
      assertThat(table.get(i.toLong() * 17L + 1L, i * 31 + 1)).isEqualTo(i)
    }
  }

  // -------- Multi-chunk + cross-chunk-boundary --------

  @Test
  fun `multi-chunk layout supports many entries across chunks`() {
    val table = Bytes12IntMap(initialCapacity = 64L, loadFactor = 0.95f, maxChunkShift = 4)
    assertThat(table.numChunks()).isEqualTo(4)
    val initialCap = table.capacity()
    val n = 50
    for (i in 0 until n) {
      table.put(i.toLong() * 1_000_003L, i * 7919, i + 1)
    }
    assertThat(table.size).isEqualTo(n.toLong())
    assertThat(table.capacity()).isEqualTo(initialCap)
    for (i in 0 until n) {
      assertThat(table.get(i.toLong() * 1_000_003L, i * 7919)).isEqualTo(i + 1)
    }
  }

  @Test
  fun `cross-chunk probe wraps correctly on insert and lookup`() {
    val table = Bytes12IntMap(initialCapacity = 64L, loadFactor = 0.95f, maxChunkShift = 4)
    val mask = table.capacity() - 1L
    val target = 15L
    val keys = generateKeysHittingIndex(target = target, count = 6, mask = mask)
    for ((i, k) in keys.withIndex()) {
      assertThat(table.put(k.first, k.second, i + 1)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
    }
    for ((i, k) in keys.withIndex()) {
      assertThat(table.get(k.first, k.second)).isEqualTo(i + 1)
    }
  }

  @Test
  fun `cross-chunk shiftKeys preserves lookups after removal`() {
    val table = Bytes12IntMap(initialCapacity = 64L, loadFactor = 0.95f, maxChunkShift = 4)
    val mask = table.capacity() - 1L
    val keys = generateKeysHittingIndex(target = 15L, count = 6, mask = mask)
    for ((i, k) in keys.withIndex()) {
      table.put(k.first, k.second, i + 1)
    }
    val (rmHi, rmLo) = keys[2]
    assertThat(table.remove(rmHi, rmLo)).isEqualTo(3)
    for ((i, k) in keys.withIndex()) {
      if (i == 2) {
        assertThat(table.get(k.first, k.second)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
      } else {
        assertThat(table.get(k.first, k.second)).isEqualTo(i + 1)
      }
    }
  }

  @Test
  fun `cross-chunk wrap from last slot to first slot works`() {
    val table = Bytes12IntMap(initialCapacity = 64L, loadFactor = 0.95f, maxChunkShift = 4)
    val mask = table.capacity() - 1L
    val lastIndex = (table.capacity() - 1L)
    val keys = generateKeysHittingIndex(target = lastIndex, count = 5, mask = mask)
    for ((i, k) in keys.withIndex()) {
      table.put(k.first, k.second, i + 1)
    }
    for ((i, k) in keys.withIndex()) {
      assertThat(table.get(k.first, k.second)).isEqualTo(i + 1)
    }
    val (rmHi, rmLo) = keys[1]
    assertThat(table.remove(rmHi, rmLo)).isEqualTo(2)
    for ((i, k) in keys.withIndex()) {
      if (i == 1) {
        assertThat(table.get(k.first, k.second)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
      } else {
        assertThat(table.get(k.first, k.second)).isEqualTo(i + 1)
      }
    }
  }

  // -------- Backward-shift deletion --------

  @Test
  fun `remove preserves lookups for keys with overlapping probe chains (single chunk)`() {
    val table = Bytes12IntMap(initialCapacity = 16L, loadFactor = 0.95f, maxChunkShift = 4)
    val mask = table.capacity() - 1L
    val collidingKeys = generateKeysHittingIndex(target = 7L, count = 8, mask = mask)
    for ((i, k) in collidingKeys.withIndex()) {
      table.put(k.first, k.second, i + 1)
    }
    for ((i, k) in collidingKeys.withIndex()) {
      assertThat(table.get(k.first, k.second)).isEqualTo(i + 1)
    }
    val (rmHi, rmLo) = collidingKeys[3]
    assertThat(table.remove(rmHi, rmLo)).isEqualTo(4)
    for ((i, k) in collidingKeys.withIndex()) {
      if (i == 3) {
        assertThat(table.get(k.first, k.second)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
      } else {
        assertThat(table.get(k.first, k.second)).isEqualTo(i + 1)
      }
    }
  }

  @Test
  fun `delete-then-reinsert preserves correctness for many entries`() {
    val table = Bytes12IntMap(initialCapacity = 64L, maxChunkShift = 4)
    val n = 5_000
    for (i in 0 until n) {
      table.put(i.toLong() * 7L, i, i * 10)
    }
    for (i in 0 until n step 2) {
      assertThat(table.remove(i.toLong() * 7L, i)).isEqualTo(i * 10)
    }
    assertThat(table.size).isEqualTo((n / 2).toLong())
    for (i in 0 until n step 2) {
      assertThat(table.put(i.toLong() * 7L, i, i * 100)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
    }
    for (i in 0 until n) {
      val expected = if (i % 2 == 0) i * 100 else i * 10
      assertThat(table.get(i.toLong() * 7L, i)).isEqualTo(expected)
    }
  }

  @Test
  fun `delete all entries then table is empty and reusable`() {
    val table = Bytes12IntMap(initialCapacity = 64L, maxChunkShift = 4)
    val n = 1_000
    for (i in 0 until n) {
      table.put(i.toLong(), i, i)
    }
    for (i in 0 until n) {
      table.remove(i.toLong(), i)
    }
    assertThat(table.size).isEqualTo(0L)
    for (i in 0 until n) {
      assertThat(table.containsKey(i.toLong(), i)).isFalse()
    }
    table.put(42L, 7, 99)
    assertThat(table.get(42L, 7)).isEqualTo(99)
  }

  // -------- Stress / randomized --------

  @Test
  fun `randomized operations match a reference HashMap (single-chunk default)`() {
    val table = Bytes12IntMap(initialCapacity = 32L)
    randomizedReferenceCheck(table)
  }

  @Test
  fun `randomized operations match a reference HashMap (multi-chunk small)`() {
    val table = Bytes12IntMap(initialCapacity = 64L, maxChunkShift = 4)
    randomizedReferenceCheck(table)
  }

  private fun randomizedReferenceCheck(table: Bytes12IntMap) {
    val ref = HashMap<Pair<Long, Int>, Int>()
    val rng = Random(0xBEEFCAFEL)
    val iterations = 50_000
    repeat(iterations) {
      val op = rng.nextInt(3)
      val keyHi = rng.nextLong()
      val keyLo = rng.nextInt()
      val key = keyHi to keyLo
      when (op) {
        0 -> {
          val v = rng.nextInt(Int.MAX_VALUE)
          val prevTable = table.put(keyHi, keyLo, v)
          val prevRef = ref.put(key, v)
          assertThat(prevTable).isEqualTo(prevRef ?: Bytes12IntMap.NOT_PRESENT)
        }
        1 -> {
          assertThat(table.get(keyHi, keyLo)).isEqualTo(ref[key] ?: Bytes12IntMap.NOT_PRESENT)
        }
        2 -> {
          val prevTable = table.remove(keyHi, keyLo)
          val prevRef = ref.remove(key)
          assertThat(prevTable).isEqualTo(prevRef ?: Bytes12IntMap.NOT_PRESENT)
        }
      }
      assertThat(table.size).isEqualTo(ref.size.toLong())
    }
    for ((k, v) in ref) {
      assertThat(table.get(k.first, k.second)).isEqualTo(v)
    }
  }

  // -------- ByteArray API --------

  @Test
  fun `ByteArray API round-trips`() {
    val table = Bytes12IntMap()
    val fp = ByteArray(12) { it.toByte() }
    assertThat(table.put(fp, 42)).isEqualTo(Bytes12IntMap.NOT_PRESENT)
    assertThat(table.get(fp)).isEqualTo(42)
    assertThat(table.containsKey(fp)).isTrue()
    assertThat(table.remove(fp)).isEqualTo(42)
    assertThat(table.containsKey(fp)).isFalse()
  }

  @Test
  fun `ByteArray API rejects wrong size`() {
    val table = Bytes12IntMap()
    val tooShort = ByteArray(8)
    val tooLong = ByteArray(16)
    val empty = ByteArray(0)
    assertThrows(IllegalArgumentException::class.java) { table.put(tooShort, 1) }
    assertThrows(IllegalArgumentException::class.java) { table.put(tooLong, 1) }
    assertThrows(IllegalArgumentException::class.java) { table.put(empty, 1) }
    assertThrows(IllegalArgumentException::class.java) { table.get(tooShort) }
    assertThrows(IllegalArgumentException::class.java) { table.get(tooLong) }
    assertThrows(IllegalArgumentException::class.java) { table.remove(tooShort) }
    assertThrows(IllegalArgumentException::class.java) { table.containsKey(tooLong) }
  }

  @Test
  fun `ByteArray and primitive APIs agree`() {
    val table = Bytes12IntMap()
    val fp =
      byteArrayOf(
        0x12,
        0x34,
        0x56,
        0x78,
        0x9A.toByte(),
        0xBC.toByte(),
        0xDE.toByte(),
        0xF0.toByte(),
        0xCA.toByte(),
        0xFE.toByte(),
        0xBA.toByte(),
        0xBE.toByte(),
      )
    val hi = 0x123456789ABCDEF0L
    val lo = 0xCAFEBABE.toInt()
    table.put(fp, 99)
    assertThat(table.get(hi, lo)).isEqualTo(99)
    table.remove(hi, lo)
    assertThat(table.containsKey(fp)).isFalse()
  }

  // -------- Helpers --------

  /**
   * Generates [count] distinct `(hi, lo)` pairs whose `indexFor` reduction (`(keyHi xor (keyLo
   * shl 32) xor keyLo) and mask`) equals [target]. Used to force collisions on a specific bucket
   * index for probing and backward-shift deletion tests.
   */
  private fun generateKeysHittingIndex(
    target: Long,
    count: Int,
    mask: Long,
  ): List<Pair<Long, Int>> {
    val results = mutableListOf<Pair<Long, Int>>()
    val seen = HashSet<Pair<Long, Int>>()
    var hi = 1L
    while (results.size < count && hi < 100_000_000L) {
      val mixed = hi and mask
      if (mixed == target) {
        val k = hi to 0
        if ((hi != 0L) && seen.add(k)) results.add(k)
      }
      hi++
    }
    require(results.size == count) {
      "Could not generate $count keys colliding at index $target with mask $mask"
    }
    return results
  }
}
