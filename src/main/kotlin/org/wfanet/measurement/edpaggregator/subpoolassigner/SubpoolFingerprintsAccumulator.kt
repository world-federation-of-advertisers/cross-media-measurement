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

package org.wfanet.measurement.edpaggregator.subpoolassigner

import com.google.protobuf.ByteString
import com.google.protobuf.UnsafeByteOperations
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.Bytes12IntMap

/**
 * Thread-safe accumulator that buckets 12-byte fingerprints by subpool offset for the Phase-0
 * SubpoolAssigner.
 *
 * Layout is `subpool offset -> set of fingerprints`, where each per-subpool set is a
 * [Bytes12IntMap] used as a set: the 12-byte fingerprint `(keyHi, keyLo)` is the key and the value
 * is an ignored presence sentinel. Deduplication is therefore automatic, and the same fingerprint
 * routing to several subpools is stored once per subpool (the expected fan-out).
 *
 * ## Concurrency
 *
 * [add] is concurrency-safe: each subpool bucket carries its own monitor (the striping idea of
 * ResultsFulfiller's `StripedByteFrequencyVector`, but striped naturally by subpool), and distinct
 * subpools never contend. The drain API ([streamChunks] / [remove]) is **not** safe to call
 * concurrently with [add]; it is meant for the single-threaded flush phase after all adds complete.
 *
 * ## Draining
 *
 * Output is streamed: [streamChunks] yields a subpool's fingerprints as packed 12-byte chunks (≤
 * `chunkFingerprints` each, one in memory at a time), and [remove] drops a subpool's map once its
 * blob is durable. There is no whole-subpool materialization, so a single subpool is bounded only
 * by heap (not the ~2 GB `ByteArray` limit), and peak memory falls as maps are freed.
 */
class SubpoolFingerprintsAccumulator {
  private class Bucket {
    val map = Bytes12IntMap()
  }

  /**
   * Immutable, atomically-published pairing of subpool offsets to their [Bucket]s: `ids[i]` is the
   * subpool offset of `buckets[i]`. Subpools are few, so [add]'s hot path does a primitive linear
   * scan over the `ids` [LongArray] — no `Long` boxing, no ConcurrentHashMap bin lookup. A single
   * [Snapshot] reference is published (under [growthLock]) whenever a new subpool first appears, so
   * a concurrent reader always sees a consistent `(ids, buckets)` pair; a bucket, once created,
   * keeps its index and is never replaced.
   */
  private class Snapshot(val ids: LongArray, val buckets: Array<Bucket>)

  @Volatile private var snapshot = Snapshot(LongArray(0), emptyArray())
  private val growthLock = Any()

  /**
   * Records that the fingerprint `(keyHi, keyLo)` belongs to [subpoolId]. Idempotent per subpool.
   */
  fun add(subpoolId: Long, keyHi: Long, keyLo: Int) {
    val bucket = bucketFor(subpoolId)
    synchronized(bucket) { bucket.map.put(keyHi, keyLo, PRESENT) }
  }

  /** The [Bucket] for [subpoolId], creating it (under [growthLock]) the first time it is seen. */
  private fun bucketFor(subpoolId: Long): Bucket {
    // Lock-free fast path: subpools almost always already exist (few subpools, many adds).
    val current = snapshot
    val index = indexOf(current.ids, subpoolId)
    if (index >= 0) return current.buckets[index]
    // Slow path: create the bucket (or find it, if a concurrent add just created it), then publish
    // a
    // new snapshot with the appended (id, bucket) pair.
    synchronized(growthLock) {
      val latest = snapshot
      val existing = indexOf(latest.ids, subpoolId)
      if (existing >= 0) return latest.buckets[existing]
      val bucket = Bucket()
      snapshot = Snapshot(latest.ids + subpoolId, latest.buckets + bucket)
      return bucket
    }
  }

  private fun indexOf(ids: LongArray, subpoolId: Long): Int {
    for (i in ids.indices) {
      if (ids[i] == subpoolId) return i
    }
    return -1
  }

  /** The subpool offsets seen so far. */
  fun subpoolIds(): Set<Long> {
    val current = snapshot
    val ids = LinkedHashSet<Long>(current.ids.size)
    for (id in current.ids) {
      ids.add(id)
    }
    return ids
  }

  /** Number of distinct fingerprints accumulated for [subpoolId]. */
  fun size(subpoolId: Long): Long {
    val current = snapshot
    val index = indexOf(current.ids, subpoolId)
    if (index < 0) return 0L
    val bucket = current.buckets[index]
    return synchronized(bucket) { bucket.map.size }
  }

  /**
   * Streams [subpoolId]'s fingerprints as packed 12-byte chunks (8-byte high + 4-byte low big-
   * endian, matching [Bytes12IntMap] / `EventIdDigest`), each holding at most [chunkFingerprints]
   * fingerprints. Only one chunk is held in memory at a time. Order is unspecified (open-addressing
   * iteration order). Drain-phase only: call after all [add]s have completed.
   */
  fun streamChunks(
    subpoolId: Long,
    chunkFingerprints: Int = DEFAULT_CHUNK_FINGERPRINTS,
  ): Flow<ByteString> = flow {
    val current = snapshot
    val index = indexOf(current.ids, subpoolId)
    if (index < 0) return@flow
    val bucket = current.buckets[index]
    val total = bucket.map.size
    if (total == 0L) return@flow

    var produced = 0L
    var buffer = ByteArray((minOf(chunkFingerprints.toLong(), total) * FINGERPRINT_WIDTH).toInt())
    var pos = 0
    // forEach is inline, so emit() (suspend) is legal here within the flow block.
    bucket.map.forEach { keyHi, keyLo, _ ->
      writeBigEndianLong(buffer, pos, keyHi)
      pos += 8
      writeBigEndianInt(buffer, pos, keyLo)
      pos += 4
      produced++
      if (pos == buffer.size) {
        // unsafeWrap hands the buffer off without copying; a fresh one is sized for the remainder.
        emit(UnsafeByteOperations.unsafeWrap(buffer))
        val remaining = total - produced
        if (remaining > 0) {
          buffer =
            ByteArray((minOf(chunkFingerprints.toLong(), remaining) * FINGERPRINT_WIDTH).toInt())
          pos = 0
        }
      }
    }
  }

  /** Drops [subpoolId]'s map, freeing its heap once the subpool's blob has been written. */
  fun remove(subpoolId: Long) {
    synchronized(growthLock) {
      val latest = snapshot
      val index = indexOf(latest.ids, subpoolId)
      if (index < 0) return
      val keptIds = latest.ids.filterIndexed { i, _ -> i != index }.toLongArray()
      val keptBuckets = latest.buckets.filterIndexed { i, _ -> i != index }.toTypedArray()
      snapshot = Snapshot(keptIds, keptBuckets)
    }
  }

  companion object {
    private const val FINGERPRINT_WIDTH = 12L

    /** Ignored value stored for every fingerprint; the map is used purely as a set. */
    private const val PRESENT = 1

    /**
     * ~1M fingerprints (~12 MB) per chunk: one RecordIO record, one buffer in memory at a time.
     *
     * Phase-0 writes these per-(shard, subpool) records and the last-shard-out merge
     * ([org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore.mergeSubpool])
     * streams them straight through unchanged (a pure record-by-record concatenation, no
     * re-chunking), so the merged blob Phase-1 reads inherits this same ~1M-fingerprint record
     * size.
     */
    const val DEFAULT_CHUNK_FINGERPRINTS = 1 * 1024 * 1024

    private fun writeBigEndianLong(out: ByteArray, offset: Int, value: Long) {
      out[offset] = (value ushr 56).toByte()
      out[offset + 1] = (value ushr 48).toByte()
      out[offset + 2] = (value ushr 40).toByte()
      out[offset + 3] = (value ushr 32).toByte()
      out[offset + 4] = (value ushr 24).toByte()
      out[offset + 5] = (value ushr 16).toByte()
      out[offset + 6] = (value ushr 8).toByte()
      out[offset + 7] = value.toByte()
    }

    private fun writeBigEndianInt(out: ByteArray, offset: Int, value: Int) {
      out[offset] = (value ushr 24).toByte()
      out[offset + 1] = (value ushr 16).toByte()
      out[offset + 2] = (value ushr 8).toByte()
      out[offset + 3] = value.toByte()
    }
  }
}
