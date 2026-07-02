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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class SubpoolFingerprintsAccumulatorTest {
  /** Collects all of [subpoolId]'s chunks into a single byte array. */
  private fun drain(
    accumulator: SubpoolFingerprintsAccumulator,
    subpoolId: Long,
    chunkFingerprints: Int = SubpoolFingerprintsAccumulator.DEFAULT_CHUNK_FINGERPRINTS,
  ): ByteArray = runBlocking {
    val out = ByteString.newOutput()
    accumulator.streamChunks(subpoolId, chunkFingerprints).toList().forEach {
      out.write(it.toByteArray())
    }
    out.toByteString().toByteArray()
  }

  @Test
  fun `dedupes fingerprints within a subpool`() {
    val accumulator = SubpoolFingerprintsAccumulator()
    accumulator.add(5L, 0x1122334455667788L, 0x99AABBCC.toInt())
    accumulator.add(5L, 0x1122334455667788L, 0x99AABBCC.toInt())

    assertThat(accumulator.size(5L)).isEqualTo(1L)
  }

  @Test
  fun `keeps a fingerprint that fans out to multiple subpools once per subpool`() {
    val accumulator = SubpoolFingerprintsAccumulator()
    accumulator.add(1L, 7L, 7)
    accumulator.add(2L, 7L, 7)

    assertThat(accumulator.subpoolIds()).containsExactly(1L, 2L)
    assertThat(accumulator.size(1L)).isEqualTo(1L)
    assertThat(accumulator.size(2L)).isEqualTo(1L)
  }

  @Test
  fun `streamChunks packs fingerprints as 12 big-endian bytes`() {
    val accumulator = SubpoolFingerprintsAccumulator()
    val hi = 0x0102030405060708L
    val lo = 0x090A0B0C
    accumulator.add(42L, hi, lo)

    val bytes = drain(accumulator, 42L)

    assertThat(bytes.size).isEqualTo(12)
    val buffer = ByteBuffer.wrap(bytes)
    assertThat(buffer.long).isEqualTo(hi)
    assertThat(buffer.int).isEqualTo(lo)
  }

  @Test
  fun `streamChunks total length is 12 times the fingerprint count`() {
    val accumulator = SubpoolFingerprintsAccumulator()
    repeat(100) { i -> accumulator.add(9L, i.toLong(), i) }

    assertThat(drain(accumulator, 9L).size).isEqualTo(12 * 100)
  }

  @Test
  fun `streamChunks splits into records of at most chunkFingerprints`() {
    val accumulator = SubpoolFingerprintsAccumulator()
    repeat(3) { i -> accumulator.add(1L, i.toLong(), i) }

    val chunks = runBlocking { accumulator.streamChunks(1L, chunkFingerprints = 2).toList() }

    // 3 fingerprints, chunk=2 -> records of 2 then 1 fingerprint (24 then 12 bytes).
    assertThat(chunks.map { it.size() }).containsExactly(24, 12).inOrder()
  }

  @Test
  fun `handles the all-zero fingerprint`() {
    val accumulator = SubpoolFingerprintsAccumulator()
    accumulator.add(3L, 0L, 0)

    assertThat(accumulator.size(3L)).isEqualTo(1L)
    assertThat(drain(accumulator, 3L).size).isEqualTo(12)
  }

  @Test
  fun `remove drops a subpool`() {
    val accumulator = SubpoolFingerprintsAccumulator()
    accumulator.add(5L, 1L, 1)

    accumulator.remove(5L)

    assertThat(accumulator.subpoolIds()).isEmpty()
    assertThat(drain(accumulator, 5L)).isEmpty()
  }

  @Test
  fun `concurrent adds across and within subpools are correct`() {
    val accumulator = SubpoolFingerprintsAccumulator()
    val threads = 8
    val perThread = 5_000
    val executor = Executors.newFixedThreadPool(threads)
    val startLatch = CountDownLatch(1)
    val doneLatch = CountDownLatch(threads)

    repeat(threads) {
      executor.submit {
        startLatch.await()
        for (i in 0 until perThread) {
          accumulator.add((i % 4).toLong(), i.toLong(), i)
        }
        doneLatch.countDown()
      }
    }
    startLatch.countDown()
    doneLatch.await()
    executor.shutdown()

    var total = 0L
    for (subpool in 0L until 4L) {
      total += accumulator.size(subpool)
    }
    assertThat(total).isEqualTo(perThread.toLong())
  }
}
