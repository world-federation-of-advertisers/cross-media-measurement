/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.ratelimit

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TestTimeSource
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class TokenBucketTest {
  private val testTimeSource = TestTimeSource()

  @Test
  fun `tryAcquire returns true up to size`() {
    val size = 5
    val tokenBucket = TokenBucket(size, 1.0, testTimeSource)

    repeat(size) { assertThat(tokenBucket.tryAcquire()).isTrue() }
  }

  @Test
  fun `tryAcquire returns true up to size when cost is specified`() {
    val size = 5
    val tokenBucket = TokenBucket(size, 1.0, testTimeSource)

    assertThat(tokenBucket.tryAcquire(size)).isTrue()
  }

  @Test
  fun `tryAcquire returns false after tokens exhausted`() {
    val size = 5
    val tokenBucket = TokenBucket(size, 1.0, testTimeSource)
    repeat(size) { tokenBucket.tryAcquire() }

    assertThat(tokenBucket.tryAcquire()).isFalse()
  }

  @Test
  fun `tryAcquire returns false when cost exceeds token count`() {
    val size = 5
    val tokenBucket = TokenBucket(size, 1.0, testTimeSource)
    tokenBucket.tryAcquire()

    assertThat(tokenBucket.tryAcquire(size)).isFalse()
  }

  @Test
  fun `tryAcquire returns true after token refill`() {
    val size = 5
    val tokenBucket = TokenBucket(size, 1.0, testTimeSource)
    tokenBucket.tryAcquire(5)
    testTimeSource += 1.seconds // Refill one token.

    assertThat(tokenBucket.tryAcquire()).isTrue()
  }

  @Test
  fun `tryAcquire returns false after refilled tokens exhausted`() {
    val size = 5
    val tokenBucket = TokenBucket(size, 1.0, testTimeSource)
    tokenBucket.tryAcquire(size)
    testTimeSource += 1.seconds
    tokenBucket.tryAcquire()

    assertThat(tokenBucket.tryAcquire()).isFalse()
  }

  @Test
  fun `tryAcquire returns false after max refilled tokens exhausted`() {
    val size = 5
    val tokenBucket = TokenBucket(size, 1.0, testTimeSource)
    tokenBucket.tryAcquire(size)
    testTimeSource += 10.seconds // Refill tokens up to size.
    repeat(size) {
      assertThat(tokenBucket.tryAcquire()).isTrue() // Consume all refilled tokens.
    }

    assertThat(tokenBucket.tryAcquire()).isFalse()
  }

  @Test
  fun `tryAcquire throws when cost is negative`() {
    val tokenBucket = TokenBucket(5, 1.0, testTimeSource)

    assertFailsWith<IllegalArgumentException> { tokenBucket.tryAcquire(-1) }
  }

  @Test
  fun `constructor throws when size is negative `() {
    assertFailsWith<IllegalArgumentException> { TokenBucket(-1, 1.0, testTimeSource) }
  }

  @Test
  fun `constructor throws when fill rate is not positive`() {
    assertFailsWith<IllegalArgumentException> { TokenBucket(5, 0.0, testTimeSource) }
    assertFailsWith<IllegalArgumentException> { TokenBucket(5, -1.0, testTimeSource) }
  }
}
