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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.io.EOFException
import java.io.IOException
import java.net.SocketException
import java.security.GeneralSecurityException
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExponentialBackoff

@RunWith(JUnit4::class)
class RetriesTest {
  private val fastBackoff =
    ExponentialBackoff(
      initialDelay = Duration.ofMillis(1),
      multiplier = 1.0,
      randomnessFactor = 0.0,
    )

  @Test
  fun `retryTransient returns result without retrying on success`(): Unit = runBlocking {
    var attempts = 0
    val result =
      retryTransient(maxAttempts = 3, backoff = fastBackoff, isTransient = { true }) {
        attempts++
        "ok"
      }
    assertThat(result).isEqualTo("ok")
    assertThat(attempts).isEqualTo(1)
  }

  @Test
  fun `retryTransient retries transient failures then succeeds`(): Unit = runBlocking {
    var attempts = 0
    val result =
      retryTransient(maxAttempts = 5, backoff = fastBackoff, isTransient = { true }) {
        attempts++
        if (attempts < 3) throw IOException("boom")
        "ok"
      }
    assertThat(result).isEqualTo("ok")
    assertThat(attempts).isEqualTo(3)
  }

  @Test
  fun `retryTransient rethrows the last failure after exhausting attempts`(): Unit = runBlocking {
    var attempts = 0
    val exception =
      assertFailsWith<IOException> {
        retryTransient(maxAttempts = 3, backoff = fastBackoff, isTransient = { true }) {
          attempts++
          throw IOException("boom $attempts")
        }
      }
    assertThat(attempts).isEqualTo(3)
    assertThat(exception).hasMessageThat().contains("boom 3")
  }

  @Test
  fun `retryTransient does not retry non-transient failures`(): Unit = runBlocking {
    var attempts = 0
    assertFailsWith<IllegalStateException> {
      retryTransient(maxAttempts = 3, backoff = fastBackoff, isTransient = { false }) {
        attempts++
        throw IllegalStateException("nope")
      }
    }
    assertThat(attempts).isEqualTo(1)
  }

  @Test
  fun `retryTransient never retries cancellation`(): Unit = runBlocking {
    var attempts = 0
    assertFailsWith<CancellationException> {
      retryTransient(maxAttempts = 3, backoff = fastBackoff, isTransient = { true }) {
        attempts++
        throw CancellationException("cancelled")
      }
    }
    assertThat(attempts).isEqualTo(1)
  }

  @Test
  fun `retryTransient rejects non-positive maxAttempts`(): Unit = runBlocking {
    assertFailsWith<IllegalArgumentException> {
      retryTransient(maxAttempts = 0, backoff = fastBackoff, isTransient = { true }) { "x" }
    }
  }

  @Test
  fun `isTransientStorageFailure matches wrapped IO and transient gRPC failures`() {
    assertThat(isTransientStorageFailure(IOException("Broken pipe"))).isTrue()
    assertThat(isTransientStorageFailure(RuntimeException(SocketException("Broken pipe")))).isTrue()
    assertThat(isTransientStorageFailure(RuntimeException(EOFException("SSL peer shut down"))))
      .isTrue()
    // Cloud KMS "decryption failed" wrapping a token-endpoint IOException (the production case).
    assertThat(
        isTransientStorageFailure(
          GeneralSecurityException(
            "decryption failed",
            IOException("Error requesting access token"),
          )
        )
      )
      .isTrue()
    assertThat(isTransientStorageFailure(StatusException(Status.UNAVAILABLE))).isTrue()
  }

  @Test
  fun `isTransientStorageFailure ignores non-transient failures`() {
    assertThat(isTransientStorageFailure(IllegalArgumentException("bad blob"))).isFalse()
    // A genuine crypto failure with no IO cause must not be retried.
    assertThat(isTransientStorageFailure(GeneralSecurityException("decryption failed"))).isFalse()
  }

  @Test
  fun `isTransientGrpcFailure matches UNAVAILABLE and DEADLINE_EXCEEDED`() {
    assertThat(isTransientGrpcFailure(StatusException(Status.UNAVAILABLE))).isTrue()
    assertThat(isTransientGrpcFailure(StatusRuntimeException(Status.DEADLINE_EXCEEDED))).isTrue()
    assertThat(isTransientGrpcFailure(RuntimeException(StatusException(Status.UNAVAILABLE))))
      .isTrue()
  }

  @Test
  fun `isTransientGrpcFailure ignores non-transient statuses and non-gRPC failures`() {
    assertThat(isTransientGrpcFailure(StatusException(Status.INTERNAL))).isFalse()
    assertThat(isTransientGrpcFailure(StatusException(Status.INVALID_ARGUMENT))).isFalse()
    assertThat(isTransientGrpcFailure(IOException("io"))).isFalse()
  }
}
