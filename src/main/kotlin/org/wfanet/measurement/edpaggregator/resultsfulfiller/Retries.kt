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

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.io.IOException
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import org.wfanet.measurement.common.ExponentialBackoff

private val logger: Logger =
  Logger.getLogger("org.wfanet.measurement.edpaggregator.resultsfulfiller.Retries")

/**
 * Runs [block], retrying up to [maxAttempts] total attempts when it fails with a transient error
 * (as judged by [isTransient]), backing off via [backoff] between attempts.
 *
 * The final attempt's failure and any non-transient failure are rethrown as-is.
 * [CancellationException] is never retried, so coroutine cancellation propagates immediately.
 *
 * This absorbs the transient egress blips (TLS handshake terminated, broken pipe, Cloud KMS token
 * 503s, worker `DEADLINE_EXCEEDED`) that would otherwise fail an entire report and force a full,
 * expensive re-run of every blob read, decrypt, and fulfillment.
 *
 * @throws IllegalArgumentException if [maxAttempts] is less than 1.
 */
suspend fun <T> retryTransient(
  maxAttempts: Int,
  backoff: ExponentialBackoff,
  isTransient: (Throwable) -> Boolean,
  block: suspend () -> T,
): T {
  require(maxAttempts >= 1) { "maxAttempts must be at least 1" }
  var attempt = 0
  while (true) {
    attempt++
    try {
      return block()
    } catch (e: CancellationException) {
      throw e
    } catch (e: Throwable) {
      if (attempt >= maxAttempts || !isTransient(e)) {
        throw e
      }
      logger.warning {
        "Transient failure on attempt $attempt of $maxAttempts (${e.message}); retrying"
      }
      delay(backoff.durationForAttempt(attempt).toMillis())
    }
  }
}

/**
 * Returns true if [t], or any throwable in its cause chain, looks like a transient storage / KMS
 * failure worth retrying.
 *
 * The dominant production failures surface as an [IOException] somewhere in the chain: a GCS
 * `StorageException` wraps a `SocketException` ("Broken pipe") or `SSLHandshakeException` ("Remote
 * host terminated the handshake"), and a KMS `GeneralSecurityException` ("decryption failed") wraps
 * an `IOException` ("Error requesting access token") from the token endpoint. Transient gRPC
 * statuses ([Status.Code.UNAVAILABLE] / [Status.Code.DEADLINE_EXCEEDED]) are also treated as
 * transient. Non-transient failures (e.g. a missing blob or malformed data) are not matched, so
 * they fail fast rather than burning the retry budget.
 */
fun isTransientStorageFailure(t: Throwable): Boolean {
  return t.causalChain().any { cause -> cause is IOException || cause.isTransientGrpcStatus() }
}

/**
 * Returns true if [t], or any throwable in its cause chain, is a [StatusException] /
 * [StatusRuntimeException] carrying a transient gRPC status ([Status.Code.UNAVAILABLE] /
 * [Status.Code.DEADLINE_EXCEEDED]).
 */
fun isTransientGrpcFailure(t: Throwable): Boolean {
  return t.causalChain().any { it.isTransientGrpcStatus() }
}

private fun Throwable.isTransientGrpcStatus(): Boolean {
  val code =
    when (this) {
      is StatusException -> status.code
      is StatusRuntimeException -> status.code
      else -> return false
    }
  return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED
}

/** Lazily walks this throwable's cause chain (including itself), guarding against cycles. */
private fun Throwable.causalChain(): Sequence<Throwable> = sequence {
  val seen = mutableSetOf<Throwable>()
  var current: Throwable? = this@causalChain
  while (current != null && seen.add(current)) {
    yield(current)
    current = current.cause
  }
}
