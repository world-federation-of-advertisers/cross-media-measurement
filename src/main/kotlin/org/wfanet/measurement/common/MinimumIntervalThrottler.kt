package org.wfanet.measurement.common

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * A throttler to ensure the minimum interval between actions is at least some [Duration].
 *
 * @property[clock] the clock to use
 * @property[interval] the minimum interval between events
 */
class MinimumIntervalThrottler(
  private val clock: Clock,
  private val interval: Duration
) : Throttler {
  private val mutex = Mutex(false)  // Guarantees FIFO order.
  private var lastAttempt = Instant.EPOCH

  private fun timeDelta(now: Instant): Duration {
    return Duration.between(lastAttempt, now)
  }

  override fun attempt(): Boolean {
    // Since attempt is required to be non-blocking, we return false if we can't immediately acquire
    // the mutex.
    if (!mutex.tryLock()) return false
    try {
      return attemptUnsafe()
    } finally {
      mutex.unlock()
    }
  }

  override fun reportThrottled() {
    // Deliberate no-op. This throttler does not pay any attention to pushback -- it only limits how
    // often attempt() returns true.
  }

  /**
   * Runs [block] once [attempt] would return true.
   *
   * Note that this deliberately locks the throttler until [block] completes and offers FIFO
   * semantics for concurrent calls to onReady.
   *
   * DEADLOCK WARNING: do not call [onReady] from [block]!
   */
  override suspend fun <T> onReady(block: suspend () -> T): T {
    mutex.lock()
    try {
      while (!attemptUnsafe()) {
        val delta = timeDelta(clock.instant())
        if (delta > Duration.ZERO) {
          delay(delta.toMillis())
        }
      }
      return block()
    } finally {
      mutex.unlock()
    }
  }

  private fun attemptUnsafe(): Boolean {
    val now = clock.instant()
    if (timeDelta(now) > interval) {
      lastAttempt = now
      return true
    }
    return false
  }
}
