package org.wfanet.measurement.common

import kotlinx.coroutines.Job
import java.util.concurrent.atomic.AtomicInteger

/**
 * Suspending version of [java.util.concurrent.CountDownLatch].
 *
 * @param count the number of times [countDown] can be called before a call to
 *     [await] would resume.
 */
class CountDownLatch(count: Int) {
  init {
    require(count >= 0)
  }

  private val counter = AtomicInteger(count)
  private val job = Job().apply { if (count == 0) complete() }

  /** The current count. */
  val count: Int
    get() = counter.get()

  /** Decrements [count] if it's greater than zero. */
  fun countDown() {
    val previous = counter.getAndUpdate { maxOf(0, it - 1) }
    if (previous == 1) {
      job.complete()
    }
  }

  /** Suspends the current coroutine until [count] is zero. */
  suspend fun await() = job.join()
}
