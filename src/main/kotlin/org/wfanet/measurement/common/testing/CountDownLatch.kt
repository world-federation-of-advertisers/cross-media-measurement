package org.wfanet.measurement.common.testing

import java.time.Duration
import java.util.concurrent.CountDownLatch
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

/**
 * Runs [block] in a coroutine until [latch] is zero and then cancels the coroutine running [block].
 *
 * Since [CountDownLatch] is from Java, there is no suspending version of [CountDownLatch.await].
 * Thus, this implementation calls [delay] in a loop until the latch is at zero.
 */
fun launchAndCancelWithLatch(
  latch: CountDownLatch,
  timeout: Duration = Duration.ofSeconds(10),
  block: suspend () -> Unit
) = runBlocking<Unit> {
  withTimeout(timeout.toMillis()) {
    val job = launch { block() }
    while (latch.count > 0) {
      delay(200)
    }
    job.cancelAndJoin()
  }
}
