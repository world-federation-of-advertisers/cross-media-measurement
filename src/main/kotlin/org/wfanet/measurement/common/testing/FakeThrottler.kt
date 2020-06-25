package org.wfanet.measurement.common.testing

import kotlinx.coroutines.delay
import org.wfanet.measurement.common.Throttler

class FakeThrottler : Throttler {
  var canAttempt = true

  override suspend fun <T> onReady(block: suspend () -> T): T {
    while (!canAttempt) {
      delay(200)
    }
    return block()
  }
}
