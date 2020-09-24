// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
