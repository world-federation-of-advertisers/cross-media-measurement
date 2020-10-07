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
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.wfanet.measurement.common.CountDownLatch

/**
 * Runs [block] in a coroutine, cancelling the coroutine once [latch]'s
 * [count][CountDownLatch.count] is zero or [timeout] is reached.
 */
suspend fun launchAndCancelWithLatch(
  latch: CountDownLatch,
  timeout: Duration = Duration.ofSeconds(10),
  block: suspend () -> Unit
) {
  withTimeout(timeout.toMillis()) {
    val job = launch { block() }
    latch.await()
    job.cancelAndJoin()
  }
}
