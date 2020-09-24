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

package org.wfanet.measurement.common

import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.Job

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
