// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.common

import java.time.Duration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout

/** Generalization of duration-based timeouts to arbitrary conditions. */
interface Timeout {

  /**
   * Executes [block].
   *
   * This may throw [TimeoutCancellationException] if some condition happens before [block]
   * finishes.
   */
  @Throws(TimeoutCancellationException::class)
  suspend fun <T> runWithTimeout(block: suspend CoroutineScope.() -> T): T
}

/** Builds a [Timeout] instance from a [Duration]. */
fun Duration.asTimeout(): Timeout {
  return object : Timeout {
    override suspend fun <T> runWithTimeout(block: suspend CoroutineScope.() -> T): T {
      return withTimeout(toMillis(), block)
    }
  }
}
