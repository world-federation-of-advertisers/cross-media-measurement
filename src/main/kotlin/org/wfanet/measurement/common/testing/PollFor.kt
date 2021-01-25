// Copyright 2020 The Cross-Media Measurement Authors
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

import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout

/**
 * Repeatedly invokes [producer] until it returns something non-null.
 *
 * @param timeoutMillis the maximum amount of time to wait for a non-null value
 * @param delayMillis the time between calling [producer]
 * @param producer block that produces a [T] or null
 */
suspend fun <T> pollFor(
  timeoutMillis: Long = 3_000L,
  delayMillis: Long = 250L,
  producer: suspend () -> T?
): T {
  return withTimeout<T>(timeoutMillis) {
    var t: T? = producer()
    while (t == null) {
      delay(delayMillis)
      t = producer()
    }
    t
  }
}
