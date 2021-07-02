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

package org.wfanet.measurement.dataprovider.daemon

import kotlinx.coroutines.yield
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.throttler.Throttler

/**
 * Runs [block] indefinitely, logging and suppressing exceptions that it throws.
 *
 * The only way to terminate this is to cancel the coroutine running it.
 */
suspend fun Throttler.loopOnReadySuppressingExceptions(block: suspend () -> Unit) {
  while (true) {
    logAndSuppressExceptionSuspend { onReady(block) }
    yield()
  }
}
