// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill

import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.time.delay
import kotlinx.coroutines.yield

/**
 * Processes available work serially, waiting only when no work was claimed or an error occurred.
 */
object MillWorkLoop {
  /**
   * Runs [claimAndProcessWork] until cancellation.
   *
   * Successful claims are followed immediately by another claim. Empty claims and exceptions wait
   * [pollingInterval] before trying again. Cancellation is propagated.
   */
  suspend fun run(pollingInterval: Duration, claimAndProcessWork: suspend () -> Boolean) {
    require(!pollingInterval.isNegative && !pollingInterval.isZero) {
      "Polling interval must be positive"
    }
    while (currentCoroutineContext().isActive) {
      val claimed =
        try {
          claimAndProcessWork()
        } catch (e: CancellationException) {
          throw e
        } catch (e: Exception) {
          logger.log(Level.WARNING, "Error claiming or processing computation", e)
          false
        }
      if (claimed) {
        yield()
      } else {
        delay(pollingInterval)
      }
    }
  }

  private val logger = Logger.getLogger(MillWorkLoop::class.java.name)
}
