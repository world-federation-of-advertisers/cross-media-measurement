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

import java.util.logging.Level
import java.util.logging.Logger

private val logger = Logger.getLogger("org.wfanet.measurement.common.LogExceptions")

/**
 * Logs any exception thrown by [block].
 *
 * @param level the log level to log exceptions at
 * @param block the function to run
 * @return the result of [block] or null if it threw
 */
fun <T> logAndSuppressException(level: Level = Level.SEVERE, block: () -> T): T? =
  runCatching { block() }.onFailure { logException(it, level) }.getOrNull()

suspend fun <T> logAndSuppressExceptionSuspend(
  level: Level = Level.SEVERE,
  block: suspend () -> T
): T? =
  runCatching { block() }.onFailure { logException(it, level) }.getOrNull()

private fun logException(throwable: Throwable, level: Level) {
  logger.log(level, "Exception:", throwable)
}
