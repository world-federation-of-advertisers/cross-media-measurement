package org.wfanet.measurement.common

import java.util.logging.Level
import java.util.logging.Logger

private val logger = Logger.getLogger("org.wfanet.measurement.common.LogExceptions")

/**
 * Logs any exception thrown by [block].
 *
 * @param[level] the log level to log exceptions at
 * @param[block] the function to run
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
