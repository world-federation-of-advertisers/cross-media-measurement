package org.wfanet.measurement.common

import java.util.logging.Level
import java.util.logging.Logger

private val logger = Logger.getLogger("org.wfanet.measurement.common.LogExceptions")

/**
 * Logs any exception thrown by [block] and rethrows it.
 *
 * @param[level] the log level to log exceptions at
 * @param[block] the function to run
 * @throws[Exception] any exception that [block] throws
 * @return the result of [block]
 */
fun <T> logBeforeThrowing(level: Level = Level.SEVERE, block: () -> T): T =
  runCatching(block).getOrElse { logAndThrow(it, level) }

/**
 * Logs any exception thrown by [block].
 *
 * @param[level] the log level to log exceptions at
 * @param[block] the function to run
 * @return the result of [block] or null if it threw
 */
fun <T> logAndSuppressException(level: Level = Level.SEVERE, block: () -> T): T? =
  runCatching { logBeforeThrowing(level, block) }.getOrNull()

private fun logAndThrow(throwable: Throwable, level: Level): Nothing {
  logger.log(level, "Exception:", throwable)
  throw throwable
}
