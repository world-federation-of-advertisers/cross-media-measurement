/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator

import java.time.Duration
import java.util.logging.Logger
import kotlin.concurrent.thread

/**
 * Opt-in periodic thread dumps for TEE applications.
 *
 * Confidential Space gives operators no shell, `exec`, or `jstack`, so a stuck worker is otherwise
 * opaque. When [INTERVAL_ENV] is set to a positive number of seconds, a daemon thread logs every
 * thread's stack through `java.util.logging` at that interval, which the TEE log redirect delivers
 * to Cloud Logging. Disabled by default.
 */
object TeeStackDumpLogger {
  /** Environment variable holding the dump interval in seconds. Unset or blank disables dumps. */
  const val INTERVAL_ENV = "TEE_STACK_DUMP_INTERVAL_SECONDS"

  /** Frames logged per thread; deeper frames are summarized as a count. */
  const val MAX_FRAMES_PER_THREAD = 20

  private const val THREAD_NAME = "tee-stack-dump"
  private val logger: Logger = Logger.getLogger(TeeStackDumpLogger::class.java.name)

  /**
   * Starts periodic dumps if [INTERVAL_ENV] in [environment] is a positive integer.
   *
   * @return the dump thread, or `null` when dumps are disabled or the value is invalid (logged).
   */
  fun startFromEnvironment(environment: Map<String, String> = System.getenv()): Thread? {
    val raw: String = environment[INTERVAL_ENV]?.takeIf { it.isNotBlank() } ?: return null
    val seconds: Long? = raw.trim().toLongOrNull()
    if (seconds == null || seconds <= 0) {
      logger.warning("Ignoring $INTERVAL_ENV=$raw; expected a positive whole number of seconds")
      return null
    }
    return start(Duration.ofSeconds(seconds))
  }

  /** Starts a daemon thread that logs a thread dump every [interval] until interrupted. */
  fun start(interval: Duration): Thread {
    require(interval > Duration.ZERO) { "interval must be positive" }
    return thread(isDaemon = true, name = THREAD_NAME) {
      logger.info("Logging thread stack dumps every ${interval.seconds}s")
      while (!Thread.currentThread().isInterrupted) {
        try {
          Thread.sleep(interval.toMillis())
        } catch (e: InterruptedException) {
          break
        }
        logger.info(format(Thread.getAllStackTraces()))
      }
    }
  }

  /**
   * Formats [stacks] as a thread dump ordered by thread name, with at most [maxFrames] frames per
   * thread.
   */
  fun format(
    stacks: Map<Thread, Array<StackTraceElement>>,
    maxFrames: Int = MAX_FRAMES_PER_THREAD,
  ): String = buildString {
    append("Thread dump (").append(stacks.size).append(" threads)")
    for ((thread, frames) in stacks.entries.sortedBy { it.key.name }) {
      append("\n\"").append(thread.name).append("\" daemon=").append(thread.isDaemon)
      append(" state=").append(thread.state)
      for (frame in frames.take(maxFrames)) {
        append("\n    at ").append(frame)
      }
      if (frames.size > maxFrames) {
        append("\n    ... ").append(frames.size - maxFrames).append(" more")
      }
    }
  }
}
