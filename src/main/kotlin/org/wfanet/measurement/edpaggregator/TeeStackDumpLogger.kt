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
import java.util.UUID
import java.util.logging.Logger
import kotlin.concurrent.thread

/**
 * Opt-in periodic thread dumps for TEE applications.
 *
 * Confidential Space gives operators no shell, `exec`, or `jstack`, so a stuck worker is otherwise
 * opaque. When [INTERVAL_ENV] is set to a positive number of seconds, a daemon thread logs every
 * thread's stack through `java.util.logging` at that interval, which the TEE log redirect delivers
 * to Cloud Logging. Disabled by default.
 *
 * Each capture is emitted as one or more records sharing a capture ID, each at most
 * [MAX_RECORD_BYTES] of UTF-8, because Cloud Logging rejects entries over 256 KiB and the logging
 * handler does not split messages. Threads are identified by numeric ID, daemon flag, and state —
 * never by name, which is an unrestricted runtime string.
 */
object TeeStackDumpLogger {
  /** Environment variable holding the dump interval in seconds. Unset or blank disables dumps. */
  const val INTERVAL_ENV = "TEE_STACK_DUMP_INTERVAL_SECONDS"

  /** Largest supported [INTERVAL_ENV] value: the sampler waits in milliseconds. */
  const val MAX_SUPPORTED_INTERVAL_SECONDS: Long = Long.MAX_VALUE / 1000

  /** Frames logged per thread; deeper frames are summarized as a count. */
  const val MAX_FRAMES_PER_THREAD = 20

  /**
   * Maximum UTF-8 size of one emitted record. Cloud Logging limits a LogEntry to 256 KiB including
   * metadata; this leaves room for that metadata.
   */
  const val MAX_RECORD_BYTES = 200 * 1024

  private const val THREAD_NAME = "tee-stack-dump"
  private val logger: Logger = Logger.getLogger(TeeStackDumpLogger::class.java.name)

  /**
   * Starts periodic dumps if [INTERVAL_ENV] in [environment] is a positive whole number of seconds
   * no greater than [MAX_SUPPORTED_INTERVAL_SECONDS].
   *
   * @return the dump thread, or `null` when dumps are disabled or the value is invalid. An invalid
   *   value is logged as a warning without echoing it.
   */
  fun startFromEnvironment(environment: Map<String, String> = System.getenv()): Thread? {
    val raw: String = environment[INTERVAL_ENV]?.takeIf { it.isNotBlank() } ?: return null
    val seconds: Long? = raw.trim().toLongOrNull()
    if (seconds == null || seconds <= 0 || seconds > MAX_SUPPORTED_INTERVAL_SECONDS) {
      logger.warning(
        "Ignoring $INTERVAL_ENV; expected a positive whole number of seconds no greater than " +
          "$MAX_SUPPORTED_INTERVAL_SECONDS"
      )
      return null
    }
    return start(Duration.ofSeconds(seconds))
  }

  /**
   * Starts a daemon thread that logs a thread dump after each [awaitTick] returns, until the thread
   * is interrupted.
   *
   * @param interval time between dumps.
   * @param awaitTick blocks until the next dump is due; by default, sleeps for [interval]. It must
   *   respond to interruption by throwing [InterruptedException].
   */
  fun start(
    interval: Duration,
    awaitTick: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
  ): Thread {
    require(interval > Duration.ZERO) { "interval must be positive" }
    require(interval.seconds <= MAX_SUPPORTED_INTERVAL_SECONDS) {
      "interval must be no greater than $MAX_SUPPORTED_INTERVAL_SECONDS seconds"
    }
    return thread(isDaemon = true, name = THREAD_NAME) {
      logger.info("Logging thread stack dumps every ${interval.seconds}s")
      while (!Thread.currentThread().isInterrupted) {
        try {
          awaitTick(interval)
        } catch (e: InterruptedException) {
          break
        }
        logDumpNow()
      }
    }
  }

  /** Logs one dump of every live thread immediately, as one or more bounded records. */
  fun logDumpNow() {
    for (record in formatRecords(Thread.getAllStackTraces(), UUID.randomUUID().toString())) {
      logger.info(record)
    }
  }

  /**
   * Formats [stacks] as the records of one capture identified by [captureId].
   *
   * Threads are ordered by numeric ID and listed with at most [maxFrames] frames each. Threads are
   * packed into records of at most [maxRecordBytes] of UTF-8; a thread is only split across records
   * if its own block exceeds the limit, in which case each part repeats its numeric ID.
   */
  fun formatRecords(
    stacks: Map<Thread, Array<StackTraceElement>>,
    captureId: String,
    maxFrames: Int = MAX_FRAMES_PER_THREAD,
    maxRecordBytes: Int = MAX_RECORD_BYTES,
  ): List<String> {
    // Leaves room for the per-record header, which includes the final part count.
    val bodyBudget =
      maxRecordBytes - headerBytes(captureId, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE)
    require(bodyBudget > 0) { "maxRecordBytes is too small for the record header" }

    val blocks: List<String> =
      stacks.entries
        .sortedBy { it.key.id }
        .flatMap { (thread, frames) -> threadBlocks(thread, frames, maxFrames, bodyBudget) }

    val bodies = mutableListOf<StringBuilder>()
    var current = StringBuilder()
    var currentBytes = 0
    for (block in blocks) {
      val blockBytes = utf8Size(block)
      if (currentBytes > 0 && currentBytes + blockBytes > bodyBudget) {
        bodies += current
        current = StringBuilder()
        currentBytes = 0
      }
      current.append(block)
      currentBytes += blockBytes
    }
    bodies += current

    return bodies.mapIndexed { index, body ->
      header(captureId, index + 1, bodies.size, stacks.size) + body
    }
  }

  private fun header(captureId: String, part: Int, parts: Int, threads: Int): String =
    "Thread dump capture=$captureId part=$part/$parts threads=$threads"

  private fun headerBytes(captureId: String, part: Int, parts: Int, threads: Int): Int =
    utf8Size(header(captureId, part, parts, threads))

  /**
   * Returns the block(s) for one thread. Usually one; more only if the block alone exceeds
   * [budget], in which case frames are spread across parts that each repeat the thread's ID.
   */
  private fun threadBlocks(
    thread: Thread,
    frames: Array<StackTraceElement>,
    maxFrames: Int,
    budget: Int,
  ): List<String> {
    val threadLine = "\nthread id=${thread.id} daemon=${thread.isDaemon} state=${thread.state}"
    val lines: List<String> =
      frames.take(maxFrames).map { "\n    at $it" } +
        if (frames.size > maxFrames) listOf("\n    ... ${frames.size - maxFrames} more")
        else emptyList()

    val blocks = mutableListOf<String>()
    var block = StringBuilder(threadLine)
    var blockBytes = utf8Size(threadLine)
    for (line in lines) {
      val lineBytes = utf8Size(line)
      if (blockBytes + lineBytes > budget && block.length > threadLine.length) {
        blocks += block.toString()
        val continuation = "$threadLine (continued)"
        block = StringBuilder(continuation)
        blockBytes = utf8Size(continuation)
      }
      // A single frame longer than the budget is truncated rather than dropped.
      val fitted =
        if (blockBytes + lineBytes > budget) truncateUtf8(line, budget - blockBytes) else line
      block.append(fitted)
      blockBytes += utf8Size(fitted)
    }
    blocks += block.toString()
    return blocks
  }

  private fun utf8Size(text: String): Int = text.toByteArray(Charsets.UTF_8).size

  private fun truncateUtf8(text: String, maxBytes: Int): String {
    if (maxBytes <= 0) return ""
    var end = text.length
    while (end > 0 && utf8Size(text.substring(0, end)) > maxBytes) {
      end--
    }
    return text.substring(0, end)
  }
}
