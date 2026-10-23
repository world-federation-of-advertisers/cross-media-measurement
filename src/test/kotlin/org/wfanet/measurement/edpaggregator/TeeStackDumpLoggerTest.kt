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

import com.google.common.truth.Truth.assertThat
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class TeeStackDumpLoggerTest {
  /** A captured log record and the thread that published it. */
  private data class Published(val message: String, val thread: Thread)

  private val published = mutableListOf<Published>()
  private val dumpPublished = Semaphore(0)
  private val handler =
    object : Handler() {
      override fun publish(record: LogRecord) {
        val message = record.message
        synchronized(published) { published += Published(message, Thread.currentThread()) }
        if (message.startsWith("Thread dump ") && message.contains(" part=1/")) {
          dumpPublished.release()
        }
      }

      override fun flush() {}

      override fun close() {}
    }
  private val logger = Logger.getLogger(TeeStackDumpLogger::class.java.name)

  @Before
  fun addHandler() {
    logger.addHandler(handler)
  }

  @After
  fun removeHandler() {
    logger.removeHandler(handler)
  }

  private fun messages(): List<String> = synchronized(published) { published.map { it.message } }

  @Test
  fun `startFromEnvironment is disabled when the variable is unset or blank`() {
    assertThat(TeeStackDumpLogger.startFromEnvironment(emptyMap())).isNull()
    assertThat(
        TeeStackDumpLogger.startFromEnvironment(mapOf(TeeStackDumpLogger.INTERVAL_ENV to "  "))
      )
      .isNull()
  }

  @Test
  fun `startFromEnvironment ignores invalid values without echoing them`() {
    val invalidValues =
      listOf("0", "-5", "30s", "abc", "sensitive-interval-marker", Long.MAX_VALUE.toString())
    for (value in invalidValues) {
      assertThat(
          TeeStackDumpLogger.startFromEnvironment(mapOf(TeeStackDumpLogger.INTERVAL_ENV to value))
        )
        .isNull()
    }

    val warnings = messages()
    assertThat(warnings).hasSize(invalidValues.size)
    assertThat(warnings.toSet()).hasSize(1)
    assertThat(warnings.first()).doesNotContain("sensitive-interval-marker")
  }

  @Test
  fun `start rejects non-positive and overflowing intervals`() {
    for (interval in
      listOf(Duration.ZERO, Duration.ofSeconds(-1), Duration.ofSeconds(Long.MAX_VALUE))) {
      assertFailsWith<IllegalArgumentException> { TeeStackDumpLogger.start(interval) }
    }
  }

  @Test
  fun `sampler logs a dump on each tick while a worker thread stays blocked`() {
    val workerStarted = CountDownLatch(1)
    val releaseWorker = CountDownLatch(1)
    val workerFinished = AtomicBoolean(false)
    val worker =
      Thread(
        null,
        {
          workerStarted.countDown()
          releaseWorker.await()
          workerFinished.set(true)
        },
        "worker",
      )
    worker.start()
    workerStarted.await()
    val ticks = Semaphore(0)
    val sampler = TeeStackDumpLogger.start(Duration.ofSeconds(30)) { ticks.acquire() }
    try {
      ticks.release()
      dumpPublished.acquire()
      ticks.release()
      dumpPublished.acquire()

      val dumpRecords =
        synchronized(published) { published.filter { it.message.startsWith("Thread dump ") } }
      val captureIds =
        dumpRecords.map { Regex("capture=(\\S+)").find(it.message)!!.groupValues[1] }.toSet()
      assertThat(captureIds).hasSize(2)
      assertThat(dumpRecords.map { it.thread }.toSet()).containsExactly(sampler)
      // The worker's thread state depends on scheduling, so only its ID is checked.
      val captureIdsWithWorker =
        dumpRecords
          .filter { it.message.contains("\nthread id=${worker.id} ") }
          .map { Regex("capture=(\\S+)").find(it.message)!!.groupValues[1] }
          .toSet()
      assertThat(captureIdsWithWorker).isEqualTo(captureIds)
      assertThat(workerFinished.get()).isFalse()
      assertThat(worker.isAlive).isTrue()
    } finally {
      sampler.interrupt()
      sampler.join()
      releaseWorker.countDown()
      worker.join()
    }
    assertThat(sampler.isAlive).isFalse()
  }

  @Test
  fun `dumps identify threads by numeric id and never by name`() {
    val releaseWorker = CountDownLatch(1)
    val worker = Thread(null, { releaseWorker.await() }, "sensitive-thread-name-marker")
    worker.start()
    try {
      TeeStackDumpLogger.logDumpNow()
    } finally {
      releaseWorker.countDown()
      worker.join()
    }

    val dump = messages().filter { it.startsWith("Thread dump ") }.joinToString("\n")
    assertThat(dump).contains("thread id=${worker.id} ")
    assertThat(dump).doesNotContain("sensitive-thread-name-marker")
  }

  @Test
  fun `formatRecords splits a large dump into bounded records of one capture`() {
    val frames =
      Array(TeeStackDumpLogger.MAX_FRAMES_PER_THREAD) { i ->
        StackTraceElement(
          "com.example.some.deeply.nested.pkg.Worker$i",
          "run$i",
          "Worker.kt",
          i + 1,
        )
      }
    val threads = List(3_000) { Thread(null, {}, "t$it") }
    val stacks = threads.associateWith { frames }

    val records = TeeStackDumpLogger.formatRecords(stacks, captureId = "capture-1")

    assertThat(records.size).isGreaterThan(1)
    for ((index, record) in records.withIndex()) {
      assertThat(record.toByteArray(Charsets.UTF_8).size)
        .isAtMost(TeeStackDumpLogger.MAX_RECORD_BYTES)
      assertThat(record)
        .startsWith(
          "Thread dump capture=capture-1 part=${index + 1}/${records.size} threads=${threads.size}"
        )
    }
    val threadLines =
      records.flatMap { record -> record.lines().filter { it.startsWith("thread id=") } }
    assertThat(threadLines.map { Regex("id=(\\d+)").find(it)!!.groupValues[1].toLong() })
      .containsExactlyElementsIn(threads.map { it.id })
    assertThat(records.sumOf { record -> record.lines().count { it.startsWith("    at ") } })
      .isEqualTo(threads.size * frames.size)
  }

  @Test
  fun `formatRecords splits one oversized thread across parts with its id repeated`() {
    val frames =
      Array(10) { i -> StackTraceElement("com.example.Long${"x".repeat(200)}", "f$i", "L.kt", i) }
    val thread = Thread(null, {}, "t")

    val records =
      TeeStackDumpLogger.formatRecords(
        mapOf(thread to frames),
        captureId = "c",
        maxRecordBytes = 1_000,
      )

    assertThat(records.size).isGreaterThan(1)
    for (record in records) {
      assertThat(record.toByteArray(Charsets.UTF_8).size).isAtMost(1_000)
      assertThat(record).contains("thread id=${thread.id} ")
    }
    assertThat(records.sumOf { record -> record.lines().count { it.startsWith("    at ") } })
      .isEqualTo(frames.size)
  }

  @Test
  fun `formatRecords orders threads by id and truncates deep stacks`() {
    val first = Thread(null, {}, "zeta")
    val second = Thread(null, {}, "alpha")
    val deep =
      Array(TeeStackDumpLogger.MAX_FRAMES_PER_THREAD + 5) { i ->
        StackTraceElement("com.example.Deep", "frame$i", "Deep.kt", i + 1)
      }

    val record =
      TeeStackDumpLogger.formatRecords(
          mapOf(
            second to deep,
            first to arrayOf(StackTraceElement("com.example.Z", "run", "Z.kt", 1)),
          ),
          captureId = "c",
        )
        .single()

    assertThat(record.indexOf("thread id=${first.id} "))
      .isLessThan(record.indexOf("thread id=${second.id} "))
    assertThat(record).contains("at com.example.Deep.frame0(Deep.kt:1)")
    assertThat(record).doesNotContain("frame${TeeStackDumpLogger.MAX_FRAMES_PER_THREAD}(")
    assertThat(record).contains("... 5 more")
  }
}
