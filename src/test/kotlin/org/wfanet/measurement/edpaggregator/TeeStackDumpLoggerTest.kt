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
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class TeeStackDumpLoggerTest {
  @Test
  fun `startFromEnvironment is disabled when the variable is unset or blank`() {
    assertThat(TeeStackDumpLogger.startFromEnvironment(emptyMap())).isNull()
    assertThat(
        TeeStackDumpLogger.startFromEnvironment(mapOf(TeeStackDumpLogger.INTERVAL_ENV to "  "))
      )
      .isNull()
  }

  @Test
  fun `startFromEnvironment ignores non-positive or non-numeric values`() {
    for (value in listOf("0", "-5", "30s", "abc", Long.MAX_VALUE.toString())) {
      assertThat(
          TeeStackDumpLogger.startFromEnvironment(mapOf(TeeStackDumpLogger.INTERVAL_ENV to value))
        )
        .isNull()
    }
  }

  @Test
  fun `startFromEnvironment starts a daemon dump thread for a positive interval`() {
    val thread =
      TeeStackDumpLogger.startFromEnvironment(mapOf(TeeStackDumpLogger.INTERVAL_ENV to "3600"))
    try {
      assertThat(thread).isNotNull()
      assertThat(thread!!.isDaemon).isTrue()
      assertThat(thread.isAlive).isTrue()
    } finally {
      thread?.interrupt()
      thread?.join()
    }
    assertThat(thread!!.isAlive).isFalse()
  }

  @Test
  fun `logDumpNow logs a dump of the live threads`() {
    val records = mutableListOf<String>()
    val handler =
      object : Handler() {
        override fun publish(record: LogRecord) {
          records += record.message
        }

        override fun flush() {}

        override fun close() {}
      }
    val logger = Logger.getLogger(TeeStackDumpLogger::class.java.name)
    logger.addHandler(handler)
    try {
      TeeStackDumpLogger.logDumpNow()
    } finally {
      logger.removeHandler(handler)
    }

    assertThat(records).hasSize(1)
    assertThat(records.single()).startsWith("Thread dump (")
    assertThat(records.single()).contains("\"${Thread.currentThread().name}\"")
  }

  @Test
  fun `start rejects an interval that overflows milliseconds`() {
    assertFailsWith<IllegalArgumentException> {
      TeeStackDumpLogger.start(Duration.ofSeconds(Long.MAX_VALUE))
    }
  }

  @Test
  fun `format orders threads by name and truncates deep stacks`() {
    val frames =
      Array(TeeStackDumpLogger.MAX_FRAMES_PER_THREAD + 5) { i ->
        StackTraceElement("com.example.Deep", "frame$i", "Deep.kt", i + 1)
      }
    val zeta = Thread(null, {}, "zeta")
    val alpha = Thread(null, {}, "alpha")

    val dump =
      TeeStackDumpLogger.format(
        mapOf(
          zeta to arrayOf(StackTraceElement("com.example.Z", "run", "Z.kt", 1)),
          alpha to frames,
        )
      )

    assertThat(dump).startsWith("Thread dump (2 threads)")
    assertThat(dump.indexOf("\"alpha\"")).isLessThan(dump.indexOf("\"zeta\""))
    assertThat(dump).contains("at com.example.Deep.frame0(Deep.kt:1)")
    assertThat(dump).doesNotContain("frame${TeeStackDumpLogger.MAX_FRAMES_PER_THREAD}(")
    assertThat(dump).contains("... 5 more")
    assertThat(dump).contains("at com.example.Z.run(Z.kt:1)")
  }
}
