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

package org.wfanet.measurement.common.telemetry

import com.google.common.truth.Truth.assertThat
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class XmmTraceLoggingTest {
  @Test
  fun `log writes only allowlisted error classification`() {
    val records = mutableListOf<LogRecord>()
    val logger =
      Logger.getAnonymousLogger().apply {
        useParentHandlers = false
        addHandler(recordingHandler(records))
      }
    val error = IllegalStateException("sensitive failure details")

    XmmTraceLogging.log(
      logger,
      Level.SEVERE,
      "xmm.test.failed",
      setOf(XmmTraceAttributes.ERROR_TYPE_STRING),
      XmmTraceAttributes.ERROR_TYPE_STRING to error.javaClass.simpleName,
    )

    val record = records.single()
    assertThat(record.level).isEqualTo(Level.SEVERE)
    assertThat(record.message)
      .isEqualTo("event=xmm.test.failed xmm.error.type=IllegalStateException")
    assertThat(record.message).doesNotContain("sensitive failure details")
    assertThat(record.thrown).isNull()
  }

  private fun recordingHandler(records: MutableList<LogRecord>): Handler =
    object : Handler() {
      override fun publish(record: LogRecord) {
        records += record
      }

      override fun flush() {}

      override fun close() {}
    }
}
