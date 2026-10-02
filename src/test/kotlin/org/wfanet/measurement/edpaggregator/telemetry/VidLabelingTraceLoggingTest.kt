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

package org.wfanet.measurement.edpaggregator.telemetry

import com.google.common.truth.Truth.assertThat
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes

@RunWith(JUnit4::class)
class VidLabelingTraceLoggingTest {
  @Test
  fun `sha256 returns a stable one-way identifier`() {
    assertThat(VidLabelingTraceLogging.sha256("gs://bucket/path/done"))
      .isEqualTo("87d8490f17edd49fce27593d73573197633cf69b51d8bfa4ae35fec95acc1c94")
  }

  @Test
  fun `log writes allowlisted VID lifecycle fields`() {
    val records = mutableListOf<LogRecord>()
    val logger = Logger.getAnonymousLogger().apply { addHandler(recordingHandler(records)) }

    VidLabelingTraceLogging.log(
      logger,
      "edpa.vid_labeling.dispatch",
      VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME_STRING to
        "dataProviders/123/rawImpressionUploads/456",
      XmmTraceAttributes.OUTCOME_STRING to "started",
    )

    assertThat(records.single().message)
      .isEqualTo(
        "event=edpa.vid_labeling.dispatch " +
          "xmm.edpa.raw_impression_upload.name=dataProviders/123/rawImpressionUploads/456 " +
          "xmm.outcome=started"
      )
  }

  @Test
  fun `log rejects payload fields`() {
    val logger = Logger.getAnonymousLogger()

    assertFailsWith<IllegalArgumentException> {
      VidLabelingTraceLogging.log(
        logger,
        "edpa.vid_labeling.dispatch",
        "fingerprint" to "sensitive-value",
      )
    }
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
