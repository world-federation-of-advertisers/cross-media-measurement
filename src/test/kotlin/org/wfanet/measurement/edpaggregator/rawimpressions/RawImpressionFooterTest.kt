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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.ParquetStorageClient

@RunWith(JUnit4::class)
class RawImpressionFooterTest {
  @get:Rule val tempFolder = TemporaryFolder()

  private fun client(): ParquetStorageClient =
    ParquetStorageClient(Configuration(), Path(tempFolder.root.absolutePath))

  @Test
  fun `reads event_date from the plaintext footer`() = runBlocking {
    val client = client()
    client.writeBlob(
      "file.parquet",
      emptyFlow(),
      mapOf("event_group_reference_id" to "eg-1", "event_date" to "2026-06-01"),
    )

    assertThat(readEventDateFromFooter(client, "file.parquet"))
      .isEqualTo(LocalDate.parse("2026-06-01"))
  }

  @Test
  fun `throws when the footer has no event_date`() {
    runBlocking {
      val client = client()
      client.writeBlob("file.parquet", emptyFlow(), mapOf("event_group_reference_id" to "eg-1"))

      assertFailsWith<IllegalArgumentException> { readEventDateFromFooter(client, "file.parquet") }
    }
  }

  @Test
  fun `throws when the footer event_date is malformed`() {
    runBlocking {
      val client = client()
      client.writeBlob(
        "file.parquet",
        emptyFlow(),
        mapOf("event_group_reference_id" to "eg-1", "event_date" to "not-a-date"),
      )

      assertFailsWith<IllegalArgumentException> { readEventDateFromFooter(client, "file.parquet") }
    }
  }
}
