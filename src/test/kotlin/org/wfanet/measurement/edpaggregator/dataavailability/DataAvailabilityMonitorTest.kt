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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.io.File
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class DataAvailabilityMonitorTest {

  @get:Rule val tempFolder = TemporaryFolder()

  companion object {
    private const val EDP_IMPRESSION_PATH = "edp/meta/vid-labeled-impressions"
    private val MODEL_LINE_A = ModelLineKey("provider1", "suite1", "modelLineA")
    private val MODEL_LINE_B = ModelLineKey("provider1", "suite1", "modelLineB")
    private val TODAY = LocalDate.of(2026, 3, 15)
  }

  private fun createDoneBlob(
    storageClient: FileSystemStorageClient,
    modelLine: String,
    date: String,
  ): Unit = runBlocking {
    val path = "$EDP_IMPRESSION_PATH/model-line/$modelLine/$date/done"
    storageClient.writeBlob(path, ByteString.copyFromUtf8("done"))
  }

  private fun createStorageClient(): FileSystemStorageClient {
    return FileSystemStorageClient(tempFolder.root)
  }

  private fun ensureDirectories(modelLine: String, date: String) {
    val dir = File(tempFolder.root, "$EDP_IMPRESSION_PATH/model-line/$modelLine/$date")
    dir.mkdirs()
  }

  // --- check() tests (both staleness and gaps) ---

  @Test
  fun `check returns no issues when all dates are present and recent`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 12..15) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isFalse()
    assertThat(result.statuses).hasSize(1)

    val status = result.statuses.single()
    assertThat(status.modelLineId).isEqualTo(MODEL_LINE_A.toName())
    assertThat(status.isStale).isFalse()
    assertThat(status.missingDates).isEmpty()
    assertThat(status.latestDate).isEqualTo(LocalDate.of(2026, 3, 15))
  }

  @Test
  fun `check detects staleness when latest upload is too old`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 9..11) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.isStale).isTrue()
    assertThat(status.staleDays).isEqualTo(4)
    assertThat(status.latestDate).isEqualTo(LocalDate.of(2026, 3, 11))
  }

  @Test
  fun `check detects gap in uploaded dates`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in listOf(12, 13, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.isStale).isFalse()
    assertThat(status.missingDates).containsExactly(LocalDate.of(2026, 3, 14))
  }

  @Test
  fun `check detects multiple gaps`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in listOf(10, 12, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.missingDates)
      .containsExactly(
        LocalDate.of(2026, 3, 11),
        LocalDate.of(2026, 3, 13),
        LocalDate.of(2026, 3, 14),
      )
  }

  @Test
  fun `check handles multiple model lines independently`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 13..15) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }
    for (day in 9..11) {
      ensureDirectories(MODEL_LINE_B.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_B.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A, MODEL_LINE_B),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val statusA = result.statuses.first { it.modelLineId == MODEL_LINE_A.toName() }
    assertThat(statusA.isStale).isFalse()
    assertThat(statusA.missingDates).isEmpty()

    val statusB = result.statuses.first { it.modelLineId == MODEL_LINE_B.toName() }
    assertThat(statusB.isStale).isTrue()
    assertThat(statusB.staleDays).isEqualTo(4)
  }

  @Test
  fun `check throws when model line has no uploads`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    assertFailsWith<IllegalArgumentException> { monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY }) }
  }

  @Test
  fun `check does not flag staleness when within threshold`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 10..12) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isFalse()

    val status = result.statuses.single()
    assertThat(status.isStale).isFalse()
    assertThat(status.staleDays).isEqualTo(3)
  }

  @Test
  fun `check detects both staleness and gaps simultaneously`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in listOf(8, 10)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.isStale).isTrue()
    assertThat(status.staleDays).isEqualTo(5)
    assertThat(status.missingDates).containsExactly(LocalDate.of(2026, 3, 9))
  }

  // --- checkGaps() tests ---

  @Test
  fun `checkGaps returns no issues when all dates are contiguous`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 12..15) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      val dataPath =
        "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-%02d/metadata_campaign_1.json"
          .format(day)
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    assertThat(result.hasIssues).isFalse()

    val status = result.statuses.single()
    assertThat(status.missingDates).isEmpty()
    assertThat(status.isStale).isNull()
    assertThat(status.staleDays).isNull()
  }

  @Test
  fun `checkGaps detects missing dates`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in listOf(12, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      val dataPath =
        "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-%02d/metadata_campaign_1.json"
          .format(day)
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.missingDates)
      .containsExactly(LocalDate.of(2026, 3, 13), LocalDate.of(2026, 3, 14))
    assertThat(status.isStale).isNull()
  }

  @Test
  fun `checkGaps throws when model line has no uploads`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    assertFailsWith<IllegalArgumentException> { monitor.checkGaps() }
  }

  // --- Edge case tests ---

  @Test
  fun `checkGaps detects empty date folder with done blob but no data files`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // March 13 has done + data, March 14 has only done (empty), March 15 has done + data
    for (day in listOf(13, 14, 15)) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
    }
    // Add data files for March 13 and 15 only
    for (day in listOf(13, 15)) {
      val dataPath =
        "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-%02d/metadata_campaign_1.json"
          .format(day)
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.missingDates).isEmpty()
    assertThat(status.emptyDateFolders).containsExactly(LocalDate.of(2026, 3, 14))
  }

  @Test
  fun `checkGaps returns no issues when all folders have data files`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    for (day in 13..15) {
      ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-%02d".format(day))
      val dataPath =
        "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/2026-03-%02d/metadata_campaign_1.json"
          .format(day)
      storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    val result = monitor.checkGaps()
    assertThat(result.hasIssues).isFalse()
    assertThat(result.statuses.single().emptyDateFolders).isEmpty()
  }


  @Test
  fun `checkFullStatus throws when folder name is not a valid date`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // Valid date folder
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    // Invalid date format folder
    val badPath = "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/not-a-date/done"
    File(tempFolder.root, badPath).parentFile.mkdirs()
    storageClient.writeBlob(badPath, ByteString.copyFromUtf8("done"))

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    assertFailsWith<java.time.format.DateTimeParseException> {
      monitor.checkFullStatus(maxStaleDays = 3, clock = { TODAY })
    }
  }

  @Test
  fun `checkGaps throws when folder name is not a valid date`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    // Valid date folder
    ensureDirectories(MODEL_LINE_A.modelLineId, "2026-03-15")
    createDoneBlob(storageClient, MODEL_LINE_A.modelLineId, "2026-03-15")

    // Invalid date format folder
    val badPath = "$EDP_IMPRESSION_PATH/model-line/${MODEL_LINE_A.modelLineId}/not-a-date/done"
    File(tempFolder.root, badPath).parentFile.mkdirs()
    storageClient.writeBlob(badPath, ByteString.copyFromUtf8("done"))

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
      )

    assertFailsWith<java.time.format.DateTimeParseException> {
      monitor.checkGaps()
    }
  }
}
