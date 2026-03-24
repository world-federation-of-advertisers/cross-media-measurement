/*
 * Copyright 2025 The Cross-Media Measurement Authors
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
import kotlinx.coroutines.runBlocking
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
    private const val MODEL_LINE_A = "modelLineA"
    private const val MODEL_LINE_B = "modelLineB"
    private val TODAY = LocalDate.of(2026, 3, 15)
  }

  private fun createDoneBlob(
    storageClient: FileSystemStorageClient,
    modelLine: String,
    date: String,
  ) {
    val path = "$EDP_IMPRESSION_PATH/model-line/$modelLine/$date/done"
    runBlocking { storageClient.writeBlob(path, ByteString.copyFromUtf8("done")) }
  }

  private fun createStorageClient(): FileSystemStorageClient {
    return FileSystemStorageClient(tempFolder.root)
  }

  private fun ensureDirectories(modelLine: String, date: String) {
    val dir = File(tempFolder.root, "$EDP_IMPRESSION_PATH/model-line/$modelLine/$date")
    dir.mkdirs()
  }

  @Test
  fun `check returns no issues when all dates are present and recent`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    for (day in 12..15) {
      ensureDirectories(MODEL_LINE_A, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        maxStaleDays = 3,
        clock = { TODAY },
      )

    val result = monitor.check()
    assertThat(result.hasIssues).isFalse()
    assertThat(result.statuses).hasSize(1)

    val status = result.statuses.single()
    assertThat(status.modelLineId).isEqualTo(MODEL_LINE_A)
    assertThat(status.isStale).isFalse()
    assertThat(status.missingDates).isEmpty()
    assertThat(status.latestDate).isEqualTo(LocalDate.of(2026, 3, 15))
  }

  @Test
  fun `check detects staleness when latest upload is too old`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    // Only upload through March 11 — 4 days before "today" (March 15)
    for (day in 9..11) {
      ensureDirectories(MODEL_LINE_A, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        maxStaleDays = 3,
        clock = { TODAY },
      )

    val result = monitor.check()
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.isStale).isTrue()
    assertThat(status.staleDays).isEqualTo(4)
    assertThat(status.latestDate).isEqualTo(LocalDate.of(2026, 3, 11))
  }

  @Test
  fun `check detects gap in uploaded dates`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    // Upload March 12, 13, 15 — missing March 14
    for (day in listOf(12, 13, 15)) {
      ensureDirectories(MODEL_LINE_A, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        maxStaleDays = 3,
        clock = { TODAY },
      )

    val result = monitor.check()
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.isStale).isFalse()
    assertThat(status.missingDates).containsExactly(LocalDate.of(2026, 3, 14))
  }

  @Test
  fun `check detects multiple gaps`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    // Upload March 10, 12, 15 — missing March 11, 13, 14
    for (day in listOf(10, 12, 15)) {
      ensureDirectories(MODEL_LINE_A, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        maxStaleDays = 3,
        clock = { TODAY },
      )

    val result = monitor.check()
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
    // Model line A: up to date
    for (day in 13..15) {
      ensureDirectories(MODEL_LINE_A, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A, "2026-03-%02d".format(day))
    }
    // Model line B: stale (only up to March 11)
    for (day in 9..11) {
      ensureDirectories(MODEL_LINE_B, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_B, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A, MODEL_LINE_B),
        maxStaleDays = 3,
        clock = { TODAY },
      )

    val result = monitor.check()
    assertThat(result.hasIssues).isTrue()

    val statusA = result.statuses.first { it.modelLineId == MODEL_LINE_A }
    assertThat(statusA.isStale).isFalse()
    assertThat(statusA.missingDates).isEmpty()

    val statusB = result.statuses.first { it.modelLineId == MODEL_LINE_B }
    assertThat(statusB.isStale).isTrue()
    assertThat(statusB.staleDays).isEqualTo(4)
  }

  @Test
  fun `check handles model line with no uploads`(): Unit = runBlocking {
    val storageClient = createStorageClient()

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        maxStaleDays = 3,
        clock = { TODAY },
      )

    val result = monitor.check()
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.isStale).isTrue()
    assertThat(status.latestDate).isNull()
    assertThat(status.staleDays).isEqualTo(Int.MAX_VALUE)
  }

  @Test
  fun `check does not flag staleness when within threshold`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    // Upload through March 12 — exactly 3 days before "today" (March 15)
    for (day in 10..12) {
      ensureDirectories(MODEL_LINE_A, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        maxStaleDays = 3,
        clock = { TODAY },
      )

    val result = monitor.check()
    assertThat(result.hasIssues).isFalse()

    val status = result.statuses.single()
    assertThat(status.isStale).isFalse()
    assertThat(status.staleDays).isEqualTo(3)
  }

  @Test
  fun `check detects both staleness and gaps simultaneously`(): Unit = runBlocking {
    val storageClient = createStorageClient()
    // Upload March 8, 10 — missing March 9, and stale (latest is March 10, 5 days ago)
    for (day in listOf(8, 10)) {
      ensureDirectories(MODEL_LINE_A, "2026-03-%02d".format(day))
      createDoneBlob(storageClient, MODEL_LINE_A, "2026-03-%02d".format(day))
    }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = EDP_IMPRESSION_PATH,
        activeModelLines = setOf(MODEL_LINE_A),
        maxStaleDays = 3,
        clock = { TODAY },
      )

    val result = monitor.check()
    assertThat(result.hasIssues).isTrue()

    val status = result.statuses.single()
    assertThat(status.isStale).isTrue()
    assertThat(status.staleDays).isEqualTo(5)
    assertThat(status.missingDates).containsExactly(LocalDate.of(2026, 3, 9))
  }
}
