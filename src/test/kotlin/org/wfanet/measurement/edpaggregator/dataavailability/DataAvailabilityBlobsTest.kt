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
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class DataAvailabilityBlobsTest {

  // -------------------- enumerateDateInfo --------------------

  @Test
  fun `enumerateDateInfo returns empty when prefix has no blobs`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()

      val result = DataAvailabilityBlobs.enumerateDateInfo(client, "edp/test/model-line/m1/")

      assertThat(result.datesWithDoneBlob).isEmpty()
      assertThat(result.datesWithoutDoneBlob).isEmpty()
    }

  @Test
  fun `enumerateDateInfo classifies date folder with done blob as finalized`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      val prefix = "edp/test/model-line/m1/"
      client.writeBlob("${prefix}2026-03-15/done", ByteString.copyFromUtf8("done"))
      client.writeBlob("${prefix}2026-03-15/metadata.binpb", ByteString.copyFromUtf8("x"))

      val result = DataAvailabilityBlobs.enumerateDateInfo(client, prefix)

      assertThat(result.datesWithDoneBlob.keys).containsExactly(LocalDate.of(2026, 3, 15))
      assertThat(result.datesWithoutDoneBlob).isEmpty()
    }

  @Test
  fun `enumerateDateInfo classifies date folder without done blob as unfinalized`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      val prefix = "edp/test/model-line/m1/"
      client.writeBlob("${prefix}2026-03-15/metadata.binpb", ByteString.copyFromUtf8("x"))

      val result = DataAvailabilityBlobs.enumerateDateInfo(client, prefix)

      assertThat(result.datesWithDoneBlob).isEmpty()
      assertThat(result.datesWithoutDoneBlob).containsExactly(LocalDate.of(2026, 3, 15))
    }

  @Test
  fun `enumerateDateInfo skips non-date subfolder`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      val prefix = "edp/test/model-line/m1/"
      client.writeBlob("${prefix}not-a-date/done", ByteString.copyFromUtf8("done"))
      client.writeBlob("${prefix}2026-03-15/done", ByteString.copyFromUtf8("done"))

      val result = DataAvailabilityBlobs.enumerateDateInfo(client, prefix)

      assertThat(result.datesWithDoneBlob.keys).containsExactly(LocalDate.of(2026, 3, 15))
      assertThat(result.datesWithoutDoneBlob).isEmpty()
    }

  @Test
  fun `enumerateDateInfo returns unfinalized list sorted`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      val prefix = "edp/test/model-line/m1/"
      for (date in listOf("2026-03-17", "2026-03-13", "2026-03-15")) {
        client.writeBlob("${prefix}$date/metadata.binpb", ByteString.copyFromUtf8("x"))
      }

      val result = DataAvailabilityBlobs.enumerateDateInfo(client, prefix)

      assertThat(result.datesWithoutDoneBlob)
        .containsExactly(
          LocalDate.of(2026, 3, 13),
          LocalDate.of(2026, 3, 15),
          LocalDate.of(2026, 3, 17),
        )
        .inOrder()
    }

  @Test
  fun `enumerateDateInfo returns done blob handle for finalized dates`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      val prefix = "edp/test/model-line/m1/"
      client.writeBlob("${prefix}2026-03-15/done", ByteString.copyFromUtf8("done"))

      val result = DataAvailabilityBlobs.enumerateDateInfo(client, prefix)

      val doneBlob = result.datesWithDoneBlob.getValue(LocalDate.of(2026, 3, 15))
      assertThat(doneBlob.blobKey).isEqualTo("${prefix}2026-03-15/done")
    }

  // -------------------- findGaps --------------------

  @Test
  fun `findGaps returns empty for empty input`() {
    assertThat(DataAvailabilityBlobs.findGaps(emptyList())).isEmpty()
  }

  @Test
  fun `findGaps returns empty for single date`() {
    assertThat(DataAvailabilityBlobs.findGaps(listOf(LocalDate.of(2026, 3, 15)))).isEmpty()
  }

  @Test
  fun `findGaps returns empty for contiguous dates`() {
    val dates = (13..15).map { LocalDate.of(2026, 3, it) }
    assertThat(DataAvailabilityBlobs.findGaps(dates)).isEmpty()
  }

  @Test
  fun `findGaps returns single missing date`() {
    val dates = listOf(LocalDate.of(2026, 3, 13), LocalDate.of(2026, 3, 15))
    assertThat(DataAvailabilityBlobs.findGaps(dates)).containsExactly(LocalDate.of(2026, 3, 14))
  }

  @Test
  fun `findGaps returns multiple missing dates including consecutive ones`() {
    val dates = listOf(LocalDate.of(2026, 3, 13), LocalDate.of(2026, 3, 17))
    assertThat(DataAvailabilityBlobs.findGaps(dates))
      .containsExactly(
        LocalDate.of(2026, 3, 14),
        LocalDate.of(2026, 3, 15),
        LocalDate.of(2026, 3, 16),
      )
      .inOrder()
  }

  @Test
  fun `findGaps accepts unordered input`() {
    val dates = listOf(LocalDate.of(2026, 3, 17), LocalDate.of(2026, 3, 13))
    assertThat(DataAvailabilityBlobs.findGaps(dates))
      .containsExactly(
        LocalDate.of(2026, 3, 14),
        LocalDate.of(2026, 3, 15),
        LocalDate.of(2026, 3, 16),
      )
      .inOrder()
  }

  // -------------------- isMetadataBlob --------------------

  @Test
  fun `isMetadataBlob returns true for metadata-named non-empty blob`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      client.writeBlob("edp/m1/2026-03-15/metadata.binpb", ByteString.copyFromUtf8("x"))
      val blob: StorageClient.Blob = client.getBlob("edp/m1/2026-03-15/metadata.binpb")!!

      assertThat(DataAvailabilityBlobs.isMetadataBlob(blob)).isTrue()
    }

  @Test
  fun `isMetadataBlob returns false for zero-byte metadata-named blob`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      client.writeBlob("edp/m1/2026-03-15/metadata.binpb", ByteString.EMPTY)
      val blob = client.getBlob("edp/m1/2026-03-15/metadata.binpb")!!

      assertThat(DataAvailabilityBlobs.isMetadataBlob(blob)).isFalse()
    }

  @Test
  fun `isMetadataBlob returns false for done blob`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      client.writeBlob("edp/m1/2026-03-15/done", ByteString.copyFromUtf8("done"))
      val blob = client.getBlob("edp/m1/2026-03-15/done")!!

      assertThat(DataAvailabilityBlobs.isMetadataBlob(blob)).isFalse()
    }

  @Test
  fun `isMetadataBlob returns false for non-metadata-named blob`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      client.writeBlob("edp/m1/2026-03-15/data_campaign_1", ByteString.copyFromUtf8("x"))
      val blob = client.getBlob("edp/m1/2026-03-15/data_campaign_1")!!

      assertThat(DataAvailabilityBlobs.isMetadataBlob(blob)).isFalse()
    }

  @Test
  fun `isMetadataBlob is case-insensitive on the metadata substring`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      client.writeBlob("edp/m1/2026-03-15/METADATA_campaign.binpb", ByteString.copyFromUtf8("x"))
      val blob = client.getBlob("edp/m1/2026-03-15/METADATA_campaign.binpb")!!

      assertThat(DataAvailabilityBlobs.isMetadataBlob(blob)).isTrue()
    }

  // -------------------- isSynced --------------------

  @Test
  fun `isSynced returns true when marker matches`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      client.writeBlob("edp/m1/2026-03-15/metadata.binpb", ByteString.copyFromUtf8("x"))
      client.updateBlobMetadata(
        "edp/m1/2026-03-15/metadata.binpb",
        metadata =
          mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE),
      )
      val blob = client.getBlob("edp/m1/2026-03-15/metadata.binpb")!!

      assertThat(DataAvailabilityBlobs.isSynced(blob)).isTrue()
    }

  @Test
  fun `isSynced returns false when marker absent`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      client.writeBlob("edp/m1/2026-03-15/metadata.binpb", ByteString.copyFromUtf8("x"))
      val blob = client.getBlob("edp/m1/2026-03-15/metadata.binpb")!!

      assertThat(DataAvailabilityBlobs.isSynced(blob)).isFalse()
    }

  @Test
  fun `isSynced returns false when marker has wrong value`() =
    runBlocking<Unit> {
      val client = InMemoryStorageClient()
      client.writeBlob("edp/m1/2026-03-15/metadata.binpb", ByteString.copyFromUtf8("x"))
      client.updateBlobMetadata(
        "edp/m1/2026-03-15/metadata.binpb",
        metadata = mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to "some-other-syncer"),
      )
      val blob = client.getBlob("edp/m1/2026-03-15/metadata.binpb")!!

      assertThat(DataAvailabilityBlobs.isSynced(blob)).isFalse()
    }
}
