// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class HighOverlapDataAvailabilityTest {
  @Test
  fun `publishDates waits for publication before notifying the next date in the path`() = runTest {
    withTimeout(5.seconds) {
      val storage = ObservedStorage()
      val publisher = publisher(storage)
      val publication = async {
        publisher.publishDates(mapOf(PATH to linkedSetOf(SECOND_DATE, FIRST_DATE)))
      }
      assertThat(storage.created.receive()).isEqualTo(key(PATH, FIRST_DATE))
      storage.backend.updateBlobMetadata(
        key(PATH, FIRST_DATE),
        metadata =
          mapOf("synced-by" to "data-availability-sync", "data-availability-sync-id" to "attempt-1"),
      )
      repeat(3) { storage.read.receive() }
      assertThat(storage.backend.contents.keys).containsExactly(key(PATH, FIRST_DATE))
      assertThat(publication.isCompleted).isFalse()

      markPublished(storage.backend, key(PATH, FIRST_DATE), "attempt-1")
      assertThat(storage.created.receive()).isEqualTo(key(PATH, SECOND_DATE))
      markPublished(storage.backend, key(PATH, SECOND_DATE))
      publication.await()

      assertThat(storage.backend.contents.keys)
        .containsExactly(key(PATH, FIRST_DATE), key(PATH, SECOND_DATE))
    }
  }

  @Test
  fun `publishDates bounds outstanding notifications across paths`() = runTest {
    withTimeout(5.seconds) {
      val storage = ObservedStorage()
      val paths = (1..8).associate { "edp/provider-$it" to setOf(FIRST_DATE) }
      val publication = async { publisher(storage).publishDates(paths) }
      val first = List(4) { storage.created.receive() }
      assertThat(storage.created.tryReceive().isFailure).isTrue()
      assertThat(storage.backend.contents.keys).containsExactlyElementsIn(first)

      first.forEach { markPublished(storage.backend, it) }
      val second = List(4) { storage.created.receive() }
      assertThat(storage.created.tryReceive().isFailure).isTrue()
      assertThat(first.toSet().intersect(second.toSet())).isEmpty()
      second.forEach { markPublished(storage.backend, it) }
      publication.await()
      assertThat(storage.backend.contents).hasSize(8)
    }
  }

  @Test
  fun `publishDates reports an unprocessed notification and stops publishing more dates`() =
    runTest {
      val storage = ObservedStorage()
      val publisher =
        HighOverlapDataAvailability(storage, MODEL_LINE_ID, 50.milliseconds, 1.milliseconds)
      val exception =
        assertFailsWith<IllegalStateException> {
          publisher.publishDates(mapOf(PATH to setOf(FIRST_DATE, SECOND_DATE)))
        }
      assertThat(exception.message).contains(key(PATH, FIRST_DATE))
      assertThat(storage.backend.contents.keys).containsExactly(key(PATH, FIRST_DATE))
    }

  @Test
  fun `publishDates rejects replacement of the completion blob while waiting`() = runTest {
    supervisorScope {
      val storage = ObservedStorage()
      val publication = async { publisher(storage).publishDates(mapOf(PATH to setOf(FIRST_DATE))) }
      val key = storage.created.receive()
      storage.backend.writeBlob(key, emptyFlow())
      markPublished(storage.backend, key)

      val exception = assertFailsWith<IllegalStateException> { publication.await() }
      assertThat(exception.message).contains("was replaced")
    }
  }

  @Test
  fun `publishDates rejects a preexisting completion blob even if it is published`() = runTest {
    val storage = ObservedStorage()
    storage.backend.writeBlob(key(PATH, FIRST_DATE), emptyFlow())
    markPublished(storage.backend, key(PATH, FIRST_DATE))

    assertFailsWith<BlobChangedException> {
      publisher(storage).publishDates(mapOf(PATH to setOf(FIRST_DATE)))
    }
  }

  @Test
  fun `requirePublished rejects a missing completion blob`() = runTest {
    val publisher = publisher(ObservedStorage())
    val exception =
      assertFailsWith<IllegalStateException> {
        publisher.requirePublished(mapOf(PATH to setOf(FIRST_DATE)))
      }
    assertThat(exception.message).contains(key(PATH, FIRST_DATE))
  }

  @Test
  fun `requirePublished rejects an incomplete or superseded publication`() = runTest {
    val storage = ObservedStorage()
    val key = key(PATH, FIRST_DATE)
    storage.backend.writeBlob(key, emptyFlow())
    val publisher = publisher(storage)
    val dates = mapOf(PATH to setOf(FIRST_DATE))

    storage.backend.updateBlobMetadata(
      key,
      metadata =
        mapOf(
          "synced-by" to "data-availability-sync",
          "data-availability-sync-id" to "new-attempt",
          "data-availability-published-sync-id" to "old-attempt",
        ),
    )
    assertFailsWith<IllegalStateException> { publisher.requirePublished(dates) }

    markPublished(storage.backend, key, "new-attempt")
    publisher.requirePublished(dates)
  }

  @Test
  fun `canceling publication leaves the remaining dates unnotified`() = runTest {
    val storage = ObservedStorage()
    val publication = async {
      publisher(storage).publishDates(mapOf(PATH to setOf(FIRST_DATE, SECOND_DATE)))
    }
    storage.created.receive()
    publication.cancelAndJoin()

    assertThat(storage.backend.contents.keys).containsExactly(key(PATH, FIRST_DATE))
  }

  private fun publisher(storage: ObservedStorage) =
    HighOverlapDataAvailability(storage, MODEL_LINE_ID, 3.seconds, 1.milliseconds)

  private suspend fun markPublished(
    storage: InMemoryStorageClient,
    key: String,
    syncId: String = "attempt",
  ) {
    storage.updateBlobMetadata(
      key,
      metadata =
        mapOf(
          "synced-by" to "data-availability-sync",
          "data-availability-sync-id" to syncId,
          "data-availability-published-sync-id" to syncId,
        ),
    )
  }

  private class ObservedStorage(val backend: InMemoryStorageClient = InMemoryStorageClient()) :
    ConditionalOperationStorageClient by backend {
    val created = Channel<String>(Channel.UNLIMITED)
    val read = Channel<String>(Channel.UNLIMITED)

    override suspend fun writeBlobIfNotFound(
      blobKey: String,
      content: Flow<ByteString>,
    ): StorageClient.Blob {
      val blob = backend.writeBlobIfNotFound(blobKey, content)
      created.send(blobKey)
      return blob
    }

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
      val blob = backend.getBlob(blobKey)
      read.send(blobKey)
      return blob
    }
  }

  companion object {
    private const val MODEL_LINE_ID = "test-line"
    private const val PATH = "edp/test-provider"
    private val FIRST_DATE = LocalDate.of(2026, 4, 1)
    private val SECOND_DATE = FIRST_DATE.plusDays(1)

    private fun key(path: String, date: LocalDate) = "$path/model-line/$MODEL_LINE_ID/$date/done"
  }
}
