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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.api.gax.paging.Page
import com.google.cloud.functions.HttpRequest
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException
import com.google.common.truth.Truth.assertThat
import com.google.type.date
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.DoneBlobReplayer
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs

@RunWith(JUnit4::class)
class VidLabelingHealingControllerFunctionTest {
  @Test
  fun `service runs controller and returns no content`() {
    val invoked = CompletableDeferred<Unit>()
    val response = mock<com.google.cloud.functions.HttpResponse>()
    val function = VidLabelingHealingControllerFunction { invoked.complete(Unit) }

    function.service(httpRequest(), response)

    assertThat(invoked.isCompleted).isTrue()
    verify(response).setStatusCode(204)
  }

  @Test
  fun `service starts trace context for scheduled invocation across suspension`() {
    val response = mock<com.google.cloud.functions.HttpResponse>()
    val callerContext = Tracing.currentW3CTraceContext()
    var beforeSuspension: Map<String, String> = emptyMap()
    var afterSuspension: Map<String, String> = emptyMap()
    val function = VidLabelingHealingControllerFunction {
      beforeSuspension = Tracing.currentW3CTraceContext()
      withContext(Dispatchers.Default) {
        yield()
        afterSuspension = Tracing.currentW3CTraceContext()
      }
    }

    function.service(httpRequest(), response)

    assertThat(beforeSuspension).containsKey("traceparent")
    assertThat(afterSuspension).isEqualTo(beforeSuspension)
    assertThat(Tracing.currentW3CTraceContext()).isEqualTo(callerContext)
    verify(response).setStatusCode(204)
  }

  @Test
  fun `service preserves incoming trace context across suspension`() {
    val incomingTraceId = "11111111111111111111111111111111"
    val incomingParent = "00-$incomingTraceId-2222222222222222-01"
    val response = mock<com.google.cloud.functions.HttpResponse>()
    val callerContext = Tracing.currentW3CTraceContext()
    var beforeSuspension: Map<String, String> = emptyMap()
    var afterSuspension: Map<String, String> = emptyMap()
    val function = VidLabelingHealingControllerFunction {
      beforeSuspension = Tracing.currentW3CTraceContext()
      withContext(Dispatchers.Default) {
        yield()
        afterSuspension = Tracing.currentW3CTraceContext()
      }
    }

    function.service(
      httpRequest(
        mapOf("traceparent" to listOf(incomingParent), "tracestate" to listOf("test=value"))
      ),
      response,
    )

    assertThat(beforeSuspension).containsKey("traceparent")
    assertThat(beforeSuspension.getValue("traceparent")).startsWith("00-$incomingTraceId-")
    assertThat(beforeSuspension.getValue("traceparent")).isNotEqualTo(incomingParent)
    assertThat(beforeSuspension).containsEntry("tracestate", "test=value")
    assertThat(afterSuspension).isEqualTo(beforeSuspension)
    assertThat(Tracing.currentW3CTraceContext()).isEqualTo(callerContext)
    verify(response).setStatusCode(204)
  }

  @Test
  fun `service restores caller trace context when controller fails`() {
    val response = mock<com.google.cloud.functions.HttpResponse>()
    val callerContext = Tracing.currentW3CTraceContext()
    val failure = IllegalStateException("Controller failed")
    var controllerContext: Map<String, String> = emptyMap()
    val function = VidLabelingHealingControllerFunction {
      withContext(Dispatchers.Default) {
        yield()
        controllerContext = Tracing.currentW3CTraceContext()
        throw failure
      }
    }

    assertFailsWith<IllegalStateException> { function.service(httpRequest(), response) }

    assertThat(controllerContext).containsKey("traceparent")
    assertThat(Tracing.currentW3CTraceContext()).isEqualTo(callerContext)
    verify(response, never()).setStatusCode(204)
  }

  @Test
  fun `manifest reader returns live objects for exact done generation`() {
    runBlocking {
      val storage = mock<Storage>()
      val done = blob("day/done", 11L)
      val first = blob("day/first.avro", 21L)
      val second = blob("day/second.avro", 22L)
      val nestedDone = blob("day/backfill/DONE", 23L)
      val page = mock<Page<Blob>>()
      whenever(storage.get("raw", "day/done")).thenReturn(done)
      whenever(storage.list(eq("raw"), any<Storage.BlobListOption>())).thenReturn(page)
      whenever(page.iterateAll()).thenReturn(listOf(first, done, second, nestedDone))

      val manifest =
        GcsCorrectionManifestReader(storage).read("gs://raw/day/done", 11L, emptyList())

      assertThat(manifest)
        .containsExactly(
          RawImpressionUploadManifestClassifier.File("gs://raw/day/first.avro", 21L),
          RawImpressionUploadManifestClassifier.File("gs://raw/day/second.avro", 22L),
        )
    }
  }

  @Test
  fun `manifest reader preserves persisted dates for exact object generations`() = runBlocking {
    val storage = mock<Storage>()
    val done = blob("day/done", 11L)
    val input = blob("day/input.parquet", 21L)
    val page = mock<Page<Blob>>()
    whenever(storage.get("raw", "day/done")).thenReturn(done)
    whenever(storage.list(eq("raw"), any<Storage.BlobListOption>())).thenReturn(page)
    whenever(page.iterateAll()).thenReturn(listOf(input, done))
    val persisted =
      listOf(
        RawImpressionUploadManifestClassifier.File(
          "gs://raw/day/input.parquet",
          21L,
          date {
            year = 2026
            month = 9
            day = 1
          },
        )
      )

    val live = GcsCorrectionManifestReader(storage).read("gs://raw/day/done", 11L, persisted)

    val classifier = RawImpressionUploadManifestClassifier()
    assertThat(live).containsExactlyElementsIn(persisted)
    assertThat(classifier.digest(live)).isEqualTo(classifier.digest(persisted))
  }

  @Test
  fun `manifest reader rejects changed done generation`() {
    val storage = mock<Storage>()
    val done = blob("day/done", 12L)
    whenever(storage.get("raw", "day/done")).thenReturn(done)

    val error =
      kotlin.test.assertFailsWith<IllegalStateException> {
        runBlocking {
          GcsCorrectionManifestReader(storage).read("gs://raw/day/done", 11L, emptyList())
        }
      }

    assertThat(error).hasMessageThat().contains("generation changed")
  }

  @Test
  fun `manifest reader rejects done rewrite during listing`() {
    val storage = mock<Storage>()
    val original = blob("day/done", 11L)
    val rewritten = blob("day/done", 12L)
    val page = mock<Page<Blob>>()
    whenever(storage.get("raw", "day/done")).thenReturn(original, rewritten)
    whenever(storage.list(eq("raw"), any<Storage.BlobListOption>())).thenReturn(page)
    whenever(page.iterateAll()).thenReturn(emptyList())

    val error =
      kotlin.test.assertFailsWith<IllegalStateException> {
        runBlocking {
          GcsCorrectionManifestReader(storage).read("gs://raw/day/done", 11L, emptyList())
        }
      }

    assertThat(error).hasMessageThat().contains("generation changed")
  }

  @Test
  fun `labeled output store deletes only the requested generation`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val live = blob("day/output.avro", 42L)
      val unqualified = BlobId.of("output", "day/output.avro")
      val qualified = BlobId.of("output", "day/output.avro", 42L)
      whenever(storage.get(unqualified)).thenReturn(live)
      whenever(storage.delete(qualified)).thenReturn(true)
      val outputStore = GcsLabeledOutputStore(storage)

      assertThat(outputStore.getGeneration("gs://output/day/output.avro")).isEqualTo(42L)
      assertThat(outputStore.delete("gs://output/day/output.avro", 42L)).isTrue()

      verify(storage).delete(qualified)
      verify(storage, never()).delete(unqualified)
    }

  @Test
  fun `done object metadata reader returns exact generation and create time`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val done = blob("model-line/ml/2026-11-01/done", 42L)
      val createTime = Instant.parse("2026-11-01T00:00:00Z")
      whenever(done.createTimeOffsetDateTime).thenReturn(createTime.atOffset(ZoneOffset.UTC))
      whenever(storage.get(BlobId.of("output", done.name))).thenReturn(done)

      val metadata =
        GcsDoneObjectMetadataReader(storage).read("gs://output/model-line/ml/2026-11-01/done")

      assertThat(metadata!!.generation).isEqualTo(42L)
      assertThat(metadata.createTime).isEqualTo(createTime)
    }

  @Test
  fun `recovery writer creates a fresh generation with recovery metadata`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val current = blob("day/done", 11L)
      val rewritten = blob("day/done", 12L)
      whenever(current.metadata).thenReturn(mapOf("existing" to "value"))
      whenever(storage.get(BlobId.of("raw", "day/done"))).thenReturn(current)
      whenever(storage.create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>()))
        .thenReturn(rewritten)

      val generation =
        GcsDoneBlobRecoveryWriter(storage)
          .rewrite("gs://raw/day/done", 11L, mapOf("recovery" to "operation"))

      assertThat(generation).isEqualTo(12L)
      val blobInfo = argumentCaptor<BlobInfo>()
      verify(storage).create(blobInfo.capture(), any<ByteArray>(), any<Storage.BlobTargetOption>())
      assertThat(blobInfo.firstValue.metadata)
        .containsExactly("existing", "value", "recovery", "operation")
    }

  @Test
  fun `recovery writer reuses a matching recovery generation`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val current = blob("day/done", 12L)
      whenever(current.metadata).thenReturn(mapOf("recovery" to "operation"))
      whenever(storage.get(BlobId.of("raw", "day/done"))).thenReturn(current)

      val generation =
        GcsDoneBlobRecoveryWriter(storage)
          .rewrite("gs://raw/day/done", 11L, mapOf("recovery" to "operation"))

      assertThat(generation).isEqualTo(12L)
      verify(storage, never())
        .create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>())
    }

  @Test
  fun `done replayer routes exact recovery metadata`() {
    runBlocking {
      val dataWatcher = mock<DataWatcher>()
      val request =
        DoneBlobReplayer.Request(
          doneBlobUri = "gs://raw/day/done",
          doneBlobGeneration = 11L,
          sourceRawImpressionUpload = "dataProviders/dp/rawImpressionUploads/source",
          cmmsModelLines = listOf("modelLines/first", "modelLines/second"),
          uploadHealingOperation =
            "dataProviders/dp/uploadHealingOperations/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        )

      DataWatcherDoneBlobReplayer(dataWatcher).replay(request)

      verify(dataWatcher)
        .receivePath(
          "gs://raw/day/done",
          mapOf(
            DataWatcher.GENERATION_METADATA_KEY to "11",
            WatchedBlobs.OVERRIDE_MODEL_LINES_KEY to "modelLines/first,modelLines/second",
            WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY to
              "dataProviders/dp/rawImpressionUploads/source",
            WatchedBlobs.EVICTION_OPERATION_ID_KEY to "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
          ),
        )
    }
  }

  @Test
  fun `generation reads overlap on IO instead of blocking the caller`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val entered = CountDownLatch(2)
      val callerThread = Thread.currentThread()
      whenever(storage.get(any<BlobId>())).thenAnswer { invocation ->
        assertThat(Thread.currentThread()).isNotSameInstanceAs(callerThread)
        entered.countDown()
        check(entered.await(5, TimeUnit.SECONDS)) { "Generation reads did not overlap" }
        blob(invocation.getArgument<BlobId>(0).name, 42L)
      }
      val store = GcsLabeledOutputStore(storage)

      val generations = coroutineScope {
        listOf("first", "second")
          .map { key -> async { store.getGeneration("gs://output/$key") } }
          .awaitAll()
      }

      assertThat(generations).containsExactly(42L, 42L)
    }

  @Test
  fun `exact generation deletes overlap on IO instead of blocking the caller`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val entered = CountDownLatch(2)
      val callerThread = Thread.currentThread()
      whenever(storage.delete(any<BlobId>())).thenAnswer { invocation ->
        assertThat(Thread.currentThread()).isNotSameInstanceAs(callerThread)
        assertThat(invocation.getArgument<BlobId>(0).generation).isEqualTo(42L)
        entered.countDown()
        check(entered.await(5, TimeUnit.SECONDS)) { "Output deletes did not overlap" }
        true
      }
      val store = GcsLabeledOutputStore(storage)

      val deleted = coroutineScope {
        listOf("first", "second")
          .map { key -> async { store.delete("gs://output/$key", 42L) } }
          .awaitAll()
      }

      assertThat(deleted).containsExactly(true, true)
    }

  @Test
  fun `output store reports absent objects without substituting a version`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val store = GcsLabeledOutputStore(storage)

      assertThat(store.getGeneration("gs://output/missing")).isNull()
      assertThat(store.delete("gs://output/missing", 42L)).isFalse()
      verify(storage).delete(BlobId.of("output", "missing", 42L))
    }

  @Test
  fun `output store propagates read and delete failures`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val failure = StorageException(503, "unavailable")
      whenever(storage.get(any<BlobId>())).thenThrow(failure)
      whenever(storage.delete(any<BlobId>())).thenThrow(failure)
      val store = GcsLabeledOutputStore(storage)

      assertThat(assertFailsWith<StorageException> { store.getGeneration("gs://output/file") })
        .isSameInstanceAs(failure)
      assertThat(assertFailsWith<StorageException> { store.delete("gs://output/file", 42L) })
        .isSameInstanceAs(failure)
    }

  @Test
  fun `cancelled output operations never issue storage requests`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val store = GcsLabeledOutputStore(storage)
      val job = launch {
        yield()
        store.getGeneration("gs://output/file")
        store.delete("gs://output/file", 42L)
      }
      job.cancel()
      job.join()

      verify(storage, never()).get(any<BlobId>())
      verify(storage, never()).delete(any<BlobId>())
    }

  @Test
  fun `recovery reads and conditional writes run away from the caller thread`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val callerThread = Thread.currentThread()
      val current = blob("day/done", 11L)
      val rewritten = blob("day/done", 12L)
      val calls = AtomicInteger()
      whenever(storage.get(BlobId.of("raw", "day/done"))).thenAnswer {
        assertThat(Thread.currentThread()).isNotSameInstanceAs(callerThread)
        calls.incrementAndGet()
        current
      }
      whenever(storage.create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>()))
        .thenAnswer {
          assertThat(Thread.currentThread()).isNotSameInstanceAs(callerThread)
          calls.incrementAndGet()
          rewritten
        }

      assertThat(GcsDoneBlobRecoveryWriter(storage).rewrite("gs://raw/day/done", 11L, emptyMap()))
        .isEqualTo(12L)
      assertThat(calls.get()).isEqualTo(2)
      verify(storage)
        .create(
          any<BlobInfo>(),
          eq(ByteArray(0)),
          eq(Storage.BlobTargetOption.generationMatch(11L)),
        )
    }

  @Test
  fun `recovery rejects a missing marker without creating one`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()

      assertFailsWith<IllegalStateException> {
        GcsDoneBlobRecoveryWriter(storage).rewrite("gs://raw/day/done", 11L, emptyMap())
      }

      verify(storage, never())
        .create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>())
    }

  @Test
  fun `recovery rejects a newer marker for another operation`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val current = blob("day/done", 12L)
      whenever(current.metadata).thenReturn(mapOf("recovery" to "other"))
      whenever(storage.get(BlobId.of("raw", "day/done"))).thenReturn(current)

      val failure =
        assertFailsWith<IllegalStateException> {
          GcsDoneBlobRecoveryWriter(storage)
            .rewrite("gs://raw/day/done", 11L, mapOf("recovery" to "ours"))
        }

      assertThat(failure).hasMessageThat().contains("not 11")
      verify(storage, never())
        .create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>())
    }

  @Test
  fun `recovery propagates a generation match rejection`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val current = blob("day/done", 11L)
      whenever(storage.get(BlobId.of("raw", "day/done"))).thenReturn(current)
      val failure = StorageException(412, "generation changed during write")
      whenever(storage.create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>()))
        .thenThrow(failure)

      assertThat(
          assertFailsWith<StorageException> {
            GcsDoneBlobRecoveryWriter(storage).rewrite("gs://raw/day/done", 11L, emptyMap())
          }
        )
        .isSameInstanceAs(failure)
    }

  @Test
  fun `recovery propagates a marker read failure without writing`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val failure = StorageException(503, "unavailable")
      whenever(storage.get(any<BlobId>())).thenThrow(failure)

      assertThat(
          assertFailsWith<StorageException> {
            GcsDoneBlobRecoveryWriter(storage).rewrite("gs://raw/day/done", 11L, emptyMap())
          }
        )
        .isSameInstanceAs(failure)
      verify(storage, never())
        .create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>())
    }

  @Test
  fun `cancellation during recovery marker read prevents the conditional write`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val entered = CompletableDeferred<Unit>()
      val release = CountDownLatch(1)
      val current = blob("day/done", 11L)
      whenever(storage.get(BlobId.of("raw", "day/done"))).thenAnswer {
        entered.complete(Unit)
        check(release.await(10, TimeUnit.SECONDS)) { "Marker read was not released" }
        current
      }
      val recovery = async {
        GcsDoneBlobRecoveryWriter(storage)
          .rewrite("gs://raw/day/done", 11L, mapOf("recovery" to "ours"))
      }
      try {
        withTimeout(5_000) { entered.await() }
        recovery.cancel()
      } finally {
        release.countDown()
      }
      assertFailsWith<CancellationException> { recovery.await() }

      verify(storage, never())
        .create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>())
    }

  private fun httpRequest(headers: Map<String, List<String>> = emptyMap()): HttpRequest {
    return mock<HttpRequest>().also { whenever(it.headers).thenReturn(headers) }
  }

  private fun blob(name: String, generation: Long): Blob {
    return mock<Blob>().also {
      whenever(it.name).thenReturn(name)
      whenever(it.generation).thenReturn(generation)
    }
  }

  @Test
  fun `manifest reader verifies retained versions without listing newer directory contents`() =
    runBlocking {
      val storage = mock<Storage>()
      val marker = blob("day/done", 11L)
      val input = blob("day/input.parquet", 21L)
      val createTime = Instant.parse("2026-11-01T00:00:00Z")
      whenever(marker.createTimeOffsetDateTime).thenReturn(createTime.atOffset(ZoneOffset.UTC))
      whenever(input.createTimeOffsetDateTime)
        .thenReturn(createTime.minusSeconds(1).atOffset(ZoneOffset.UTC))
      whenever(storage.get(BlobId.of("raw", "day/done", 11L))).thenReturn(marker)
      whenever(storage.get(BlobId.of("raw", "day/input.parquet", 21L))).thenReturn(input)
      val expected =
        listOf(RawImpressionUploadManifestClassifier.File("gs://raw/day/input.parquet", 21L))

      val result =
        GcsCorrectionManifestReader(storage).readPersisted("gs://raw/day/done", 11L, expected)

      assertThat(result).containsExactlyElementsIn(expected)
      verify(storage, never()).get("raw", "day/done")
      verify(storage, never()).list(any<String>(), any<Storage.BlobListOption>())
      Unit
    }

  @Test
  fun `manifest reader rejects a missing retained input instead of reading its live version`() =
    runBlocking {
      val storage = mock<Storage>()
      val marker = blob("day/done", 11L)
      whenever(marker.createTimeOffsetDateTime)
        .thenReturn(Instant.parse("2026-11-01T00:00:00Z").atOffset(ZoneOffset.UTC))
      whenever(storage.get(BlobId.of("raw", "day/done", 11L))).thenReturn(marker)

      val error =
        kotlin.test.assertFailsWith<IllegalStateException> {
          GcsCorrectionManifestReader(storage)
            .readPersisted(
              "gs://raw/day/done",
              11L,
              listOf(RawImpressionUploadManifestClassifier.File("gs://raw/day/input.parquet", 21L)),
            )
        }

      assertThat(error).hasMessageThat().contains("approved raw-object version")
      verify(storage, never()).get("raw", "day/input.parquet")
      Unit
    }
}
