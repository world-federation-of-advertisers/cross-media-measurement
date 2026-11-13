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

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException
import com.google.common.truth.Truth.assertThat
import com.google.type.date
import java.time.LocalDate
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.Rule
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.acquireRawImpressionUploadEvictionFenceResponse
import org.wfanet.measurement.edpaggregator.v1alpha.labeledOutputManifest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.EvictUploader

class GcsLabeledOutputEvictionTest {
  private val uploads =
    mockService<RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase>()
  private val modelLines =
    mockService<
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
    >()
  private val files =
    mockService<
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
    >()
  private val metadata =
    mockService<ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase>()

  @get:Rule
  val server = GrpcTestServerRule {
    addService(uploads)
    addService(modelLines)
    addService(metadata)
    addService(files)
  }

  @Test
  fun `eviction overlaps GCS deletes within its limit and preserves each sidecar order`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val deleted = Collections.synchronizedList(mutableListOf<String>())
      val firstWave = CompletableDeferred<Unit>()
      val overflow = CompletableDeferred<Unit>()
      val release = CountDownLatch(1)
      val started = AtomicInteger()
      val active = AtomicInteger()
      val maximum = AtomicInteger()
      whenever(storage.delete(any<BlobId>())).thenAnswer { invocation ->
        val id = invocation.getArgument<BlobId>(0)
        assertThat(id.generation).isEqualTo(42L)
        val count = active.incrementAndGet()
        maximum.updateAndGet { maxOf(it, count) }
        try {
          val requestNumber = started.incrementAndGet()
          if (requestNumber == 16) firstWave.complete(Unit)
          if (requestNumber == 17) overflow.complete(Unit)
          check(release.await(10, TimeUnit.SECONDS)) { "Eviction deletes were not released" }
          deleted.add(id.name)
          true
        } finally {
          active.decrementAndGet()
        }
      }
      val evictor = evictor(storage)

      val eviction = async { evictor.evict(plan(20), "bad inputs") {} }
      try {
        withTimeout(5_000) { firstWave.await() }
        assertThat(active.get()).isEqualTo(16)
        assertThat(withTimeoutOrNull(200) { overflow.await() }).isNull()
        assertThat(started.get()).isEqualTo(16)
      } finally {
        release.countDown()
      }
      val result = eviction.await()

      assertThat(result.deletedOutputBlobs).isEqualTo(40)
      assertThat(maximum.get()).isGreaterThan(1)
      assertThat(maximum.get()).isAtMost(16)
      for (index in 0 until 20) {
        assertThat(deleted.indexOf("vid/file$index.metadata.binpb"))
          .isLessThan(deleted.indexOf("vid/file$index"))
      }
    }

  @Test
  fun `failed sidecar deletion leaves its data object and checkpoint untouched`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val failure = StorageException(503, "sidecar unavailable")
      whenever(storage.delete(BlobId.of("output", "vid/file0.metadata.binpb", 42L)))
        .thenThrow(failure)
      var checkpointed = false
      val evictor = evictor(storage)

      assertThat(
          assertFailsWith<StorageException> {
            evictor.evict(plan(1), "bad inputs") { checkpointed = true }
          }
        )
        .isSameInstanceAs(failure)

      assertThat(checkpointed).isFalse()
      verify(storage, never()).delete(BlobId.of("output", "vid/file0", 42L))
    }

  @Test
  fun `output generation capture overlaps on IO within its limit and finalizes every version`() =
    runBlocking<Unit> {
      val storage = mock<Storage>()
      val evictor = evictor(storage)
      val firstWave = CompletableDeferred<Unit>()
      val overflow = CompletableDeferred<Unit>()
      val release = CountDownLatch(1)
      val started = AtomicInteger()
      val active = AtomicInteger()
      val maximum = AtomicInteger()
      val eventDate = LocalDate.of(2026, 11, 1)
      val rawUris = (0 until 10).map { "gs://raw/day/input$it.parquet" }
      whenever(files.listRawImpressionUploadFiles(any()))
        .thenReturn(
          listRawImpressionUploadFilesResponse {
            rawImpressionUploadFiles +=
              rawUris.map { uri ->
                rawImpressionUploadFile {
                  blobUri = uri
                  this.eventDate = date {
                    year = 2026
                    month = 11
                    day = 1
                  }
                }
              }
          }
        )
      val outputUris =
        rawUris
          .map { uri ->
            LabeledImpressionsBlobKeys.forInputUri("gs://output/vid", uri, MODEL_LINE, eventDate)
          }
          .sorted()
          .flatMap { outputUri -> listOf("$outputUri.metadata.binpb", outputUri) }
      val provisional =
        plan(1).let { initial ->
          initial.copy(
            cascade =
              listOf(
                initial.cascade
                  .single()
                  .copy(
                    labeledOutputManifest =
                      labeledOutputManifest {
                        blobs +=
                          outputUris.map { uri ->
                            LabeledOutputManifestKt.blobVersion { blobUri = uri }
                          }
                      }
                  )
              ),
            outputManifestsFinalized = false,
          )
        }
      whenever(storage.get(any<BlobId>())).thenAnswer { invocation ->
        val count = active.incrementAndGet()
        maximum.updateAndGet { maxOf(it, count) }
        try {
          val requestNumber = started.incrementAndGet()
          if (requestNumber == 16) firstWave.complete(Unit)
          if (requestNumber == 17) overflow.complete(Unit)
          check(release.await(10, TimeUnit.SECONDS)) { "Generation reads were not released" }
          mock<Blob>().also {
            whenever(it.name).thenReturn(invocation.getArgument<BlobId>(0).name)
            whenever(it.generation).thenReturn(42L)
          }
        } finally {
          active.decrementAndGet()
        }
      }

      val capture = async { evictor.finalizeOutputManifests(provisional) }
      try {
        withTimeout(5_000) { firstWave.await() }
        assertThat(active.get()).isEqualTo(16)
        assertThat(withTimeoutOrNull(200) { overflow.await() }).isNull()
        assertThat(started.get()).isEqualTo(16)
      } finally {
        release.countDown()
      }
      val finalized = capture.await()

      assertThat(finalized.outputManifestsFinalized).isTrue()
      assertThat(started.get()).isEqualTo(20)
      assertThat(maximum.get()).isGreaterThan(1)
      assertThat(maximum.get()).isAtMost(16)
      assertThat(finalized.cascade.single().labeledOutputManifest.blobsList)
        .containsExactlyElementsIn(
          outputUris.map { uri ->
            LabeledOutputManifestKt.blobVersion {
              blobUri = uri
              generation = 42L
            }
          }
        )
      verify(storage, never()).delete(any<BlobId>())
    }

  private suspend fun evictor(storage: Storage): EvictUploader {
    whenever(storage.get(any<BlobId>())).thenAnswer { invocation ->
      mock<Blob>().also {
        whenever(it.name).thenReturn(invocation.getArgument<BlobId>(0).name)
        whenever(it.generation).thenReturn(42L)
      }
    }
    whenever(uploads.acquireRawImpressionUploadEvictionFence(any()))
      .thenReturn(acquireRawImpressionUploadEvictionFenceResponse {})
    whenever(modelLines.getRawImpressionUploadModelLine(any()))
      .thenReturn(
        rawImpressionUploadModelLine {
          name = ROW
          state = RawImpressionUploadModelLine.State.COMPLETED
          etag = "etag"
        }
      )
    whenever(modelLines.markRawImpressionUploadModelLineFailed(any()))
      .thenReturn(rawImpressionUploadModelLine {})
    whenever(metadata.listImpressionMetadata(any())).thenReturn(listImpressionMetadataResponse {})
    val store = GcsLabeledOutputStore(storage)
    return EvictUploader(
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(server.channel),
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        server.channel
      ),
      RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(server.channel),
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
        server.channel
      ),
      ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(server.channel),
      "gs://output/vid",
      store::getGeneration,
      store::delete,
      rpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
    )
  }

  private fun plan(pairCount: Int): EvictUploader.EvictionPlan =
    EvictUploader.EvictionPlan(
      cascade =
        listOf(
          EvictUploader.CascadeEntry(
            UPLOAD,
            ROW,
            MODEL_LINE,
            memoized = false,
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION,
            recoveryPredecessorUploadName = "",
            labeledOutputManifest =
              labeledOutputManifest {
                for (index in 0 until pairCount) {
                  for (suffix in listOf(".metadata.binpb", "")) {
                    blobs +=
                      LabeledOutputManifestKt.blobVersion {
                        blobUri = "gs://output/vid/file$index$suffix"
                        generation = 42L
                      }
                  }
                }
              },
          )
        ),
      extraUploads = emptyList(),
      memoizedModelLines = emptySet(),
      nonMemoizedModelLines = setOf(MODEL_LINE),
      badUploads = listOf(UPLOAD),
      evictionOperationId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      recoveryTargets = emptyList(),
    )

  companion object {
    private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/upload"
    private const val ROW = "$UPLOAD/rawImpressionUploadModelLines/row"
    private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
  }
}
