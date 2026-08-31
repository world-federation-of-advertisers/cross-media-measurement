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

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine

@RunWith(JUnit4::class)
class EvictUploaderTest {
  private val uploadService:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase =
    mockService()
  private val modelLineService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()
  private val rankIndexBlobService:
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(uploadService)
    addService(modelLineService)
    addService(rankIndexBlobService)
  }

  private val evictUploader: EvictUploader by lazy {
    val channel = grpcTestServerRule.channel
    EvictUploader(
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(channel),
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        channel
      ),
      RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(channel),
    )
  }

  private fun uploadName(id: String) = "$DATA_PROVIDER/rawImpressionUploads/$id"

  private fun modelLineName(uploadId: String) =
    "${uploadName(uploadId)}/rawImpressionUploadModelLines/rml"

  private fun snapshotName(uploadId: String) = "${uploadName(uploadId)}/rankIndexBlobs/snapshot"

  private suspend fun stubModelLineRows(vararg uploadIds: String) {
    whenever(modelLineService.listRawImpressionUploadModelLines(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadModelLinesRequest>(0)
      val selectedIds =
        if (request.parent.endsWith("/rawImpressionUploads/-")) uploadIds.toList()
        else uploadIds.filter { uploadName(it) == request.parent }
      listRawImpressionUploadModelLinesResponse {
        for (id in selectedIds) {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = modelLineName(id)
            cmmsModelLine = MODEL_LINE
            etag = "etag-$id"
            state = RawImpressionUploadModelLine.State.COMPLETED
          }
        }
      }
    }
  }

  private suspend fun stubSnapshotRows(vararg uploadIds: String) {
    whenever(rankIndexBlobService.listRankIndexBlobs(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRankIndexBlobsRequest>(0)
      val selectedIds =
        if (request.parent.endsWith("/rawImpressionUploads/-")) uploadIds.toList()
        else uploadIds.filter { uploadName(it) == request.parent }
      listRankIndexBlobsResponse {
        for (id in selectedIds) {
          rankIndexBlobs += rankIndexBlob {
            name = snapshotName(id)
            blobType = RankIndexBlob.BlobType.SNAPSHOT
            cmmsModelLine = MODEL_LINE
          }
        }
      }
    }
  }

  @Test
  fun `plan builds a forward cascade and evict fails them and soft-deletes snapshots`() {
    val planAndResult = runBlocking {
      whenever(uploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName("up1")
              createTime = T1.toProtoTime()
            }
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName("up2")
              createTime = T2.toProtoTime()
            }
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName("up3")
              createTime = T3.toProtoTime()
            }
          }
        )
      stubModelLineRows("up1", "up2", "up3")
      whenever(modelLineService.markRawImpressionUploadModelLineFailed(any())).thenAnswer {
        rawImpressionUploadModelLine { state = RawImpressionUploadModelLine.State.FAILED }
      }
      stubSnapshotRows("up1", "up2", "up3")
      whenever(rankIndexBlobService.deleteRankIndexBlob(any())).thenAnswer { rankIndexBlob {} }

      val plan = evictUploader.plan(listOf(uploadName("up2")), cutoffTime = T0)
      val result = evictUploader.evict(plan, REASON)
      plan to result
    }

    val (plan, result) = planAndResult
    // Cascade starts at the earliest bad upload (up2) and includes everything after it (up3).
    assertThat(plan.cascade.map { it.uploadName })
      .containsExactly(uploadName("up2"), uploadName("up3"))
      .inOrder()
    assertThat(plan.extraUploads).containsExactly(uploadName("up3"))
    assertThat(plan.memoizedModelLines).containsExactly(MODEL_LINE)
    assertThat(plan.nonMemoizedModelLines).isEmpty()
    // Both cascade model lines are failed and each has its SNAPSHOT soft-deleted.
    assertThat(result.failedModelLines).containsExactly(modelLineName("up2"), modelLineName("up3"))
    assertThat(result.deletedSnapshots).isEqualTo(2)
    val requestCaptor = argumentCaptor<MarkRawImpressionUploadModelLineFailedRequest>()
    verifyBlocking(modelLineService, times(2)) {
      markRawImpressionUploadModelLineFailed(requestCaptor.capture())
    }
    for (request in requestCaptor.allValues) {
      assertThat(request.requestId).isNotEmpty()
    }
  }

  @Test
  fun `plan throws when a bad upload is outside the retention window`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        runBlocking {
          whenever(uploadService.listRawImpressionUploads(any()))
            .thenReturn(
              listRawImpressionUploadsResponse {
                rawImpressionUploads += rawImpressionUpload {
                  name = uploadName("up1")
                  createTime = T1.toProtoTime()
                }
              }
            )
          // Cutoff is after up1's create time, so up1 is out of the retention window.
          evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T2)
        }
      }
    assertThat(error).hasMessageThat().contains("retention window")
  }

  @Test
  fun `plan throws when a requested upload has no model-line rows`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        runBlocking {
          whenever(uploadService.listRawImpressionUploads(any()))
            .thenReturn(
              listRawImpressionUploadsResponse {
                rawImpressionUploads += rawImpressionUpload {
                  name = uploadName("up1")
                  createTime = T1.toProtoTime()
                }
                rawImpressionUploads += rawImpressionUpload {
                  name = uploadName("up2")
                  createTime = T2.toProtoTime()
                }
              }
            )
          // Only up1 has a row for MODEL_LINE; up2 is a valid, in-window upload with none.
          stubModelLineRows("up1")
          evictUploader.plan(listOf(uploadName("up1"), uploadName("up2")), cutoffTime = T0)
        }
      }
    assertThat(error).hasMessageThat().contains("no model-line rows")
  }

  @Test
  fun `plan rejects bad uploads spanning multiple DataProviders`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        runBlocking {
          evictUploader.plan(
            listOf(uploadName("up1"), "dataProviders/dp2/rawImpressionUploads/up2"),
            cutoffTime = T0,
          )
        }
      }
    assertThat(error).hasMessageThat().contains("same DataProvider")
  }

  @Test
  fun `evict skips an already-FAILED model line but still soft-deletes its snapshots`() {
    val result = runBlocking {
      whenever(uploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName("up1")
              createTime = T1.toProtoTime()
            }
          }
        )
      stubModelLineRows("up1")
      // evict() re-fetches current state before marking; it is already FAILED, so Mark is skipped.
      whenever(modelLineService.getRawImpressionUploadModelLine(any()))
        .thenReturn(
          rawImpressionUploadModelLine {
            name = modelLineName("up1")
            cmmsModelLine = MODEL_LINE
            state = RawImpressionUploadModelLine.State.FAILED
          }
        )
      stubSnapshotRows("up1")
      whenever(rankIndexBlobService.deleteRankIndexBlob(any())).thenAnswer { rankIndexBlob {} }

      val plan = evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
      evictUploader.evict(plan, REASON)
    }

    assertThat(result.failedModelLines).isEmpty()
    assertThat(result.deletedSnapshots).isEqualTo(1)
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineFailed(any()) }
  }

  @Test
  fun `non-memoized model line evicts only requested upload`(): Unit = runBlocking {
    whenever(uploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads += rawImpressionUpload {
            name = uploadName("up1")
            createTime = T1.toProtoTime()
          }
          rawImpressionUploads += rawImpressionUpload {
            name = uploadName("up2")
            createTime = T2.toProtoTime()
          }
        }
      )
    stubModelLineRows("up1", "up2")
    whenever(rankIndexBlobService.listRankIndexBlobs(any()))
      .thenReturn(listRankIndexBlobsResponse {})
    whenever(modelLineService.getRawImpressionUploadModelLine(any()))
      .thenReturn(
        rawImpressionUploadModelLine {
          name = modelLineName("up1")
          cmmsModelLine = MODEL_LINE
          state = RawImpressionUploadModelLine.State.COMPLETED
          etag = "etag-up1"
        }
      )
    whenever(modelLineService.markRawImpressionUploadModelLineFailed(any()))
      .thenReturn(
        rawImpressionUploadModelLine { state = RawImpressionUploadModelLine.State.FAILED }
      )

    val plan = evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
    val result = evictUploader.evict(plan, REASON)

    assertThat(plan.cascade.map { it.uploadName }).containsExactly(uploadName("up1"))
    assertThat(plan.memoizedModelLines).isEmpty()
    assertThat(plan.nonMemoizedModelLines).containsExactly(MODEL_LINE)
    assertThat(result.deletedSnapshots).isEqualTo(0)
  }

  @Test
  fun `memoized cascade excludes later non-memoized row for same model line`(): Unit = runBlocking {
    whenever(uploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads += rawImpressionUpload {
            name = uploadName("up1")
            createTime = T1.toProtoTime()
          }
          rawImpressionUploads += rawImpressionUpload {
            name = uploadName("up2")
            createTime = T2.toProtoTime()
          }
        }
      )
    stubModelLineRows("up1", "up2")
    stubSnapshotRows("up1")

    val plan = evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)

    assertThat(plan.cascade.map { it.uploadName }).containsExactly(uploadName("up1"))
  }

  @Test
  fun `plan rejects affected in-progress work`() = runBlocking {
    whenever(uploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads += rawImpressionUpload {
            name = uploadName("up1")
            createTime = T1.toProtoTime()
          }
        }
      )
    whenever(modelLineService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = modelLineName("up1")
            cmmsModelLine = MODEL_LINE
            state = RawImpressionUploadModelLine.State.RANKING
          }
        }
      )

    val error =
      assertFailsWith<IllegalArgumentException> {
        evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
      }

    assertThat(error).hasMessageThat().contains("drain or cancel")
  }

  @Test
  fun `evict aborts when memoized upload is registered after confirmation`(): Unit = runBlocking {
    var includeLaterUpload = false
    whenever(uploadService.listRawImpressionUploads(any())).thenAnswer {
      listRawImpressionUploadsResponse {
        rawImpressionUploads += rawImpressionUpload {
          name = uploadName("up1")
          createTime = T1.toProtoTime()
        }
        if (includeLaterUpload) {
          rawImpressionUploads += rawImpressionUpload {
            name = uploadName("up2")
            createTime = T2.toProtoTime()
          }
        }
      }
    }
    whenever(modelLineService.listRawImpressionUploadModelLines(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadModelLinesRequest>(0)
      val ids = mutableListOf("up1")
      if (includeLaterUpload) ids += "up2"
      val selected =
        if (request.parent.endsWith("/rawImpressionUploads/-")) ids
        else ids.filter { uploadName(it) == request.parent }
      listRawImpressionUploadModelLinesResponse {
        for (id in selected) {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = modelLineName(id)
            cmmsModelLine = MODEL_LINE
            state = RawImpressionUploadModelLine.State.COMPLETED
            etag = "etag-$id"
          }
        }
      }
    }
    whenever(rankIndexBlobService.listRankIndexBlobs(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRankIndexBlobsRequest>(0)
      val ids = mutableListOf("up1")
      if (includeLaterUpload) ids += "up2"
      val selected =
        if (request.parent.endsWith("/rawImpressionUploads/-")) ids
        else ids.filter { uploadName(it) == request.parent }
      listRankIndexBlobsResponse {
        for (id in selected) {
          rankIndexBlobs += rankIndexBlob {
            name = snapshotName(id)
            blobType = RankIndexBlob.BlobType.SNAPSHOT
            cmmsModelLine = MODEL_LINE
          }
        }
      }
    }
    whenever(modelLineService.getRawImpressionUploadModelLine(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadModelLineRequest
        >(
          0
        )
      rawImpressionUploadModelLine {
        name = request.name
        cmmsModelLine = MODEL_LINE
        state = RawImpressionUploadModelLine.State.COMPLETED
        etag = "etag-${request.name}"
      }
    }
    whenever(modelLineService.markRawImpressionUploadModelLineFailed(any()))
      .thenReturn(
        rawImpressionUploadModelLine { state = RawImpressionUploadModelLine.State.FAILED }
      )
    whenever(rankIndexBlobService.deleteRankIndexBlob(any())).thenReturn(rankIndexBlob {})

    val confirmedPlan = evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
    includeLaterUpload = true
    val error =
      assertFailsWith<IllegalArgumentException> { evictUploader.evict(confirmedPlan, REASON) }

    assertThat(error).hasMessageThat().contains("plan changed")
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineFailed(any()) }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val REASON = "bad data"
    private val T0: Instant = Instant.parse("2026-06-30T00:00:00Z")
    private val T1: Instant = Instant.parse("2026-07-01T00:00:00Z")
    private val T2: Instant = Instant.parse("2026-07-02T00:00:00Z")
    private val T3: Instant = Instant.parse("2026-07-03T00:00:00Z")
  }
}
