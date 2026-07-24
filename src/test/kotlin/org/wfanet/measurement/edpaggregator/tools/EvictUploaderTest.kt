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
import org.mockito.kotlin.never
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toProtoTime
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
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            for (id in listOf("up1", "up2", "up3")) {
              rawImpressionUploadModelLines += rawImpressionUploadModelLine {
                name = modelLineName(id)
                cmmsModelLine = MODEL_LINE
                etag = "etag-$id"
                state = RawImpressionUploadModelLine.State.COMPLETED
              }
            }
          }
        )
      whenever(modelLineService.markRawImpressionUploadModelLineFailed(any())).thenAnswer {
        rawImpressionUploadModelLine { state = RawImpressionUploadModelLine.State.FAILED }
      }
      whenever(rankIndexBlobService.listRankIndexBlobs(any()))
        .thenReturn(
          listRankIndexBlobsResponse {
            rankIndexBlobs += rankIndexBlob {
              name = "snapshot-blob"
              blobType = RankIndexBlob.BlobType.SNAPSHOT
            }
          }
        )
      whenever(rankIndexBlobService.deleteRankIndexBlob(any())).thenAnswer { rankIndexBlob {} }

      val plan = evictUploader.plan(MODEL_LINE, listOf(uploadName("up2")), cutoffTime = T0)
      val result = evictUploader.evict(MODEL_LINE, plan, REASON)
      plan to result
    }

    val (plan, result) = planAndResult
    // Cascade starts at the earliest bad upload (up2) and includes everything after it (up3).
    assertThat(plan.cascade.map { it.uploadName })
      .containsExactly(uploadName("up2"), uploadName("up3"))
      .inOrder()
    assertThat(plan.extraUploads).containsExactly(uploadName("up3"))
    // Both cascade model lines are failed and each has its SNAPSHOT soft-deleted.
    assertThat(result.failedModelLines).containsExactly(modelLineName("up2"), modelLineName("up3"))
    assertThat(result.deletedSnapshots).isEqualTo(2)
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
          evictUploader.plan(MODEL_LINE, listOf(uploadName("up1")), cutoffTime = T2)
        }
      }
    assertThat(error).hasMessageThat().contains("retention window")
  }

  @Test
  fun `plan throws when a requested upload has no model-line row for the model line`() {
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
          whenever(modelLineService.listRawImpressionUploadModelLines(any()))
            .thenReturn(
              listRawImpressionUploadModelLinesResponse {
                rawImpressionUploadModelLines += rawImpressionUploadModelLine {
                  name = modelLineName("up1")
                  cmmsModelLine = MODEL_LINE
                  state = RawImpressionUploadModelLine.State.COMPLETED
                }
              }
            )
          evictUploader.plan(
            MODEL_LINE,
            listOf(uploadName("up1"), uploadName("up2")),
            cutoffTime = T0,
          )
        }
      }
    assertThat(error).hasMessageThat().contains("no $MODEL_LINE model-line row")
  }

  @Test
  fun `plan rejects bad uploads spanning multiple DataProviders`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        runBlocking {
          evictUploader.plan(
            MODEL_LINE,
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
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = modelLineName("up1")
              cmmsModelLine = MODEL_LINE
              state = RawImpressionUploadModelLine.State.COMPLETED
            }
          }
        )
      // evict() re-fetches current state before marking; it is already FAILED, so Mark is skipped.
      whenever(modelLineService.getRawImpressionUploadModelLine(any()))
        .thenReturn(
          rawImpressionUploadModelLine {
            name = modelLineName("up1")
            cmmsModelLine = MODEL_LINE
            state = RawImpressionUploadModelLine.State.FAILED
          }
        )
      whenever(rankIndexBlobService.listRankIndexBlobs(any()))
        .thenReturn(
          listRankIndexBlobsResponse {
            rankIndexBlobs += rankIndexBlob {
              name = "snapshot-blob"
              blobType = RankIndexBlob.BlobType.SNAPSHOT
            }
          }
        )
      whenever(rankIndexBlobService.deleteRankIndexBlob(any())).thenAnswer { rankIndexBlob {} }

      val plan = evictUploader.plan(MODEL_LINE, listOf(uploadName("up1")), cutoffTime = T0)
      evictUploader.evict(MODEL_LINE, plan, REASON)
    }

    assertThat(result.failedModelLines).isEmpty()
    assertThat(result.deletedSnapshots).isEqualTo(1)
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
