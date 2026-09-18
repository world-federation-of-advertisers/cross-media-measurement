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
import com.google.protobuf.timestamp
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
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
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ReleaseRawImpressionUploadEvictionFenceResponse
import org.wfanet.measurement.edpaggregator.v1alpha.acquireRawImpressionUploadEvictionFenceResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys

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
  private val rawImpressionUploadFileService:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase =
    mockService()
  private val impressionMetadataService:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase =
    mockService()
  private val deletedBlobUris = mutableListOf<String>()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(uploadService)
    addService(modelLineService)
    addService(rankIndexBlobService)
    addService(rawImpressionUploadFileService)
    addService(impressionMetadataService)
  }

  private val evictUploader: EvictUploader by lazy {
    val channel = grpcTestServerRule.channel
    EvictUploader(
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(channel),
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        channel
      ),
      RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(channel),
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(channel),
      ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(channel),
      LABELED_IMPRESSIONS_BLOB_PREFIX,
      deleteBlob = { blobUri -> deletedBlobUris.add(blobUri) },
    )
  }

  @Before
  fun stubServices(): Unit = runBlocking {
    whenever(uploadService.acquireRawImpressionUploadEvictionFence(any()))
      .thenReturn(acquireRawImpressionUploadEvictionFenceResponse { newlyAcquired = true })
    whenever(uploadService.releaseRawImpressionUploadEvictionFence(any()))
      .thenReturn(ReleaseRawImpressionUploadEvictionFenceResponse.getDefaultInstance())
    deletedBlobUris.clear()
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(listRawImpressionUploadFilesResponse {})
    whenever(impressionMetadataService.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse {})
    whenever(impressionMetadataService.batchDeleteImpressionMetadata(any()))
      .thenReturn(batchDeleteImpressionMetadataResponse {})
  }

  private fun uploadName(id: String) = "$DATA_PROVIDER/rawImpressionUploads/$id"

  private fun modelLineName(uploadId: String, id: String = "rml") =
    "${uploadName(uploadId)}/rawImpressionUploadModelLines/$id"

  private fun snapshotName(uploadId: String, id: String = "snapshot") =
    "${uploadName(uploadId)}/rankIndexBlobs/$id"

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
              doneBlobUri = "gs://raw/up1/done"
              doneBlobGeneration = 1L
            }
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName("up2")
              createTime = T2.toProtoTime()
              doneBlobUri = "gs://raw/up2/done"
              doneBlobGeneration = 1L
            }
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName("up3")
              createTime = T3.toProtoTime()
              doneBlobUri = "gs://raw/up3/done"
              doneBlobGeneration = 1L
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
    assertThat(plan.recoveryTargets)
      .containsExactly(EvictUploader.RecoveryTarget(uploadName("up3"), listOf(MODEL_LINE)))
    assertThat(plan.cascade.map { it.recoveryAction })
      .containsExactly(
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION,
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY,
      )
      .inOrder()
    assertThat(plan.cascade.map { it.recoveryPredecessorUploadName })
      .containsExactly(uploadName("up1"), uploadName("up2"))
      .inOrder()
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
      assertThat(request.failureReason)
        .isEqualTo(RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT)
      assertThat(request.evictionOperationId).isEqualTo(plan.evictionOperationId)
    }
    assertThat(requestCaptor.allValues.map { it.recoveryAction })
      .containsExactly(
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION,
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY,
      )
      .inOrder()
    assertThat(requestCaptor.allValues.map { it.requestId }).containsNoDuplicates()
  }

  @Test
  fun `recovery targets include only latest revision for each later done path`(): Unit =
    runBlocking {
      whenever(uploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName("up1")
              createTime = T1.toProtoTime()
              doneBlobUri = "gs://raw/d1/done"
              doneBlobGeneration = 1L
            }
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName("up2-old")
              createTime = T2.toProtoTime()
              doneBlobUri = "gs://raw/d2/done"
              doneBlobGeneration = 200L
              doneBlobCreateTime = T2.toProtoTime()
            }
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName("up2-new")
              createTime = T3.toProtoTime()
              doneBlobUri = "gs://raw/d2/done"
              doneBlobGeneration = 100L
              doneBlobCreateTime = T3.toProtoTime()
              replacesRawImpressionUpload = uploadName("up2-old")
            }
          }
        )
      stubModelLineRows("up1", "up2-old", "up2-new")
      stubSnapshotRows("up1", "up2-old", "up2-new")

      val plan = evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)

      assertThat(plan.recoveryTargets)
        .containsExactly(EvictUploader.RecoveryTarget(uploadName("up2-new"), listOf(MODEL_LINE)))
      assertThat(
          plan.cascade
            .single { it.uploadName == uploadName("up2-new") }
            .recoveryPredecessorUploadName
        )
        .isEqualTo(uploadName("up1"))
    }

  @Test
  fun `plan keeps recovery dependencies in chronological order`(): Unit = runBlocking {
    whenever(uploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          for ((id, time) in
            listOf("up1" to T1, "up2" to T2, "up3" to T3, "up4" to T4, "up5" to T5)) {
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName(id)
              createTime = time.toProtoTime()
              doneBlobUri = "gs://raw/$id/done"
              doneBlobGeneration = 1L
            }
          }
        }
      )
    stubModelLineRows("up1", "up2", "up3", "up4", "up5")
    stubSnapshotRows("up1", "up2", "up3", "up4", "up5")

    val plan = evictUploader.plan(listOf(uploadName("up2"), uploadName("up4")), cutoffTime = T0)
    val entries = plan.cascade.associateBy { it.uploadName }

    assertThat(entries.getValue(uploadName("up2")).recoveryAction)
      .isEqualTo(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION)
    assertThat(entries.getValue(uploadName("up2")).recoveryPredecessorUploadName)
      .isEqualTo(uploadName("up1"))
    assertThat(entries.getValue(uploadName("up4")).recoveryAction)
      .isEqualTo(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION)
    assertThat(entries.getValue(uploadName("up4")).recoveryPredecessorUploadName)
      .isEqualTo(uploadName("up3"))
    assertThat(entries.getValue(uploadName("up3")).recoveryAction)
      .isEqualTo(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY)
    assertThat(entries.getValue(uploadName("up3")).recoveryPredecessorUploadName)
      .isEqualTo(uploadName("up2"))
    assertThat(entries.getValue(uploadName("up5")).recoveryAction)
      .isEqualTo(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY)
    assertThat(entries.getValue(uploadName("up5")).recoveryPredecessorUploadName)
      .isEqualTo(uploadName("up4"))
  }

  @Test
  fun `plan skips noncontiguous permanently removed uploads in recovery dependencies`(): Unit =
    runBlocking {
      whenever(uploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            for ((id, time) in
              listOf("up1" to T1, "up2" to T2, "up3" to T3, "up4" to T4, "up5" to T5)) {
              rawImpressionUploads += rawImpressionUpload {
                name = uploadName(id)
                createTime = time.toProtoTime()
                doneBlobUri = "gs://raw/$id/done"
                doneBlobGeneration = 1L
              }
            }
          }
        )
      stubModelLineRows("up1", "up2", "up3", "up4", "up5")
      stubSnapshotRows("up1", "up2", "up3", "up4", "up5")

      val plan =
        evictUploader.plan(
          listOf(uploadName("up2"), uploadName("up4")),
          cutoffTime = T0,
          noReplacementUploads = setOf(uploadName("up2"), uploadName("up4")),
        )
      val entries = plan.cascade.associateBy { it.uploadName }

      assertThat(entries.getValue(uploadName("up2")).recoveryAction)
        .isEqualTo(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT)
      assertThat(entries.getValue(uploadName("up2")).recoveryPredecessorUploadName)
        .isEqualTo(uploadName("up1"))
      assertThat(entries.getValue(uploadName("up3")).recoveryAction)
        .isEqualTo(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY)
      assertThat(entries.getValue(uploadName("up3")).recoveryPredecessorUploadName)
        .isEqualTo(uploadName("up1"))
      assertThat(entries.getValue(uploadName("up4")).recoveryAction)
        .isEqualTo(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT)
      assertThat(entries.getValue(uploadName("up4")).recoveryPredecessorUploadName)
        .isEqualTo(uploadName("up3"))
      assertThat(entries.getValue(uploadName("up5")).recoveryAction)
        .isEqualTo(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY)
      assertThat(entries.getValue(uploadName("up5")).recoveryPredecessorUploadName)
        .isEqualTo(uploadName("up3"))
      assertThat(plan.recoveryTargets)
        .containsExactly(
          EvictUploader.RecoveryTarget(uploadName("up3"), listOf(MODEL_LINE)),
          EvictUploader.RecoveryTarget(uploadName("up5"), listOf(MODEL_LINE)),
        )
        .inOrder()
    }

  @Test
  fun `plan starts recovery without a predecessor when first upload is permanently removed`():
    Unit = runBlocking {
    whenever(uploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          for ((id, time) in listOf("up1" to T1, "up2" to T2, "up3" to T3)) {
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName(id)
              createTime = time.toProtoTime()
              doneBlobUri = "gs://raw/$id/done"
              doneBlobGeneration = 1L
            }
          }
        }
      )
    stubModelLineRows("up1", "up2", "up3")
    stubSnapshotRows("up1", "up2", "up3")

    val plan =
      evictUploader.plan(
        listOf(uploadName("up1")),
        cutoffTime = T0,
        noReplacementUploads = setOf(uploadName("up1")),
      )
    val entries = plan.cascade.associateBy { it.uploadName }

    assertThat(entries.getValue(uploadName("up1")).recoveryAction)
      .isEqualTo(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT)
    assertThat(entries.getValue(uploadName("up2")).recoveryPredecessorUploadName).isEmpty()
    assertThat(entries.getValue(uploadName("up3")).recoveryPredecessorUploadName)
      .isEqualTo(uploadName("up2"))
  }

  @Test
  fun `plan skips a previously removed upload when selecting the first predecessor`(): Unit =
    runBlocking {
      whenever(uploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            for ((id, time) in listOf("up1" to T1, "up2" to T2, "up3" to T3)) {
              rawImpressionUploads += rawImpressionUpload {
                name = uploadName(id)
                createTime = time.toProtoTime()
                doneBlobUri = "gs://raw/$id/done"
                doneBlobGeneration = 1L
              }
            }
          }
        )
      whenever(modelLineService.listRawImpressionUploadModelLines(any())).thenAnswer { invocation ->
        val request = invocation.getArgument<ListRawImpressionUploadModelLinesRequest>(0)
        val ids = listOf("up1", "up2", "up3")
        val selected =
          if (request.parent.endsWith("/rawImpressionUploads/-")) ids
          else ids.filter { uploadName(it) == request.parent }
        listRawImpressionUploadModelLinesResponse {
          for (id in selected) {
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = modelLineName(id)
              cmmsModelLine = MODEL_LINE
              state =
                if (id == "up2") {
                  RawImpressionUploadModelLine.State.FAILED
                } else {
                  RawImpressionUploadModelLine.State.COMPLETED
                }
              if (id == "up2") {
                failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
                evictionOperationId = "00000000-0000-4000-8000-000000000001"
                recoveryAction =
                  RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT
              }
            }
          }
        }
      }
      whenever(rankIndexBlobService.listRankIndexBlobs(any()))
        .thenReturn(
          listRankIndexBlobsResponse {
            rankIndexBlobs += rankIndexBlob {
              name = snapshotName("up1")
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              cmmsModelLine = MODEL_LINE
            }
            rankIndexBlobs += rankIndexBlob {
              name = snapshotName("up2")
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              cmmsModelLine = MODEL_LINE
              deleteTime = timestamp { seconds = 1 }
            }
            rankIndexBlobs += rankIndexBlob {
              name = snapshotName("up3")
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              cmmsModelLine = MODEL_LINE
            }
          }
        )

      val plan = evictUploader.plan(listOf(uploadName("up3")), cutoffTime = T0)

      assertThat(plan.cascade.single().recoveryPredecessorUploadName).isEqualTo(uploadName("up1"))
    }

  @Test
  fun `plan rejects no-replacement upload outside bad uploads`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        runBlocking {
          evictUploader.plan(
            listOf(uploadName("up1")),
            cutoffTime = T0,
            noReplacementUploads = setOf(uploadName("up2")),
          )
        }
      }

    assertThat(error).hasMessageThat().contains("must also be listed in badUploads")
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
  fun `evict skips an already-evicted model line but still soft-deletes its snapshots`() {
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
      stubSnapshotRows("up1")
      whenever(rankIndexBlobService.deleteRankIndexBlob(any())).thenAnswer { rankIndexBlob {} }

      val plan = evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
      val entry = plan.cascade.single()
      // evict() re-fetches current state before marking. This row already has this operation's
      // complete recovery metadata, so Mark is skipped.
      whenever(modelLineService.getRawImpressionUploadModelLine(any()))
        .thenReturn(
          rawImpressionUploadModelLine {
            name = modelLineName("up1")
            cmmsModelLine = MODEL_LINE
            state = RawImpressionUploadModelLine.State.FAILED
            failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
            evictionOperationId = plan.evictionOperationId
            recoveryAction = entry.recoveryAction
            recoveryPredecessorRawImpressionUpload = entry.recoveryPredecessorUploadName
          }
        )
      evictUploader.evict(plan, REASON)
    }

    assertThat(result.failedModelLines).isEmpty()
    assertThat(result.deletedSnapshots).isEqualTo(1)
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineFailed(any()) }
  }

  @Test
  fun `evict reclassifies a processing failure before deleting snapshots`() {
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
              state = RawImpressionUploadModelLine.State.FAILED
              failureReason = RawImpressionUploadModelLine.FailureReason.PROCESSING_FAILURE
            }
          }
        )
      whenever(modelLineService.getRawImpressionUploadModelLine(any()))
        .thenReturn(
          rawImpressionUploadModelLine {
            name = modelLineName("up1")
            cmmsModelLine = MODEL_LINE
            state = RawImpressionUploadModelLine.State.FAILED
            failureReason = RawImpressionUploadModelLine.FailureReason.PROCESSING_FAILURE
            etag = "etag-up1"
          }
        )
      whenever(modelLineService.markRawImpressionUploadModelLineFailed(any()))
        .thenReturn(
          rawImpressionUploadModelLine {
            state = RawImpressionUploadModelLine.State.FAILED
            failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
          }
        )
      whenever(rankIndexBlobService.listRankIndexBlobs(any()))
        .thenReturn(listRankIndexBlobsResponse {})

      val plan = evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
      evictUploader.evict(plan, REASON)
    }

    assertThat(result.failedModelLines).containsExactly(modelLineName("up1"))
    val requestCaptor = argumentCaptor<MarkRawImpressionUploadModelLineFailedRequest>()
    verifyBlocking(modelLineService) {
      markRawImpressionUploadModelLineFailed(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.failureReason)
      .isEqualTo(RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT)
    assertThat(requestCaptor.firstValue.etag).isEqualTo("etag-up1")
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
  fun `plan rejects old non-memoized revision whose replacement owns current output`(): Unit =
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
              replacesRawImpressionUpload = uploadName("up1")
            }
          }
        )
      stubModelLineRows("up1", "up2")
      whenever(rankIndexBlobService.listRankIndexBlobs(any()))
        .thenReturn(listRankIndexBlobsResponse {})

      val error =
        assertFailsWith<IllegalArgumentException> {
          evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
        }

      assertThat(error).hasMessageThat().contains("completed replacement rows")
      assertThat(error).hasMessageThat().contains(modelLineName("up2"))
    }

  @Test
  fun `plan rejects old memoized revision whose replacement owns current output`(): Unit =
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
              replacesRawImpressionUpload = uploadName("up1")
            }
          }
        )
      stubModelLineRows("up1", "up2")
      stubSnapshotRows("up1", "up2")

      val error =
        assertFailsWith<IllegalArgumentException> {
          evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
        }

      assertThat(error).hasMessageThat().contains("completed replacement rows")
      assertThat(error).hasMessageThat().contains(modelLineName("up2"))
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
  fun `plan discovers snapshots with one paginated wildcard lookup per model line`(): Unit =
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
            rawImpressionUploads += rawImpressionUpload {
              name = uploadName("up3")
              createTime = T3.toProtoTime()
            }
          }
        )
      stubModelLineRows("up1", "up2", "up3")
      whenever(rankIndexBlobService.listRankIndexBlobs(any())).thenAnswer { invocation ->
        val request = invocation.getArgument<ListRankIndexBlobsRequest>(0)
        check(request.parent == "$DATA_PROVIDER/rawImpressionUploads/-")
        if (request.pageToken.isEmpty()) {
          listRankIndexBlobsResponse {
            rankIndexBlobs += rankIndexBlob {
              name = snapshotName("up1")
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              cmmsModelLine = MODEL_LINE
            }
            nextPageToken = "page-2"
          }
        } else {
          listRankIndexBlobsResponse {
            rankIndexBlobs += rankIndexBlob {
              name = snapshotName("up2")
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              cmmsModelLine = MODEL_LINE
            }
          }
        }
      }

      val plan = evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)

      assertThat(plan.cascade.map { it.uploadName })
        .containsExactly(uploadName("up1"), uploadName("up2"))
        .inOrder()
      val requests = argumentCaptor<ListRankIndexBlobsRequest>()
      verifyBlocking(rankIndexBlobService, times(2)) { listRankIndexBlobs(requests.capture()) }
      assertThat(requests.allValues.map { it.parent })
        .containsExactly(
          "$DATA_PROVIDER/rawImpressionUploads/-",
          "$DATA_PROVIDER/rawImpressionUploads/-",
        )
      assertThat(requests.allValues.map { it.pageToken }).containsExactly("", "page-2").inOrder()
      assertThat(requests.allValues.all { it.showDeleted }).isTrue()
    }

  @Test
  fun `one upload can mix memoized and non-memoized model lines`(): Unit = runBlocking {
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
    whenever(modelLineService.listRawImpressionUploadModelLines(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadModelLinesRequest>(0)
      val uploadIds =
        if (request.parent.endsWith("/rawImpressionUploads/-")) {
          listOf("up1", "up2")
        } else {
          listOf("up1", "up2").filter { uploadName(it) == request.parent }
        }
      val modelLines =
        if (request.filter.cmmsModelLine.isEmpty()) {
          listOf(MEMOIZED_MODEL_LINE, NON_MEMOIZED_MODEL_LINE)
        } else {
          listOf(request.filter.cmmsModelLine)
        }
      listRawImpressionUploadModelLinesResponse {
        for (uploadId in uploadIds) {
          for (cmmsModelLine in modelLines) {
            val id = if (cmmsModelLine == MEMOIZED_MODEL_LINE) "memoized" else "non-memoized"
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = modelLineName(uploadId, id)
              this.cmmsModelLine = cmmsModelLine
              state = RawImpressionUploadModelLine.State.COMPLETED
              etag = "etag-$uploadId-$id"
            }
          }
        }
      }
    }
    whenever(rankIndexBlobService.listRankIndexBlobs(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRankIndexBlobsRequest>(0)
      if (request.filter.cmmsModelLine != MEMOIZED_MODEL_LINE) {
        listRankIndexBlobsResponse {}
      } else {
        val uploadIds =
          if (request.parent.endsWith("/rawImpressionUploads/-")) {
            listOf("up1", "up2")
          } else {
            listOf("up1", "up2").filter { uploadName(it) == request.parent }
          }
        listRankIndexBlobsResponse {
          for (uploadId in uploadIds) {
            rankIndexBlobs += rankIndexBlob {
              name = snapshotName(uploadId, "memoized")
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              cmmsModelLine = MEMOIZED_MODEL_LINE
            }
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
        cmmsModelLine =
          if (request.name.endsWith("/memoized")) MEMOIZED_MODEL_LINE else NON_MEMOIZED_MODEL_LINE
        state = RawImpressionUploadModelLine.State.COMPLETED
        etag = "etag-${request.name}"
      }
    }
    whenever(modelLineService.markRawImpressionUploadModelLineFailed(any()))
      .thenReturn(
        rawImpressionUploadModelLine { state = RawImpressionUploadModelLine.State.FAILED }
      )
    whenever(rankIndexBlobService.deleteRankIndexBlob(any())).thenReturn(rankIndexBlob {})

    val plan = evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
    val result = evictUploader.evict(plan, REASON)

    assertThat(plan.memoizedModelLines).containsExactly(MEMOIZED_MODEL_LINE)
    assertThat(plan.nonMemoizedModelLines).containsExactly(NON_MEMOIZED_MODEL_LINE)
    assertThat(plan.cascade.map { it.modelLineName })
      .containsExactly(
        modelLineName("up1", "memoized"),
        modelLineName("up1", "non-memoized"),
        modelLineName("up2", "memoized"),
      )
    assertThat(result.failedModelLines).hasSize(3)
    assertThat(result.deletedSnapshots).isEqualTo(2)
  }

  @Test
  fun `plan rejects queued or running work anywhere under data provider`(): Unit = runBlocking {
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
            name = modelLineName("queued-upload")
            cmmsModelLine = MODEL_LINE
            state = RawImpressionUploadModelLine.State.CREATED
          }
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = modelLineName("running-upload")
            cmmsModelLine = "modelProviders/mp/modelSuites/ms/modelLines/other"
            state = RawImpressionUploadModelLine.State.RANKING
          }
        }
      )

    val error =
      assertFailsWith<IllegalArgumentException> {
        evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
      }

    assertThat(error).hasMessageThat().contains("VID-labeling pipeline is currently processing")
    assertThat(error).hasMessageThat().contains("Retry after all VID-labeling processing")
    assertThat(error).hasMessageThat().contains(modelLineName("queued-upload"))
    assertThat(error).hasMessageThat().contains(modelLineName("running-upload"))
    val request = argumentCaptor<ListRawImpressionUploadModelLinesRequest>()
    verifyBlocking(modelLineService) { listRawImpressionUploadModelLines(request.capture()) }
    assertThat(request.firstValue.parent).isEqualTo("$DATA_PROVIDER/rawImpressionUploads/-")
    assertThat(request.firstValue.filter.stateInList)
      .containsExactlyElementsIn(
        listOf(
          RawImpressionUploadModelLine.State.CREATED,
          RawImpressionUploadModelLine.State.POOL_ASSIGNING,
          RawImpressionUploadModelLine.State.RANKING,
          RawImpressionUploadModelLine.State.LABELING,
        )
      )
  }

  @Test
  fun `prepare releases newly acquired fence when plan changes`(): Unit = runBlocking {
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
    val error = assertFailsWith<IllegalArgumentException> { evictUploader.prepare(confirmedPlan) }

    assertThat(error).hasMessageThat().contains("plan changed")
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineFailed(any()) }
    verifyBlocking(uploadService) { releaseRawImpressionUploadEvictionFence(any()) }
  }

  @Test
  fun `prepare retains reacquired fence when plan refresh fails`(): Unit = runBlocking {
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
    stubSnapshotRows("up1")
    val confirmedPlan = evictUploader.plan(listOf(uploadName("up1")), cutoffTime = T0)
    whenever(uploadService.acquireRawImpressionUploadEvictionFence(any()))
      .thenReturn(acquireRawImpressionUploadEvictionFenceResponse { newlyAcquired = false })
    whenever(uploadService.listRawImpressionUploads(any()))
      .thenThrow(Status.UNAVAILABLE.asRuntimeException())

    val error = assertFailsWith<StatusException> { evictUploader.prepare(confirmedPlan) }

    assertThat(error.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineFailed(any()) }
    verifyBlocking(uploadService, never()) { releaseRawImpressionUploadEvictionFence(any()) }
  }

  @Test
  fun `evict soft deletes metadata and removes output and sidecar but retains raw input`(): Unit =
    runBlocking {
      val rawBlobUri = "gs://raw-bucket/day/file.parquet"
      val eventDate = LocalDate.of(2026, 7, 1)
      val outputKey = LabeledImpressionsBlobKeys.forInput(rawBlobUri, MODEL_LINE, eventDate)
      val outputUri = "$LABELED_IMPRESSIONS_BLOB_PREFIX/$outputKey"
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
      whenever(rankIndexBlobService.listRankIndexBlobs(any()))
        .thenReturn(listRankIndexBlobsResponse {})
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
        .thenReturn(
          listRawImpressionUploadFilesResponse {
            rawImpressionUploadFiles += rawImpressionUploadFile {
              name = "${uploadName("up1")}/files/file1"
              blobUri = rawBlobUri
              this.eventDate = date {
                year = eventDate.year
                month = eventDate.monthValue
                day = eventDate.dayOfMonth
              }
            }
          }
        )
      whenever(impressionMetadataService.listImpressionMetadata(any()))
        .thenReturn(
          listImpressionMetadataResponse {
            impressionMetadata += impressionMetadata {
              name = "$DATA_PROVIDER/impressionMetadata/im1"
              blobUri = "$outputUri.metadata.binpb"
              modelLine = MODEL_LINE
              state = org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata.State.ACTIVE
            }
          }
        )
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

      assertThat(result.deletedImpressionMetadata).isEqualTo(1)
      assertThat(result.deletedOutputBlobs).isEqualTo(2)
      assertThat(deletedBlobUris).containsExactly("$outputUri.metadata.binpb", outputUri).inOrder()
      val requestCaptor = argumentCaptor<ListImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataService) { listImpressionMetadata(requestCaptor.capture()) }
      assertThat(requestCaptor.firstValue.filter.blobUrisList)
        .containsExactly("$outputUri.metadata.binpb")
      assertThat(requestCaptor.firstValue.pageSize).isEqualTo(1000)
      assertThat(requestCaptor.firstValue.showDeleted).isTrue()
    }

  @Test
  fun `evict removes deterministic outputs before metadata has been registered`(): Unit =
    runBlocking {
      val rawBlobUri = "gs://raw-bucket/day/file.parquet"
      val eventDate = LocalDate.of(2026, 7, 1)
      val outputKey = LabeledImpressionsBlobKeys.forInput(rawBlobUri, MODEL_LINE, eventDate)
      val outputUri = "$LABELED_IMPRESSIONS_BLOB_PREFIX/$outputKey"
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
      whenever(rankIndexBlobService.listRankIndexBlobs(any()))
        .thenReturn(listRankIndexBlobsResponse {})
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
        .thenReturn(
          listRawImpressionUploadFilesResponse {
            rawImpressionUploadFiles += rawImpressionUploadFile {
              name = "${uploadName("up1")}/files/file1"
              blobUri = rawBlobUri
              this.eventDate = date {
                year = eventDate.year
                month = eventDate.monthValue
                day = eventDate.dayOfMonth
              }
            }
          }
        )
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

      assertThat(result.deletedImpressionMetadata).isEqualTo(0)
      assertThat(result.deletedOutputBlobs).isEqualTo(2)
      assertThat(deletedBlobUris).containsExactly("$outputUri.metadata.binpb", outputUri).inOrder()
    }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val LABELED_IMPRESSIONS_BLOB_PREFIX = "gs://output-bucket/prefix"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val MEMOIZED_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/memoized"
    private const val NON_MEMOIZED_MODEL_LINE =
      "modelProviders/mp1/modelSuites/ms1/modelLines/non-memoized"
    private const val REASON = "bad data"
    private val T0: Instant = Instant.parse("2026-06-30T00:00:00Z")
    private val T1: Instant = Instant.parse("2026-07-01T00:00:00Z")
    private val T2: Instant = Instant.parse("2026-07-02T00:00:00Z")
    private val T3: Instant = Instant.parse("2026-07-03T00:00:00Z")
    private val T4: Instant = Instant.parse("2026-07-04T00:00:00Z")
    private val T5: Instant = Instant.parse("2026-07-05T00:00:00Z")
  }
}
