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

package org.wfanet.measurement.edpaggregator.subpoolassigner

import com.google.common.truth.Truth.assertThat
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.MarkPoolAssignmentJobSucceededResponseKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidRankBuilderParams
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.virtualpeople.common.LabelerInput

private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/up1"
private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"
private const val POOL_ASSIGNMENT_JOB =
  "dataProviders/dp/rawImpressionUploads/up1/poolAssignmentJobs/paj-0"
private const val PARENT_NAME =
  "dataProviders/dp/rawImpressionUploads/up1/rawImpressionUploadModelLines/rl1"
private const val QUEUE = "queues/vid-rank-builder"

private val DEK_GEN = encryptedDek { kekUri = "kek-gen" }
private val DEK_MERGED = encryptedDek { kekUri = "kek-merged" }
private val DEK_SHARD0 = encryptedDek { kekUri = "kek-shard0" }

private val TEMPLATE: VidRankBuilderParams = vidRankBuilderParams {
  dataProvider = "dataProviders/dp"
  rawImpressionUpload = UPLOAD
  modelLine = MODEL_LINE
  totalShards = 1
}

@RunWith(JUnit4::class)
class SubpoolAssignerTest {
  /** Records cross-collaborator call order so tests can assert e.g. delete-after-flip. */
  private val order = mutableListOf<String>()

  private fun jobResponse(state: PoolAssignmentJob.State) = poolAssignmentJob {
    this.state = state
    etag = "etag-1"
  }

  private fun parent(
    state: RawImpressionUploadModelLine.State,
    poolOffsets: List<Long> = emptyList(),
    withMergedDek: Boolean = false,
  ) = rawImpressionUploadModelLine {
    name = PARENT_NAME
    cmmsModelLine = MODEL_LINE
    this.state = state
    this.poolOffsets += poolOffsets
    maxEventDate = date {
      year = 2026
      month = 6
      day = 15
    }
    if (withMergedDek) {
      encryptedMergedDek = DEK_MERGED
    }
  }

  /** A store mock that records write/merge/delete and hands back [DEK_GEN] from generateDek. */
  private fun storeMock(): SubpoolFingerprintsStore = mock {
    on { generateDek(any()) } doReturn DEK_GEN
    onBlocking { writeBlob(any(), any(), any(), any()) } doAnswer
      {
        order.add("write")
        Unit
      }
    onBlocking { mergeSubpool(any(), any(), any()) } doAnswer
      {
        order.add("merge")
        Unit
      }
    onBlocking { delete(any()) } doAnswer
      {
        order.add("delete")
        Unit
      }
  }

  private fun rankerStubMock(): RankerJobServiceCoroutineStub = mock {
    onBlocking { createRankerJob(any(), any()) } doAnswer
      {
        order.add("ranker")
        rankerJob { name = "$UPLOAD/rankerJobs/rj7" }
      }
  }

  private fun workItemsStubMock(): WorkItemsCoroutineStub = mock {
    onBlocking { createWorkItem(any(), any()) } doAnswer
      {
        order.add("workitem")
        WorkItem.getDefaultInstance()
      }
  }

  private fun assigner(
    store: SubpoolFingerprintsStore,
    poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
    modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
    rankerStub: RankerJobServiceCoroutineStub = rankerStubMock(),
    workItemsStub: WorkItemsCoroutineStub = workItemsStubMock(),
    source: RawImpressionSource = mock(),
    accumulator: SubpoolFingerprintsAccumulator = SubpoolFingerprintsAccumulator(),
  ) =
    SubpoolAssigner(
      rawImpressionSource = source,
      mapper = mock<LabelerInputMapper>(),
      labeler = FakePoolEmitLabeler,
      activeWindow = ActiveWindow(0, Long.MAX_VALUE),
      store = store,
      kekUri = "kek",
      blobPrefix = "maps",
      poolAssignmentJobsStub = poolAssignmentJobsStub,
      rawImpressionUploadModelLinesStub = modelLinesStub,
      rankerJobsStub = rankerStub,
      rawImpressionUploadsStub = mock<RawImpressionUploadServiceCoroutineStub>(),
      workItemsStub = workItemsStub,
      rawImpressionUpload = UPLOAD,
      modelLine = MODEL_LINE,
      poolAssignmentJob = POOL_ASSIGNMENT_JOB,
      shardIndex = 0,
      totalShards = 1,
      vidRankBuilderQueue = QUEUE,
      vidRankBuilderParamsTemplate = TEMPLATE,
      accumulator = accumulator,
    )

  private fun accumulatorWith(subpoolId: Long): SubpoolFingerprintsAccumulator =
    SubpoolFingerprintsAccumulator().apply { add(subpoolId, 1L, 1) }

  @Test
  fun `non-last shard writes its blobs and marks succeeded without fan-out`() = runBlocking {
    val store = storeMock()
    val ranker = rankerStubMock()
    val workItems = workItemsStubMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.CREATED)
        // No last_shard_result -> not the last shard out.
        onBlocking { markPoolAssignmentJobSucceeded(any(), any()) } doReturn
          markPoolAssignmentJobSucceededResponse {}
      }
    val ruml = mock<RawImpressionUploadModelLineServiceCoroutineStub>()

    val result =
      assigner(store, paj, ruml, ranker, workItems, accumulator = accumulatorWith(7L)).assign()

    assertThat(result.lastShardOut).isFalse()
    verifyBlocking(store) { writeBlob(any(), any(), any(), any()) }
    verifyBlocking(paj) { markPoolAssignmentJobSucceeded(any(), any()) }
    verifyBlocking(store, never()) { mergeSubpool(any(), any(), any()) }
    verifyBlocking(ranker, never()) { createRankerJob(any(), any()) }
    verifyBlocking(workItems, never()) { createWorkItem(any(), any()) }
    verifyBlocking(ruml, never()) { markRawImpressionUploadModelLineRanking(any(), any()) }
  }

  @Test
  fun `last shard out merges with own DEK, fans out, then deletes after the flip`() = runBlocking {
    val store = storeMock()
    val ranker = rankerStubMock()
    val workItems = workItemsStubMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.CREATED)
        onBlocking { markPoolAssignmentJobSucceeded(any(), any()) } doReturn
          markPoolAssignmentJobSucceededResponse {
            lastShardResult =
              MarkPoolAssignmentJobSucceededResponseKt.lastShardResult { poolOffsets += 7L }
          }
        onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += poolAssignmentJob {
              shardIndex = 0
              encryptedDek = DEK_SHARD0
            }
            nextPageToken = ""
          }
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              parent(RawImpressionUploadModelLine.State.POOL_ASSIGNING, listOf(7L))
            nextPageToken = ""
          }
        onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doAnswer
          {
            order.add("flip")
            parent(RawImpressionUploadModelLine.State.RANKING, listOf(7L))
          }
      }

    val result =
      assigner(store, paj, ruml, ranker, workItems, accumulator = accumulatorWith(7L)).assign()

    assertThat(result.lastShardOut).isTrue()
    assertThat(order)
      .containsExactly("write", "merge", "ranker", "workitem", "flip", "delete")
      .inOrder()
    // Normal path merges with this VM's freshly generated DEK.
    val dekCaptor = argumentCaptor<EncryptedDek>()
    verifyBlocking(store) { mergeSubpool(any(), any(), dekCaptor.capture()) }
    assertThat(dekCaptor.firstValue).isEqualTo(DEK_GEN)
  }

  @Test
  fun `recovery on already-succeeded shard that was not last-out does nothing`() = runBlocking {
    val store = storeMock()
    val ranker = rankerStubMock()
    val workItems = workItemsStubMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.SUCCEEDED)
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            // Empty pool_offsets -> this shard was not the last out.
            rawImpressionUploadModelLines +=
              parent(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
            nextPageToken = ""
          }
      }

    val result = assigner(store, paj, ruml, ranker, workItems).assign()

    assertThat(result.lastShardOut).isFalse()
    verifyBlocking(store, never()) { mergeSubpool(any(), any(), any()) }
    verifyBlocking(ranker, never()) { createRankerJob(any(), any()) }
  }

  @Test
  fun `recovery short-circuits when the parent already advanced past POOL_ASSIGNING`() =
    runBlocking {
      val store = storeMock()
      val ranker = rankerStubMock()
      val workItems = workItemsStubMock()
      val paj =
        mock<PoolAssignmentJobServiceCoroutineStub> {
          onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
            jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        }
      val ruml =
        mock<RawImpressionUploadModelLineServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
            listRawImpressionUploadModelLinesResponse {
              rawImpressionUploadModelLines +=
                parent(RawImpressionUploadModelLine.State.RANKING, listOf(7L), withMergedDek = true)
              nextPageToken = ""
            }
        }

      val result = assigner(store, paj, ruml, ranker, workItems).assign()

      assertThat(result.lastShardOut).isTrue()
      verifyBlocking(store, never()) { mergeSubpool(any(), any(), any()) }
      verifyBlocking(ranker, never()) { createRankerJob(any(), any()) }
      verifyBlocking(workItems, never()) { createWorkItem(any(), any()) }
    }

  @Test
  fun `recovery re-runs the last-shard-out reusing the persisted merged DEK`() = runBlocking {
    val store = storeMock()
    val ranker = rankerStubMock()
    val workItems = workItemsStubMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += poolAssignmentJob {
              shardIndex = 0
              encryptedDek = DEK_SHARD0
            }
            nextPageToken = ""
          }
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              parent(
                RawImpressionUploadModelLine.State.POOL_ASSIGNING,
                listOf(7L),
                withMergedDek = true,
              )
            nextPageToken = ""
          }
        onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doAnswer
          {
            order.add("flip")
            parent(RawImpressionUploadModelLine.State.RANKING, listOf(7L), withMergedDek = true)
          }
      }

    val result = assigner(store, paj, ruml, ranker, workItems).assign()

    assertThat(result.lastShardOut).isTrue()
    // No re-read/re-write of per-shard blobs; delete happens AFTER the flip (#3999 regression
    // guard).
    assertThat(order).containsExactly("merge", "ranker", "workitem", "flip", "delete").inOrder()
    // Re-merge reuses the persisted DEK so it stays consistent with the already-written blobs.
    val dekCaptor = argumentCaptor<EncryptedDek>()
    verifyBlocking(store) { mergeSubpool(any(), any(), dekCaptor.capture()) }
    assertThat(dekCaptor.firstValue).isEqualTo(DEK_MERGED)
  }

  @Test
  fun `recovery fails fast when pool_offsets are set but the merged DEK is missing`() =
    runBlocking {
      val store = storeMock()
      val paj =
        mock<PoolAssignmentJobServiceCoroutineStub> {
          onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
            jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        }
      val ruml =
        mock<RawImpressionUploadModelLineServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
            listRawImpressionUploadModelLinesResponse {
              // pool_offsets set but no encrypted_merged_dek.
              rawImpressionUploadModelLines +=
                parent(RawImpressionUploadModelLine.State.POOL_ASSIGNING, listOf(7L))
              nextPageToken = ""
            }
        }

      val thrown =
        try {
          assigner(store, paj, ruml).assign()
          null
        } catch (e: Exception) {
          e
        }

      assertThat(thrown).isInstanceOf(IllegalArgumentException::class.java)
      verifyBlocking(store, never()) { mergeSubpool(any(), any(), any()) }
    }

  @Test
  fun `transient flip failure propagates and the temp blobs are not deleted`() = runBlocking {
    val store = storeMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += poolAssignmentJob {
              shardIndex = 0
              encryptedDek = DEK_SHARD0
            }
            nextPageToken = ""
          }
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              parent(
                RawImpressionUploadModelLine.State.POOL_ASSIGNING,
                listOf(7L),
                withMergedDek = true,
              )
            nextPageToken = ""
          }
        onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doAnswer
          {
            throw StatusException(Status.UNAVAILABLE)
          }
      }

    val thrown =
      try {
        assigner(store, paj, ruml).assign()
        null
      } catch (e: Exception) {
        e
      }

    assertThat(thrown).isInstanceOf(StatusException::class.java)
    // The flip is the completion marker; on a transient failure we must NOT delete the merge
    // inputs.
    verifyBlocking(store, never()) { delete(any()) }
    // The worker never marks the job FAILED itself — the DLQ listener owns the terminal FAILED
    // transition on retry exhaustion; the failure simply propagates so the framework nacks.
    verifyBlocking(paj, never()) { markPoolAssignmentJobFailed(any(), any()) }
  }

  @Test
  fun `already-advanced flip precondition is swallowed and cleanup still runs`() = runBlocking {
    val store = storeMock()
    val paj =
      mock<PoolAssignmentJobServiceCoroutineStub> {
        onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
          jobResponse(PoolAssignmentJob.State.SUCCEEDED)
        onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += poolAssignmentJob {
              shardIndex = 0
              encryptedDek = DEK_SHARD0
            }
            nextPageToken = ""
          }
      }
    val ruml =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              parent(
                RawImpressionUploadModelLine.State.POOL_ASSIGNING,
                listOf(7L),
                withMergedDek = true,
              )
            nextPageToken = ""
          }
        onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doAnswer
          {
            throw StatusException(Status.FAILED_PRECONDITION)
          }
      }

    val result = assigner(store, paj, ruml).assign()

    assertThat(result.lastShardOut).isTrue()
    verifyBlocking(store) { delete(any()) }
  }

  @Test
  fun `last shard out stamps each subpool's ranked size from the labeler`() =
    runBlocking<Unit> {
      val store = storeMock()
      val ranker = rankerStubMock()
      val workItems = workItemsStubMock()
      val paj =
        mock<PoolAssignmentJobServiceCoroutineStub> {
          onBlocking { getPoolAssignmentJob(any(), any()) } doReturn
            jobResponse(PoolAssignmentJob.State.CREATED)
          onBlocking { markPoolAssignmentJobSucceeded(any(), any()) } doReturn
            markPoolAssignmentJobSucceededResponse {
              lastShardResult =
                MarkPoolAssignmentJobSucceededResponseKt.lastShardResult {
                  poolOffsets += listOf(7L, 11L)
                }
            }
          onBlocking { listPoolAssignmentJobs(any(), any()) } doReturn
            listPoolAssignmentJobsResponse {
              poolAssignmentJobs += poolAssignmentJob {
                shardIndex = 0
                encryptedDek = DEK_SHARD0
              }
              nextPageToken = ""
            }
        }
      val ruml =
        mock<RawImpressionUploadModelLineServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
            listRawImpressionUploadModelLinesResponse {
              rawImpressionUploadModelLines +=
                parent(RawImpressionUploadModelLine.State.POOL_ASSIGNING, listOf(7L, 11L))
              nextPageToken = ""
            }
          onBlocking { markRawImpressionUploadModelLineRanking(any(), any()) } doReturn
            parent(RawImpressionUploadModelLine.State.RANKING, listOf(7L, 11L))
        }
      val accumulator =
        SubpoolFingerprintsAccumulator().apply {
          add(7L, 1L, 1)
          add(11L, 2L, 1)
        }

      assigner(store, paj, ruml, ranker, workItems, accumulator = accumulator).assign()

      // One WorkItem per subpool; the union of their stamped ranked sizes must match the labeler
      // one-to-one (FakePoolEmitLabeler returns 1000 + offset), so a key-swap or dropped offset
      // fails.
      val captor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItems, times(2)) { createWorkItem(captor.capture(), any()) }
      val stamped: Map<Long, Int> =
        captor.allValues
          .map {
            it.workItem.workItemParams
              .unpack(WorkItemParams::class.java)
              .appParams
              .unpack(VidRankBuilderParams::class.java)
          }
          .flatMap { it.subpoolRankedSizesMap.entries }
          .associate { it.key to it.value }
      assertThat(stamped).containsExactly(7L, 1007, 11L, 1011)
    }

  private object FakePoolEmitLabeler : PoolEmitLabeler {
    override fun emit(input: LabelerInput): List<Long> = emptyList()

    // Distinct per offset so a key-swap / dropped-offset regression in the rankedSize stamping is
    // observable (a constant would hide it).
    override fun rankedSize(poolOffset: Long): Int = (1000 + poolOffset).toInt()
  }
}
