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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub

private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/up1"
private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"
private const val RANKER_JOB = "dataProviders/dp/rawImpressionUploads/up1/rankerJobs/rj7"
private const val PARENT_NAME =
  "dataProviders/dp/rawImpressionUploads/up1/rawImpressionUploadModelLines/rl1"
private const val QUEUE = "queues/vid-labeler"

@RunWith(JUnit4::class)
class VidRankBuilderTest {
  private val subpoolMapBlobUris = mapOf(7L to "merged/subpool-7")
  private val subpoolRankedSizes = mapOf(7L to 100)

  private fun rankerMock(): SubpoolRanker = mock {
    onBlocking { rank(any(), any(), any()) } doReturn
      SubpoolRanker.Result(
        poolOffset = 7L,
        allocated = 1,
        renewed = 0,
        overflow = 0,
        freed = 0,
        cumulativeSize = 1,
      )
  }

  private fun rankerJobsMock(
    state: RankerJob.State = RankerJob.State.CREATED,
    isLastJob: Boolean = false,
  ): RankerJobServiceCoroutineStub = mock {
    onBlocking { getRankerJob(any(), any()) } doReturn
      rankerJob {
        name = RANKER_JOB
        this.state = state
        etag = "etag-1"
      }
    onBlocking { markRankerJobSucceeded(any(), any()) } doReturn
      markRankerJobSucceededResponse {
        rankerJob = rankerJob { name = RANKER_JOB }
        this.isLastJob = isLastJob
      }
    onBlocking { markRankerJobFailed(any(), any()) } doReturn rankerJob { name = RANKER_JOB }
    onBlocking { listRankerJobs(any(), any()) } doReturn
      listRankerJobsResponse {
        rankerJobs += rankerJob {
          name = RANKER_JOB
          this.state = RankerJob.State.SUCCEEDED
          cmmsModelLine = MODEL_LINE
        }
      }
  }

  private fun modelLinesMock(
    state: RawImpressionUploadModelLine.State = RawImpressionUploadModelLine.State.RANKING
  ): RawImpressionUploadModelLineServiceCoroutineStub = mock {
    onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
      listRawImpressionUploadModelLinesResponse {
        rawImpressionUploadModelLines += rawImpressionUploadModelLine {
          name = PARENT_NAME
          cmmsModelLine = MODEL_LINE
          this.state = state
        }
      }
    onBlocking { markRawImpressionUploadModelLineLabeling(any(), any()) } doReturn
      rawImpressionUploadModelLine { name = PARENT_NAME }
  }

  private fun vidLabelingJobsMock(): VidLabelingJobServiceCoroutineStub = mock {
    onBlocking { batchCreateVidLabelingJobs(any(), any()) } doReturn
      batchCreateVidLabelingJobsResponse {}
  }

  private fun filesMock(): RawImpressionUploadFileServiceCoroutineStub = mock {
    onBlocking { listRawImpressionUploadFiles(any(), any()) } doReturn
      listRawImpressionUploadFilesResponse {
        rawImpressionUploadFiles += rawImpressionUploadFile { name = "$UPLOAD/files/0" }
        rawImpressionUploadFiles += rawImpressionUploadFile { name = "$UPLOAD/files/1" }
        rawImpressionUploadFiles += rawImpressionUploadFile { name = "$UPLOAD/files/2" }
      }
  }

  private fun builder(
    subpoolRanker: SubpoolRanker,
    rankerJobsStub: RankerJobServiceCoroutineStub,
    modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub = modelLinesMock(),
    vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub = vidLabelingJobsMock(),
    filesStub: RawImpressionUploadFileServiceCoroutineStub = filesMock(),
  ) =
    VidRankBuilder(
      subpoolRanker = subpoolRanker,
      rankerJobsStub = rankerJobsStub,
      rawImpressionUploadModelLinesStub = modelLinesStub,
      vidLabelingJobsStub = vidLabelingJobsStub,
      rawImpressionUploadFilesStub = filesStub,
      workItemsStub = mock<WorkItemsCoroutineStub>(),
      rawImpressionUpload = UPLOAD,
      modelLine = MODEL_LINE,
      rankerJob = RANKER_JOB,
      subpoolMapBlobUris = subpoolMapBlobUris,
      subpoolRankedSizes = subpoolRankedSizes,
      totalShards = 2,
      vidLabelerQueue = QUEUE,
    )

  @Test
  fun `non-last job ranks its subpools and marks succeeded without fan-out`() = runBlocking {
    val ranker = rankerMock()
    val rankerJobs = rankerJobsMock(isLastJob = false)
    val vidLabelingJobs = vidLabelingJobsMock()
    val modelLines = modelLinesMock()

    val result = builder(ranker, rankerJobs, modelLines, vidLabelingJobs).run()

    assertThat(result.lastJobOut).isFalse()
    assertThat(result.subpoolsRanked).isEqualTo(1)
    verifyBlocking(ranker) { rank(7L, "merged/subpool-7", 100) }
    verifyBlocking(rankerJobs) { markRankerJobSucceeded(any(), any()) }
    verifyBlocking(vidLabelingJobs, never()) { batchCreateVidLabelingJobs(any(), any()) }
    verifyBlocking(modelLines, never()) { markRawImpressionUploadModelLineLabeling(any(), any()) }
  }

  @Test
  fun `last job out creates labeling jobs and flips parent to LABELING`() = runBlocking {
    val ranker = rankerMock()
    val rankerJobs = rankerJobsMock(isLastJob = true)
    val vidLabelingJobs = vidLabelingJobsMock()
    val modelLines = modelLinesMock(RawImpressionUploadModelLine.State.RANKING)

    val result = builder(ranker, rankerJobs, modelLines, vidLabelingJobs).run()

    assertThat(result.lastJobOut).isTrue()
    verifyBlocking(vidLabelingJobs) { batchCreateVidLabelingJobs(any(), any()) }
    verifyBlocking(modelLines) { markRawImpressionUploadModelLineLabeling(any(), any()) }
  }

  @Test
  fun `redelivery of an already-succeeded last job recovers the fan-out without re-ranking`() =
    runBlocking {
      val ranker = rankerMock()
      // Job already SUCCEEDED; all jobs succeeded; parent still RANKING -> recover last-job-out.
      val rankerJobs = rankerJobsMock(state = RankerJob.State.SUCCEEDED)
      val vidLabelingJobs = vidLabelingJobsMock()
      val modelLines = modelLinesMock(RawImpressionUploadModelLine.State.RANKING)

      val result = builder(ranker, rankerJobs, modelLines, vidLabelingJobs).run()

      assertThat(result.lastJobOut).isTrue()
      verifyBlocking(ranker, never()) { rank(any(), any(), any()) }
      verifyBlocking(rankerJobs, never()) { markRankerJobSucceeded(any(), any()) }
      verifyBlocking(vidLabelingJobs) { batchCreateVidLabelingJobs(any(), any()) }
    }

  @Test
  fun `a failing subpool marks the ranker job FAILED and rethrows`() = runBlocking {
    val ranker =
      mock<SubpoolRanker> {
        onBlocking { rank(any(), any(), any()) } doAnswer { throw IllegalStateException("boom") }
      }
    val rankerJobs = rankerJobsMock(state = RankerJob.State.CREATED)

    assertFailsWith<IllegalStateException> { builder(ranker, rankerJobs).run() }

    verifyBlocking(rankerJobs) { markRankerJobFailed(any(), any()) }
  }
}
