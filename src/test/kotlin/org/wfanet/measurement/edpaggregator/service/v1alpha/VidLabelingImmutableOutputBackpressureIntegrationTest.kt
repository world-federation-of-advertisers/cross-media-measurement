/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.edpaggregator.vidlabeler.LabelingOutputAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.storage.BlobChangedException

internal class VidLabelingImmutableOutputBackpressureIntegrationTest :
  VidLabelingPipelineTestHarness(PipelineHarnessConfig(immutableOutputPublication = true)) {
  @Test
  fun `data collision releases a full writer channel and remains retryable`() =
    runBlocking<Unit> {
      setVisibleModelLines(DIRECT_MODEL_LINE)
      withholdNextVidLabelerPublications()
      val people = List(1_024) { "person-" + it }
      writeRawFile("full-channel-collision", "input.parquet", people)
      val generation = finalizeRawUpload("full-channel-collision")
      workItemTransport.awaitIdle()
      val upload = listUploads().single { it.doneBlobGeneration == generation }
      val work = vidLabelerWorkItems().single()
      val params =
        work.workItemParams
          .unpack(WorkItemParams::class.java)
          .appParams
          .unpack(VidLabelerParams::class.java)
      val jobs = VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel)
      val get = getVidLabelingJobRequest { name = params.vidLabelingJob }
      val job = jobs.getVidLabelingJob(get)
      val file = listUploadFiles(upload.name).single()
      val root = params.vidLabeledImpressionsStorageParams.impressionsBlobPrefix
      val id = "00000000-0000-4000-8000-000000000099"
      forcedLabelingAttemptId = id
      val uri =
        LabelingOutputAttempt(fileStorage, root, job.name, params.modelLinesList, id).use { attempt
          ->
          LabeledImpressionsBlobKeys.forAttemptInputUri(
            root,
            file.blobUri,
            DIRECT_MODEL_LINE,
            EVENT_DATE,
            attempt.path(file.name),
          )
        }
      fileStorage.writeBlob(blobKey(uri), ByteString.copyFromUtf8("orphan"))
      val original = fileStorage.getFreshnessToken(blobKey(uri))
      val mark =
        "wfa.measurement.edpaggregator.v1alpha.VidLabelingJobService/MarkVidLabelingJobSucceeded"
      val calls = responseGates.callCount(mark)
      assertFailsWith<BlobChangedException> {
        withTimeout(30.seconds) { runVidLabelerDirect(work) }
      }
      assertThat(jobs.getVidLabelingJob(get).state).isEqualTo(VidLabelingJob.State.CREATED)
      assertThat(responseGates.callCount(mark)).isEqualTo(calls)
      assertThat(fileStorage.getFreshnessToken(blobKey(uri))).isEqualTo(original)
      forcedLabelingAttemptId = null
      runVidLabelerDirect(work)
      awaitPipelineIdle()
      assertThat(jobs.getVidLabelingJob(get).state).isEqualTo(VidLabelingJob.State.SUCCEEDED)
      assertThat(readLabeledPeople(DIRECT_MODEL_LINE).map { it.personId })
        .containsExactlyElementsIn(people)
      assertThat(fileStorage.getFreshnessToken(blobKey(uri))).isEqualTo(original)
    }
}
