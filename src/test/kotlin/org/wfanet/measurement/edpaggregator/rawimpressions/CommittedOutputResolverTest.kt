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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecord
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecordKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.outputBlobReference
import org.wfanet.measurement.edpaggregator.v1alpha.outputManifestRecord
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class CommittedOutputResolverTest {
  private val upload = "dataProviders/provider/rawImpressionUploads/upload"
  private val owner = "$upload/vidLabelingJobs/job"
  private val inputName = "$upload/rawImpressionUploadFiles/file"
  private val model = "modelProviders/provider/modelSuites/suite/modelLines/line"
  private val attempt = "00000000-0000-4000-8000-000000000001"
  private val manifestUri = "gs://bucket/output/$attempt/manifest"
  private val input = rawImpressionUploadFile {
    name = inputName
    blobUri = "gs://input/raw.parquet"
    eventDate = eventDate.toBuilder().setYear(2026).setMonth(11).setDay(19).build()
  }
  private val header =
    OutputManifestRecordKt.header {
      phase = OutputManifestRecord.Header.Phase.VID_LABELING
      rawImpressionUpload = upload
      this.owner = this@CommittedOutputResolverTest.owner
      modelLines += model
      attemptId = attempt
    }
  private val complete = outputManifestRecord {
    fileCompleted =
      OutputManifestRecordKt.fileCompleted {
        rawImpressionUploadFile = inputName
        modelLine = model
      }
  }
  private val labeled = outputManifestRecord {
    labeledBlob =
      OutputManifestRecordKt.labeledBlob {
        rawImpressionUploadFile = inputName
        modelLine = model
        eventDate = input.eventDate
        impressions = outputBlobReference {
          blobUri = "gs://bucket/output/$attempt/data"
          blobGeneration = 1L
        }
        metadata = outputBlobReference {
          blobUri = "gs://bucket/output/$attempt/data.metadata.binpb"
          blobGeneration = 2L
        }
      }
  }
  private val storage = InMemoryStorageClient()
  private val store = OutputManifestStore(storage, "gs://bucket/output")
  private val jobs =
    object : VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase() {
      var value: VidLabelingJob = VidLabelingJob.getDefaultInstance()

      override suspend fun listVidLabelingJobs(
        request: ListVidLabelingJobsRequest
      ): ListVidLabelingJobsResponse = listVidLabelingJobsResponse { vidLabelingJobs += value }
    }
  private val files =
    object :
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase() {
      override suspend fun getRawImpressionUploadFile(
        request: GetRawImpressionUploadFileRequest
      ): RawImpressionUploadFile = input
    }
  @get:Rule
  val server = GrpcTestServerRule {
    addService(jobs)
    addService(files)
  }

  private fun resolver() =
    CommittedOutputResolver(
      VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(server.channel),
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
        server.channel
      ),
      store,
      VidLabelingRpcThrottlersTestHelper.alwaysReady().metadataRead,
    )

  private suspend fun publish(vararg records: OutputManifestRecord) {
    val reference = store.write(manifestUri, header, flowOf(*records))
    jobs.value = vidLabelingJob {
      name = owner
      state = VidLabelingJob.State.SUCCEEDED
      cmmsModelLines += model
      rawImpressionUploadFiles += inputName
      outputManifest = reference
    }
  }

  @Test
  fun `resolve accepts completed empty files without inventing an output blob`() =
    runBlocking<Unit> {
      publish(complete)
      val result = resolver().resolve(upload, model)
      assertThat(result.hasCommittedManifests).isTrue()
      assertThat(result.labeledBlobs).isEmpty()
      assertThat(result.legacyFiles).isEmpty()
    }

  @Test
  fun `resolve returns the selected matched data and sidecar versions`() =
    runBlocking<Unit> {
      publish(labeled, complete)
      val result = resolver().resolve(upload, model)
      assertThat(result.labeledBlobs.single().impressions.blobGeneration).isEqualTo(1L)
      assertThat(result.labeledBlobs.single().metadata.blobGeneration).isEqualTo(2L)
      assertThat(result.labeledBlobs.single().impressions.blobUri)
        .isEqualTo(labeled.labeledBlob.impressions.blobUri)
    }

  @Test
  fun `resolve rejects missing file completion rather than accepting partial empty output`() =
    runBlocking<Unit> {
      publish()
      assertFailsWith<IllegalStateException> { resolver().resolve(upload, model) }
    }

  @Test
  fun `resolve rejects duplicate completed input scopes`() =
    runBlocking<Unit> {
      publish(complete, complete)
      assertFailsWith<IllegalStateException> { resolver().resolve(upload, model) }
    }

  @Test
  fun `resolve rejects a labeled output for the wrong registered event date`() =
    runBlocking<Unit> {
      val wrongDate =
        labeled
          .toBuilder()
          .setLabeledBlob(
            labeled.labeledBlob
              .toBuilder()
              .setEventDate(input.eventDate.toBuilder().setDay(20))
              .build()
          )
          .build()
      publish(wrongDate, complete)
      assertFailsWith<IllegalStateException> { resolver().resolve(upload, model) }
    }

  @Test
  fun `resolve retains explicit legacy input scope when the job has no manifest`() =
    runBlocking<Unit> {
      jobs.value = vidLabelingJob {
        name = owner
        state = VidLabelingJob.State.SUCCEEDED
        cmmsModelLines += model
        rawImpressionUploadFiles += inputName
      }
      val result = resolver().resolve(upload, model)
      assertThat(result.hasCommittedManifests).isFalse()
      assertThat(result.legacyFiles.single().blobUri).isEqualTo(input.blobUri)
    }
}
