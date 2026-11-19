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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.util.UUID
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityMonitor
import org.wfanet.measurement.edpaggregator.dataavailability.ManagedDoneObjectDisposition
import org.wfanet.measurement.edpaggregator.dataavailability.ManagedDoneObjectResolver
import org.wfanet.measurement.edpaggregator.rawimpressions.CommittedOutputResolver
import org.wfanet.measurement.edpaggregator.rawimpressions.OutputManifestStore
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
import org.wfanet.measurement.edpaggregator.v1alpha.outputManifestRecord
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

class CommittedOutputMonitorTest {
  private val provider = "dataProviders/provider"
  private val upload = "$provider/rawImpressionUploads/upload"
  private val model = "modelProviders/provider/modelSuites/suite/modelLines/line"
  private val date = LocalDate.of(2026, 3, 10)
  private val inputName = "$upload/files/file"
  private var value = VidLabelingJob.getDefaultInstance()
  private val throttler =
    object : Throttler {
      override suspend fun <T> onReady(block: suspend () -> T): T = block()
    }

  @get:Rule
  val server = GrpcTestServerRule {
    addService(
      object : VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase() {
        override suspend fun listVidLabelingJobs(
          request: ListVidLabelingJobsRequest
        ): ListVidLabelingJobsResponse = listVidLabelingJobsResponse { vidLabelingJobs += value }
      }
    )
    addService(
      object :
        RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase() {
        override suspend fun getRawImpressionUploadFile(
          request: GetRawImpressionUploadFileRequest
        ): RawImpressionUploadFile = rawImpressionUploadFile {
          name = request.name
          blobUri = "gs://input/raw.parquet"
          blobGeneration = 1L
          eventDate =
            com.google.type.date {
              year = date.year
              month = date.monthValue
              day = date.dayOfMonth
            }
        }
      }
    )
  }

  private suspend fun monitor(
    empty: Boolean = false,
    synchronized: Boolean = true,
    incomplete: Boolean = false,
    external: Boolean = false,
    replaceSelected: Boolean = false,
  ): DataAvailabilityMonitor.ModelLineStatus {
    val storage = InMemoryStorageClient()
    val root = "gs://bucket/output"
    val id = UUID.randomUUID().toString()
    val owner = "$upload/vidLabelingJobs/job"
    val records = mutableListOf<OutputManifestRecord>()
    val key = "output/model-line/line/$date/.attempts/$id/data"
    val store = OutputManifestStore(storage, root)
    if (!empty) {
      val data = storage.writeBlob(key, ByteString.copyFromUtf8("data"))
      val metadata = storage.writeBlob(key + ".metadata.binpb", ByteString.copyFromUtf8("metadata"))
      if (synchronized)
        storage.updateBlobMetadata(
          key + ".metadata.binpb",
          metadata =
            mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE),
        )
      records += outputManifestRecord {
        labeledBlob =
          OutputManifestRecordKt.labeledBlob {
            rawImpressionUploadFile = inputName
            modelLine = model
            eventDate =
              com.google.type.date {
                year = date.year
                month = date.monthValue
                day = date.dayOfMonth
              }
            impressions = store.reference("gs://bucket/$key", data)
            this.metadata = store.reference("gs://bucket/$key.metadata.binpb", metadata)
          }
      }
    }
    records += outputManifestRecord {
      fileCompleted =
        OutputManifestRecordKt.fileCompleted {
          rawImpressionUploadFile = inputName
          modelLine = model
        }
    }
    val ref =
      store.write(
        "$root/.attempts/$id/manifest",
        OutputManifestRecordKt.header {
          phase = OutputManifestRecord.Header.Phase.VID_LABELING
          rawImpressionUpload = upload
          this.owner = owner
          modelLines += model
          attemptId = id
        },
        flowOf(*records.toTypedArray()),
      )
    value = vidLabelingJob {
      name = owner
      state = if (incomplete) VidLabelingJob.State.CREATED else VidLabelingJob.State.SUCCEEDED
      cmmsModelLines += model
      rawImpressionUploadFiles += inputName
      if (!incomplete) outputManifest = ref
    }
    if (replaceSelected) {
      storage.writeBlob(key, ByteString.copyFromUtf8("replacement"))
    }
    val orphanKey = "output/model-line/line/$date/.attempts/orphan/data"
    storage.writeBlob(orphanKey, ByteString.copyFromUtf8("orphan-data"))
    storage.writeBlob(orphanKey + ".metadata.binpb", ByteString.copyFromUtf8("orphan-metadata"))
    if (external && synchronized)
      storage.updateBlobMetadata(
        orphanKey + ".metadata.binpb",
        metadata =
          mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE),
      )
    val doneKey =
      if (external) "output/model-line/line/$date/done"
      else "output/" + LabeledImpressionsBlobKeys.forDone(upload, model, date)
    storage.writeBlob(doneKey, ByteString.EMPTY)
    storage.updateBlobMetadata(
      doneKey,
      metadata =
        mapOf(
          DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE,
          DataAvailabilityBlobs.SYNC_ID_KEY to "published",
          DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY to "published",
        ),
    )
    val reader =
      object : ManagedDoneObjectResolver {
        override val resolvesCommittedOutputs = true

        override suspend fun modelLineFor(eventDate: LocalDate, blobKey: String) = model

        override suspend fun resolve(modelLine: String, eventDate: LocalDate, blobKey: String) =
          ManagedDoneObjectDisposition.CURRENT

        override suspend fun committedOutputs(
          modelLine: String,
          eventDate: LocalDate,
          blobKey: String,
          storageRoot: String,
        ) =
          CommittedOutputResolver(
              VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(server.channel),
              RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
                server.channel
              ),
              OutputManifestStore(storage, storageRoot),
              throttler,
            )
            .resolve(upload, modelLine)
      }
    return DataAvailabilityMonitor(
        storage,
        "output",
        setOf(checkNotNull(ModelLineKey.fromName(model))),
        impressionMetadataStub = null,
        dataProviderName = provider,
        clock = { Instant.parse("2026-03-11T00:00:00Z") },
        managedDoneObjectResolver = { reader },
        outputStorageRoot = root,
      )
      .checkGaps(Duration.ZERO)
      .statuses
      .single()
  }

  @Test
  fun `selected published output remains healthy despite unsynced orphan sidecars`() =
    runBlocking<Unit> {
      val status = monitor()
      assertThat(status.healthyDates).containsExactly(date)
      assertThat(status.lateArrivingDates).isEmpty()
      assertThat(status.zeroImpressionDates).isEmpty()
    }

  @Test
  fun `successful empty output ignores orphan data when classifying zero impressions`() =
    runBlocking<Unit> {
      val status = monitor(empty = true)
      assertThat(status.zeroImpressionDates).containsExactly(date)
      assertThat(status.lateArrivingDates).isEmpty()
      assertThat(status.healthyDates).isEmpty()
    }

  @Test
  fun `unsynced selected sidecars still cause a late-arrival alert`() =
    runBlocking<Unit> {
      val status = monitor(synchronized = false)
      assertThat(status.lateArrivingDates).containsExactly(date)
      assertThat(status.healthyDates).isEmpty()
    }

  @Test
  fun `incomplete job output cannot be reported as healthy or successfully empty`() =
    runBlocking<Unit> {
      val status = monitor(incomplete = true)
      assertThat(status.unprocessedDoneDates).containsExactly(date)
      assertThat(status.healthyDates).isEmpty()
      assertThat(status.zeroImpressionDates).isEmpty()
    }

  @Test
  fun `replaced selected generations cannot be reported healthy or empty`() =
    runBlocking<Unit> {
      val status = monitor(replaceSelected = true)
      assertThat(status.healthyDates).isEmpty()
      assertThat(status.zeroImpressionDates).isEmpty()
      assertThat(status.unprocessedDoneDates).containsExactly(date)
      assertThat(status.lateArrivingDates).containsExactly(date)
    }

  @Test
  fun `external canonical snapshots retain valid attempt-named files`() =
    runBlocking<Unit> {
      val status = monitor(external = true)
      assertThat(status.healthyDates).containsExactly(date)
      assertThat(status.zeroImpressionDates).isEmpty()
      assertThat(status.lateArrivingDates).isEmpty()
    }
}
