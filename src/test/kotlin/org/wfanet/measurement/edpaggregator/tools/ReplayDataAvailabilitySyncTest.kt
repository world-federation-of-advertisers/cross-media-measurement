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

import com.google.cloud.storage.BlobInfo
import com.google.common.truth.Truth.assertThat
import com.google.devtools.build.runfiles.Runfiles
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Server
import io.grpc.Status
import io.grpc.netty.NettyServerBuilder
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import java.util.concurrent.TimeUnit.SECONDS
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.BatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.edpaggregator.v1alpha.AcquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchUpdateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ReleaseDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RenewDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchUpdateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule

@RunWith(JUnit4::class)
class ReplayDataAvailabilitySyncTest {
  @get:Rule val tempDir = TemporaryFolder()

  @Test
  fun `main rejects equal or nested impression paths`() {
    for (internalPath in listOf(EDP_IMPRESSION_PATH, "$EDP_IMPRESSION_PATH/internal", "edp")) {
      val configFile =
        writeConfigFile(
          validConfig(includeActivityPublishing = false) +
            "\nvid_labeled_impression_path: \"$internalPath\"\n"
        )
      val output =
        CommandLineTesting.capturingOutput(
          requiredArgs(
            configFile,
            ReplayDataAvailabilitySync.Mode.DATA_AVAILABILITY_ONLY,
            UNREACHABLE_API_TARGET,
          ),
          ::main,
        )
      CommandLineTesting.assertThat(output).status().isNotEqualTo(0)
      CommandLineTesting.assertThat(output)
        .err()
        .contains("Configured impression paths must not overlap")
    }
  }

  @Test
  fun `main exits nonzero when mode is missing`() {
    val configFile = writeConfigFile(validConfig(includeActivityPublishing = false))

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        connectionArgs(configFile, UNREACHABLE_API_TARGET) + "--blob-prefix=$EDP_IMPRESSION_PATH",
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
    CommandLineTesting.assertThat(capturedOutput).err().contains("--mode")
  }

  @Test
  fun `main exits nonzero when activity mode configuration is missing`() {
    val configFile = writeConfigFile(validConfig(includeActivityPublishing = false))

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        requiredArgs(
          configFile,
          ReplayDataAvailabilitySync.Mode.EVENT_GROUP_ACTIVITY_ONLY,
          UNREACHABLE_API_TARGET,
        ),
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
    CommandLineTesting.assertThat(capturedOutput)
      .err()
      .contains("event_group_activity_publishing must be set")
  }

  @Test
  fun `main exits zero in activity only mode when no completion blobs exist`() {
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val configFile = writeConfigFile(validConfig(includeActivityPublishing = true))

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          requiredArgs(
            configFile,
            ReplayDataAvailabilitySync.Mode.EVENT_GROUP_ACTIVITY_ONLY,
            UNREACHABLE_API_TARGET,
          ) + storageArgs,
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    } finally {
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main replays contributor prefix in another bucket`() {
    runContributorReplay()
  }

  @Test
  fun `main exits zero in full sync mode without activity configuration`() {
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val configFile = writeConfigFile(validConfig(includeActivityPublishing = false))

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          requiredArgs(
            configFile,
            ReplayDataAvailabilitySync.Mode.FULL_SYNC,
            UNREACHABLE_API_TARGET,
          ) + storageArgs,
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    } finally {
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main prefix replay rejects late changes then preserves finalized generations`() {
    runDataAvailabilityOnlyReplay(arrayOf("--blob-prefix=$EDP_IMPRESSION_PATH"))
  }

  @Test
  fun `main explicit replay rejects late changes then preserves finalized generations`() {
    runDataAvailabilityOnlyReplay(arrayOf("--done-blob-uri=gs://$BUCKET_NAME/$DONE_BLOB_KEY"))
  }

  @Test
  fun `main internal prefix replay preserves exact done generation`() {
    runConfiguredPathReplay(
      listOf(INTERNAL_IMPRESSION_PATH),
      arrayOf("--blob-prefix=gs://$BUCKET_NAME/$INTERNAL_IMPRESSION_PATH"),
    )
  }

  @Test
  fun `main internal prefix replay accepts the VID Labeler done filename`() {
    val marker = MANAGED_DONE_FILE_NAME
    runConfiguredPathReplay(
      listOf(INTERNAL_IMPRESSION_PATH),
      arrayOf("--blob-prefix=gs://$BUCKET_NAME/$INTERNAL_IMPRESSION_PATH"),
      doneFileName = marker,
    )
  }

  @Test
  fun `main explicit internal replay accepts the VID Labeler done filename`() {
    val marker = MANAGED_DONE_FILE_NAME
    runConfiguredPathReplay(
      listOf(INTERNAL_IMPRESSION_PATH),
      arrayOf(
        "--done-blob-uri=gs://$BUCKET_NAME/$INTERNAL_IMPRESSION_PATH/model-line/model-line-1/2026-01-01/$marker"
      ),
      doneFileName = marker,
    )
  }

  @Test
  fun `main replays a valid managed zero-output marker without configured model names`() {
    for (selection in
      listOf(
        arrayOf("--blob-prefix=gs://$BUCKET_NAME/$INTERNAL_IMPRESSION_PATH"),
        arrayOf(
          "--done-blob-uri=gs://$BUCKET_NAME/$INTERNAL_IMPRESSION_PATH/model-line/model-line-1/2026-01-01/$MANAGED_DONE_FILE_NAME"
        ),
      )) {
      runConfiguredPathReplay(
        listOf(INTERNAL_IMPRESSION_PATH),
        selection,
        MANAGED_DONE_FILE_NAME,
        zeroOutput = true,
      )
    }
  }

  @Test
  fun `main rejects an evicted managed replay without restoring metadata`() {
    runConfiguredPathReplay(
      listOf(INTERNAL_IMPRESSION_PATH),
      arrayOf("--blob-prefix=gs://$BUCKET_NAME/$INTERNAL_IMPRESSION_PATH"),
      MANAGED_DONE_FILE_NAME,
      obsoleteOwner = true,
    )
  }

  @Test
  fun `main rejects a VID Labeler completion filename at the external root`() {
    val configFile = writeConfigFile(validConfig(includeActivityPublishing = false))
    val output =
      CommandLineTesting.capturingOutput(
        connectionArgs(configFile, UNREACHABLE_API_TARGET) +
          "--mode=${ReplayDataAvailabilitySync.Mode.DATA_AVAILABILITY_ONLY}" +
          "--done-blob-uri=gs://$BUCKET_NAME/$EDP_IMPRESSION_PATH/model-line/model-line-1/2026-01-01/$MANAGED_DONE_FILE_NAME",
        ::main,
      )
    CommandLineTesting.assertThat(output).status().isNotEqualTo(0)
    CommandLineTesting.assertThat(output).err().contains("wrong completion filename")
  }

  @Test
  fun `main mixed completion blob replay selects each configured path`() {
    val paths = listOf(EDP_IMPRESSION_PATH, INTERNAL_IMPRESSION_PATH)
    runConfiguredPathReplay(
      paths,
      paths
        .map { "--done-blob-uri=gs://$BUCKET_NAME/$it/model-line/model-line-1/2026-01-01/done" }
        .toTypedArray(),
    )
  }

  @Test
  fun `main rejects replay selections outside the configured paths and bucket`() {
    val configFile =
      writeConfigFile(
        validConfig(includeActivityPublishing = false) +
          "\nvid_labeled_impression_path: \"$INTERNAL_IMPRESSION_PATH\"\n"
      )
    val selections =
      listOf(
        "--done-blob-uri=gs://other-bucket/$INTERNAL_IMPRESSION_PATH/model-line/ml/2026-01-01/done" to
          "done-blob-uri must use the configured GCS bucket",
        "--done-blob-uri=gs://$BUCKET_NAME/other-path/model-line/ml/2026-01-01/done" to
          "done-blob-uri must be below a configured impression path",
        "--blob-prefix=gs://other-bucket/$INTERNAL_IMPRESSION_PATH" to
          "blob-prefix must use the configured GCS bucket",
        "--blob-prefix=other-path" to "blob-prefix must be a configured impression path",
      )
    for ((selection, error) in selections) {
      val output =
        CommandLineTesting.capturingOutput(
          connectionArgs(configFile, UNREACHABLE_API_TARGET) +
            "--mode=${ReplayDataAvailabilitySync.Mode.DATA_AVAILABILITY_ONLY}" +
            selection,
          ::main,
        )
      CommandLineTesting.assertThat(output).status().isNotEqualTo(0)
      CommandLineTesting.assertThat(output).err().contains(error)
    }
  }

  @Test
  fun `main logs RPC failure and completion blob in activity only mode`() {
    assertReplayRpcFailureLogged(ReplayDataAvailabilitySync.Mode.EVENT_GROUP_ACTIVITY_ONLY)
  }

  @Test
  fun `main logs RPC failure and completion blob in full sync mode`() {
    assertReplayRpcFailureLogged(ReplayDataAvailabilitySync.Mode.FULL_SYNC)
  }

  private fun assertReplayRpcFailureLogged(mode: ReplayDataAvailabilitySync.Mode) {
    val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase = mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenThrow(
          Status.PERMISSION_DENIED.withDescription("Metadata access denied").asRuntimeException()
        )
    }
    val leaseService = ReplayLeaseService()
    val server: Server =
      NettyServerBuilder.forPort(0)
        .sslContext(serverCerts.toServerTlsContext())
        .addService(impressionMetadataServiceMock)
        .addService(leaseService)
        .build()
        .start()
    val logRecords = mutableListOf<LogRecord>()
    val logHandler =
      object : Handler() {
        override fun publish(record: LogRecord) {
          logRecords += record
        }

        override fun flush() {}

        override fun close() {}
      }
    val logger = Logger.getLogger("org.wfanet.measurement.edpaggregator.tools")
    logger.addHandler(logHandler)
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      seedInitialFinalizedFolder()
      val configFile = writeConfigFile(validConfig(includeActivityPublishing = true))

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          requiredArgs(configFile, mode, "localhost:${server.port}") + storageArgs,
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(1)
      val errorRecord = logRecords.single { it.message.contains("replay error") }
      assertThat(errorRecord.message).contains("gs://$BUCKET_NAME/$DONE_BLOB_KEY")
      assertThat(errorRecord.level).isEqualTo(Level.WARNING)
      val exception = checkNotNull(errorRecord.thrown)
      assertThat(exception.cause).isNotNull()
      assertThat(Status.fromThrowable(exception).code).isEqualTo(Status.Code.PERMISSION_DENIED)
      if (mode == ReplayDataAvailabilitySync.Mode.FULL_SYNC) {
        assertThat(leaseService.acquiredNames).hasSize(1)
        assertThat(leaseService.releasedNames).containsExactlyElementsIn(leaseService.acquiredNames)
      } else {
        assertThat(leaseService.acquiredNames).isEmpty()
      }
    } finally {
      logger.removeHandler(logHandler)
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  private fun runDataAvailabilityOnlyReplay(selectionArgs: Array<String>) {
    val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
      onBlocking {
          replaceDataAvailabilityIntervals(any<ReplaceDataAvailabilityIntervalsRequest>())
        }
        .thenReturn(DataProvider.getDefaultInstance())
    }
    var existingMetadata = impressionMetadata {
      name = "$DATA_PROVIDER_NAME/impressionMetadata/existing"
      blobUri = "gs://$BUCKET_NAME/$METADATA_BLOB_KEY"
      blobTypeUrl = BLOB_DETAILS_TYPE_URL
      eventGroupReferenceId = INITIAL_EVENT_GROUP_REFERENCE_ID
      modelLine = MODEL_LINE
      interval = interval {
        startTime = timestamp { seconds = INITIAL_INTERVAL_START_SECONDS }
        endTime = timestamp { seconds = INITIAL_INTERVAL_END_SECONDS }
      }
    }
    val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase = mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { listImpressionMetadataResponse { impressionMetadata += existingMetadata } }
      onBlocking { batchCreateImpressionMetadata(any<BatchCreateImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchCreateImpressionMetadataRequest>(0)
          batchCreateImpressionMetadataResponse {
            impressionMetadata +=
              request.requestsList.mapIndexed { index, createRequest ->
                createRequest.impressionMetadata.copy {
                  name = "${request.parent}/impressionMetadata/im-$index"
                }
              }
          }
        }
      onBlocking { batchUpdateImpressionMetadata(any<BatchUpdateImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchUpdateImpressionMetadataRequest>(0)
          batchUpdateImpressionMetadataResponse {
            impressionMetadata += request.requestsList.map { it.impressionMetadata }
          }
        }
      onBlocking { computeModelLineBounds(any<ComputeModelLineBoundsRequest>()) }
        .thenReturn(
          computeModelLineBoundsResponse {
            modelLineBounds += modelLineBoundMapEntry {
              key = MODEL_LINE
              value = interval {
                startTime = timestamp { seconds = UPDATED_INTERVAL_START_SECONDS }
                endTime = timestamp { seconds = NEW_INTERVAL_END_SECONDS }
              }
            }
          }
        )
    }
    val activitiesServiceMock: EventGroupActivitiesCoroutineImplBase = mockService {
      onBlocking { batchUpdateEventGroupActivities(any<BatchUpdateEventGroupActivitiesRequest>()) }
        .thenReturn(batchUpdateEventGroupActivitiesResponse {})
    }
    val leaseService = ReplayLeaseService()
    val server: Server =
      NettyServerBuilder.forPort(0)
        .sslContext(serverCerts.toServerTlsContext())
        .addService(dataProvidersServiceMock)
        .addService(impressionMetadataServiceMock)
        .addService(leaseService)
        .addService(activitiesServiceMock)
        .build()
        .start()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      seedInitialFinalizedFolder()
      existingMetadata =
        existingMetadata.copy {
          metadataBlobGeneration =
            storageEmulator.storage.get(BUCKET_NAME, METADATA_BLOB_KEY).generation
          impressionsBlobGeneration =
            storageEmulator.storage.get(BUCKET_NAME, IMPRESSIONS_BLOB_KEY).generation
        }
      val originalDoneGeneration =
        checkNotNull(storageEmulator.storage.get(BUCKET_NAME, DONE_BLOB_KEY)).generation
      overwriteAndAddBlobsWithoutCompletionBlob()
      assertThat(storageEmulator.storage.get(BUCKET_NAME, DONE_BLOB_KEY).generation)
        .isEqualTo(originalDoneGeneration)
      val configFile = writeConfigFile(validConfig(includeActivityPublishing = true))
      val apiTarget = "localhost:${server.port}"

      val replayArgs =
        connectionArgs(configFile, apiTarget) +
          "--mode=DATA_AVAILABILITY_ONLY" +
          selectionArgs +
          storageArgs
      val rejected = CommandLineTesting.capturingOutput(replayArgs, ::main)
      CommandLineTesting.assertThat(rejected).status().isNotEqualTo(0)
      verifyBlocking(impressionMetadataServiceMock, times(0)) {
        batchCreateImpressionMetadata(any())
      }
      verifyBlocking(impressionMetadataServiceMock, times(0)) {
        batchUpdateImpressionMetadata(any())
      }
      verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }

      val doneGeneration =
        storageEmulator.storage
          .create(BlobInfo.newBuilder(BUCKET_NAME, DONE_BLOB_KEY).build(), byteArrayOf(1))
          .generation
      assertThat(doneGeneration).isNotEqualTo(originalDoneGeneration)
      val capturedOutput = CommandLineTesting.capturingOutput(replayArgs, ::main)
      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
      val availabilityRequest = argumentCaptor<ReplaceDataAvailabilityIntervalsRequest>()
      verifyBlocking(dataProvidersServiceMock, times(1)) {
        replaceDataAvailabilityIntervals(availabilityRequest.capture())
      }
      assertThat(availabilityRequest.firstValue.name).isEqualTo(DATA_PROVIDER_NAME)
      assertThat(availabilityRequest.firstValue.dataAvailabilityIntervalsList.map { it.key })
        .containsExactly(MODEL_LINE)

      val createRequest = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchCreateImpressionMetadata(createRequest.capture())
      }
      val createdMetadata = createRequest.firstValue.requestsList.single().impressionMetadata
      assertThat(createdMetadata.blobUri).isEqualTo("gs://$BUCKET_NAME/$NEW_METADATA_BLOB_KEY")
      assertThat(createdMetadata.eventGroupReferenceId).isEqualTo(NEW_EVENT_GROUP_REFERENCE_ID)
      assertThat(createdMetadata.outputDoneBlobGeneration).isEqualTo(doneGeneration)
      assertThat(createdMetadata.metadataBlobGeneration)
        .isEqualTo(storageEmulator.storage.get(BUCKET_NAME, NEW_METADATA_BLOB_KEY).generation)
      assertThat(createdMetadata.impressionsBlobGeneration)
        .isEqualTo(storageEmulator.storage.get(BUCKET_NAME, NEW_IMPRESSIONS_BLOB_KEY).generation)
      assertThat(leaseService.acquiredNames).hasSize(2)
      val leaseName = leaseService.acquiredNames.last()
      assertThat(leaseName).startsWith("$DATA_PROVIDER_NAME/dataAvailabilitySyncLeases/")
      assertThat(createRequest.firstValue.dataAvailabilitySyncLease).isEqualTo(leaseName)
      assertThat(createRequest.firstValue.requestsList.single().dataAvailabilitySyncLease)
        .isEqualTo(leaseName)

      val updateRequest = argumentCaptor<BatchUpdateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        batchUpdateImpressionMetadata(updateRequest.capture())
      }
      val updatedMetadata = updateRequest.firstValue.requestsList.single().impressionMetadata
      assertThat(updatedMetadata.name).isEqualTo(existingMetadata.name)
      assertThat(updatedMetadata.blobUri).isEqualTo(existingMetadata.blobUri)
      assertThat(updatedMetadata.eventGroupReferenceId).isEqualTo(UPDATED_EVENT_GROUP_REFERENCE_ID)
      assertThat(updatedMetadata.interval.startTime.seconds)
        .isEqualTo(UPDATED_INTERVAL_START_SECONDS)
      assertThat(updatedMetadata.outputDoneBlobGeneration).isEqualTo(doneGeneration)
      assertThat(updatedMetadata.metadataBlobGeneration)
        .isEqualTo(storageEmulator.storage.get(BUCKET_NAME, METADATA_BLOB_KEY).generation)
      assertThat(updatedMetadata.impressionsBlobGeneration)
        .isEqualTo(storageEmulator.storage.get(BUCKET_NAME, IMPRESSIONS_BLOB_KEY).generation)
      assertThat(updateRequest.firstValue.dataAvailabilitySyncLease).isEqualTo(leaseName)
      assertThat(updateRequest.firstValue.requestsList.single().dataAvailabilitySyncLease)
        .isEqualTo(leaseName)
      assertThat(leaseService.renewalCount).isGreaterThan(0)
      assertThat(leaseService.releasedNames).containsExactlyElementsIn(leaseService.acquiredNames)

      verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
      verifyBlocking(activitiesServiceMock, never()) { batchUpdateEventGroupActivities(any()) }
      val doneMetadata =
        checkNotNull(storageEmulator.storage.get(BUCKET_NAME, DONE_BLOB_KEY).metadata)
      assertThat(doneMetadata)
        .containsEntry(DataAvailabilityBlobs.SYNCED_BY_KEY, DataAvailabilityBlobs.SYNCED_BY_VALUE)
      assertThat(doneMetadata[DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY])
        .isEqualTo(doneMetadata[DataAvailabilityBlobs.SYNC_ID_KEY])
    } finally {
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  private fun runConfiguredPathReplay(
    impressionPaths: List<String>,
    selectionArgs: Array<String>,
    doneFileName: String = "done",
    zeroOutput: Boolean = false,
    obsoleteOwner: Boolean = false,
  ) {
    val createdMetadata = mutableListOf<ImpressionMetadata>()
    val metadataService: ImpressionMetadataServiceCoroutineImplBase = mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          listImpressionMetadataResponse {
            impressionMetadata +=
              createdMetadata.filter {
                (request.filter.blobUriPrefix.isEmpty() ||
                  it.blobUri.startsWith(request.filter.blobUriPrefix)) &&
                  (request.filter.blobUrisList.isEmpty() ||
                    it.blobUri in request.filter.blobUrisList)
              }
          }
        }
      onBlocking { batchCreateImpressionMetadata(any<BatchCreateImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchCreateImpressionMetadataRequest>(0)
          val created =
            request.requestsList.mapIndexed { index, createRequest ->
              createRequest.impressionMetadata.copy {
                name =
                  "${request.parent}/impressionMetadata/created-${createdMetadata.size + index}"
                state = ImpressionMetadata.State.ACTIVE
              }
            }
          createdMetadata += created
          batchCreateImpressionMetadataResponse { impressionMetadata += created }
        }
      onBlocking { computeModelLineBounds(any<ComputeModelLineBoundsRequest>()) }
        .thenReturn(
          computeModelLineBoundsResponse {
            modelLineBounds += modelLineBoundMapEntry {
              key = MODEL_LINE
              value = interval {
                startTime = timestamp { seconds = INITIAL_INTERVAL_START_SECONDS }
                endTime = timestamp { seconds = INITIAL_INTERVAL_END_SECONDS }
              }
            }
          }
        )
    }
    val dataProvidersService: DataProvidersCoroutineImplBase = mockService {
      onBlocking {
          replaceDataAvailabilityIntervals(any<ReplaceDataAvailabilityIntervalsRequest>())
        }
        .thenReturn(DataProvider.getDefaultInstance())
    }
    val leaseService = ReplayLeaseService()
    val managedOwners =
      ManagedDoneObjectTestServices(
          DATA_PROVIDER_NAME,
          listOf(MODEL_LINE),
          LocalDate.of(2026, 1, 1),
          MANAGED_RAW_INPUT_URI,
        )
        .apply {
          if (obsoleteOwner)
            ownerState =
              org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine.State.FAILED
        }
    val server =
      managedOwners
        .addTo(NettyServerBuilder.forPort(0))
        .sslContext(serverCerts.toServerTlsContext())
        .addService(metadataService)
        .addService(dataProvidersService)
        .addService(leaseService)
        .build()
        .start()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val generations =
        impressionPaths.associate { path ->
          val folder = "$path/model-line/model-line-1/2026-01-01"
          val dataKey =
            if (doneFileName == "done") "$folder/impressions"
            else
              "$path/" +
                LabeledImpressionsBlobKeys.forInput(
                  MANAGED_RAW_INPUT_URI,
                  MODEL_LINE,
                  LocalDate.of(2026, 1, 1),
                )
          val metadataKey = "$folder/metadata.json"
          if (!zeroOutput) {
            writeBlobDetails(
              metadataKey,
              dataKey,
              INITIAL_EVENT_GROUP_REFERENCE_ID,
              "2026-01-01T00:00:00Z",
              "2026-01-02T00:00:00Z",
            )
            storageEmulator.storage.create(
              BlobInfo.newBuilder(BUCKET_NAME, dataKey).build(),
              byteArrayOf(1),
            )
          }
          val done =
            storageEmulator.storage.create(
              BlobInfo.newBuilder(BUCKET_NAME, "$folder/$doneFileName").build(),
              byteArrayOf(1),
            )
          "gs://$BUCKET_NAME/$metadataKey" to done.generation
        }
      val configFile =
        writeConfigFile(
          validConfig(includeActivityPublishing = false) +
            "\nvid_labeled_impression_path: \"$INTERNAL_IMPRESSION_PATH\"\n"
        )
      val output =
        CommandLineTesting.capturingOutput(
          connectionArgs(configFile, "localhost:${server.port}") +
            "--mode=${ReplayDataAvailabilitySync.Mode.DATA_AVAILABILITY_ONLY}" +
            selectionArgs +
            storageArgs,
          ::main,
        )
      if (zeroOutput || obsoleteOwner) {
        CommandLineTesting.assertThat(output).status().isEqualTo(if (obsoleteOwner) 1 else 0)
        verifyBlocking(metadataService, times(0)) { batchCreateImpressionMetadata(any()) }
        verifyBlocking(dataProvidersService, times(0)) { replaceDataAvailabilityIntervals(any()) }
        assertThat(leaseService.releasedNames).containsExactlyElementsIn(leaseService.acquiredNames)
        return
      }
      CommandLineTesting.assertThat(output).status().isEqualTo(0)
      val requests = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(metadataService, times(impressionPaths.size)) {
        batchCreateImpressionMetadata(requests.capture())
      }
      val created = requests.allValues.flatMap { it.requestsList }.map { it.impressionMetadata }
      assertThat(created.map { it.blobUri }).containsExactlyElementsIn(generations.keys)
      for (metadata in created) {
        assertThat(metadata.outputDoneBlobGeneration)
          .isEqualTo(generations.getValue(metadata.blobUri))
      }
      assertThat(leaseService.acquiredNames).hasSize(impressionPaths.size)
      assertThat(leaseService.releasedNames).containsExactlyElementsIn(leaseService.acquiredNames)
      assertThat(requests.allValues.map { it.dataAvailabilitySyncLease })
        .containsExactlyElementsIn(leaseService.acquiredNames)
      for (request in requests.allValues) {
        assertThat(request.requestsList.map { it.dataAvailabilitySyncLease }.distinct())
          .containsExactly(request.dataAvailabilitySyncLease)
      }
      verifyBlocking(dataProvidersService, times(impressionPaths.size)) {
        replaceDataAvailabilityIntervals(any())
      }
    } finally {
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main replays contributor exact URI in another bucket`() {
    runContributorReplay(exactUri = true)
  }

  @Test
  fun `main replays contributor prefix with a nondefault path in the default bucket`() {
    runContributorReplay(replacementBucket = BUCKET_NAME)
  }

  @Test
  fun `main replays contributor exact URI with a nondefault path in the default bucket`() {
    runContributorReplay(replacementBucket = BUCKET_NAME, exactUri = true)
  }

  @Test
  fun `main replays contributor prefix in another bucket using the standard internal path`() {
    runContributorReplay(replacementPath = INTERNAL_IMPRESSION_PATH)
  }

  @Test
  fun `main replays contributor exact URI in another bucket using the standard internal path`() {
    runContributorReplay(replacementPath = INTERNAL_IMPRESSION_PATH, exactUri = true)
  }

  @Test
  fun `main replays contributor legacy done marker in another bucket`() {
    runContributorReplay(doneFileName = "done")
  }

  @Test
  fun `main replays contributor with an empty base path by bucket prefix`() {
    runContributorReplay(replacementPath = "", broadPrefix = true)
  }

  @Test
  fun `main replays contributor with an empty base path by exact URI`() {
    runContributorReplay(replacementPath = "", exactUri = true)
  }

  @Test
  fun `main replays contributor shared root without duplicate selection`() {
    runContributorReplay(
      replacementBucket = BUCKET_NAME,
      replacementPath = EDP_IMPRESSION_PATH,
      broadPrefix = true,
      preSynchronized = true,
    )
  }

  @Test
  fun `main rejects contributor external hash marker under a shared root`() {
    runContributorReplay(
      replacementBucket = BUCKET_NAME,
      replacementPath = EDP_IMPRESSION_PATH,
      historical = true,
      exactUri = true,
      rejectMarker = true,
    )
  }

  @Test
  fun `main keeps contributor external snapshot validation under a shared root`() {
    runContributorReplay(
      replacementBucket = BUCKET_NAME,
      replacementPath = EDP_IMPRESSION_PATH,
      historical = true,
      doneFileName = "done",
      mutateExternalDuringBounds = true,
    )
  }

  @Test
  fun `main replays contributor prefix without selecting another model line`() {
    runContributorReplay(broadPrefix = true, includeOtherModelLine = true)
  }

  @Test
  fun `main rejects contributor exact URI for an unconfigured model line`() {
    runContributorReplay(exactUri = true, includeOtherModelLine = true, selectOtherModelLine = true)
  }

  @Test
  fun `main replays contributor valid hash while ignoring malformed completion names`() {
    runContributorReplay(
      extraMarkerNames =
        listOf(
          "done-${"a".repeat(63)}",
          "done-${"a".repeat(65)}",
          "done-${"A".repeat(64)}",
          "done-${"g".repeat(64)}",
          "done-unrelated",
        )
    )
  }

  @Test
  fun `main replays contributor repeated exact URI only once`() {
    runContributorReplay(exactUri = true, duplicateExactUri = true)
  }

  @Test
  fun `main replays contributor selection in full sync mode`() {
    runContributorReplay(mode = ReplayDataAvailabilitySync.Mode.FULL_SYNC)
  }

  @Test
  fun `main replays contributor hash selection in activity only mode`() {
    runContributorReplay(mode = ReplayDataAvailabilitySync.Mode.EVENT_GROUP_ACTIVITY_ONLY)
  }

  @Test
  fun `main keeps contributor ordinary external mapping without cutovers`() {
    runContributorReplay(cutoverEnabled = false, doneFileName = "done")
  }

  @Test
  fun `main rejects contributor cutover without its configured storage source`() {
    runContributorReplay(
      configuredReplacementLine = "$MODEL_LINE-missing",
      expectedConfigurationError = "every cutover contributor",
    )
  }

  @Test
  fun `main rejects contributor model lines with an ambiguous replay directory`() {
    runContributorReplay(
      replacementBucket = BUCKET_NAME,
      replacementPath = EDP_IMPRESSION_PATH,
      configuredHistoricalLine =
        "modelProviders/other-provider/modelSuites/other-suite/modelLines/model-line-1",
      expectedConfigurationError = "distinct model-line replay paths",
    )
  }

  private fun runContributorReplay(
    replacementBucket: String = CONTRIBUTOR_BUCKET_NAME,
    replacementPath: String = CONTRIBUTOR_IMPRESSION_PATH,
    exactUri: Boolean = false,
    doneFileName: String = MANAGED_DONE_FILE_NAME,
    broadPrefix: Boolean = false,
    preSynchronized: Boolean = false,
    historical: Boolean = false,
    rejectMarker: Boolean = false,
    mutateExternalDuringBounds: Boolean = false,
    includeOtherModelLine: Boolean = false,
    selectOtherModelLine: Boolean = false,
    extraMarkerNames: List<String> = emptyList(),
    duplicateExactUri: Boolean = false,
    cutoverEnabled: Boolean = true,
    configuredReplacementLine: String = MODEL_LINE,
    configuredHistoricalLine: String = MODEL_LINE.substringBeforeLast("/") + "/historical",
    expectedConfigurationError: String? = null,
    mode: ReplayDataAvailabilitySync.Mode = ReplayDataAvailabilitySync.Mode.DATA_AVAILABILITY_ONLY,
  ) {
    val historicalLine = configuredHistoricalLine
    val selectedBucket = if (historical) BUCKET_NAME else replacementBucket
    val selectedPath = if (historical) EDP_IMPRESSION_PATH else replacementPath
    val selectedLine = if (historical) historicalLine else MODEL_LINE
    fun folder(path: String, line: String, date: String = "2026-01-01"): String {
      val relative = "model-line/${line.substringAfterLast('/')}/$date"
      return if (path.isEmpty()) relative else "$path/$relative"
    }
    val selectedFolder = folder(selectedPath, selectedLine)
    val dataKey =
      if (doneFileName == "done") "$selectedFolder/impressions"
      else {
        val relative =
          LabeledImpressionsBlobKeys.forInput(
            MANAGED_RAW_INPUT_URI,
            selectedLine,
            LocalDate.of(2026, 1, 1),
          )
        if (selectedPath.isEmpty()) relative else "$selectedPath/$relative"
      }
    val metadataKey = "$selectedFolder/metadata.json"
    val doneKey = "$selectedFolder/$doneFileName"
    val registered = mutableMapOf<String, ImpressionMetadata>()
    val listRequests = mutableListOf<ListImpressionMetadataRequest>()
    var mutated = false
    val metadataService: ImpressionMetadataServiceCoroutineImplBase = mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          listRequests += request
          listImpressionMetadataResponse {
            impressionMetadata +=
              registered.values.filter {
                (request.filter.blobUriPrefix.isEmpty() ||
                  it.blobUri.startsWith(request.filter.blobUriPrefix)) &&
                  (request.filter.blobUrisList.isEmpty() ||
                    it.blobUri in request.filter.blobUrisList)
              }
          }
        }
      onBlocking { batchCreateImpressionMetadata(any<BatchCreateImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchCreateImpressionMetadataRequest>(0)
          batchCreateImpressionMetadataResponse {
            impressionMetadata +=
              request.requestsList.map { create ->
                create.impressionMetadata
                  .copy {
                    name = "$DATA_PROVIDER_NAME/impressionMetadata/created-${registered.size}"
                    state = ImpressionMetadata.State.ACTIVE
                  }
                  .also { registered[it.blobUri] = it }
              }
          }
        }
      onBlocking { batchUpdateImpressionMetadata(any<BatchUpdateImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchUpdateImpressionMetadataRequest>(0)
          batchUpdateImpressionMetadataResponse {
            impressionMetadata +=
              request.requestsList.map { update ->
                update.impressionMetadata.also { registered[it.blobUri] = it }
              }
          }
        }
      onBlocking { computeModelLineBounds(any<ComputeModelLineBoundsRequest>()) }
        .thenAnswer {
          if (mutateExternalDuringBounds && !mutated) {
            storageEmulator.storage.create(
              BlobInfo.newBuilder(selectedBucket, dataKey).build(),
              byteArrayOf(2),
            )
            mutated = true
          }
          computeModelLineBoundsResponse {
            for ((line, entries) in registered.values.groupBy { it.modelLine }) {
              modelLineBounds += modelLineBoundMapEntry {
                key = line
                value = entries.first().interval
              }
            }
          }
        }
    }
    val dataProvidersService: DataProvidersCoroutineImplBase = mockService {
      onBlocking {
          replaceDataAvailabilityIntervals(any<ReplaceDataAvailabilityIntervalsRequest>())
        }
        .thenReturn(DataProvider.getDefaultInstance())
    }
    val activitiesService: EventGroupActivitiesCoroutineImplBase = mockService {
      onBlocking { batchUpdateEventGroupActivities(any<BatchUpdateEventGroupActivitiesRequest>()) }
        .thenReturn(batchUpdateEventGroupActivitiesResponse {})
    }
    val leaseService = ReplayLeaseService()
    val server =
      ManagedDoneObjectTestServices(
          DATA_PROVIDER_NAME,
          listOf(MODEL_LINE, historicalLine),
          LocalDate.of(2026, 1, 1),
          MANAGED_RAW_INPUT_URI,
        )
        .addTo(NettyServerBuilder.forPort(0))
        .sslContext(serverCerts.toServerTlsContext())
        .addService(metadataService)
        .addService(dataProvidersService)
        .addService(activitiesService)
        .addService(leaseService)
        .build()
        .start()
    val buckets = setOf(BUCKET_NAME, replacementBucket)
    buckets.forEach(storageEmulator::createBucket)
    try {
      fun writeSyncedDone(bucket: String, key: String) {
        storageEmulator.storage.create(
          BlobInfo.newBuilder(bucket, key)
            .setMetadata(
              mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE)
            )
            .build(),
          byteArrayOf(1),
        )
      }
      if (cutoverEnabled) {
        writeSyncedDone(
          BUCKET_NAME,
          folder(EDP_IMPRESSION_PATH, historicalLine, "2025-12-31") + "/done",
        )
        writeSyncedDone(BUCKET_NAME, folder(EDP_IMPRESSION_PATH, historicalLine) + "/done")
        if (historical) {
          writeSyncedDone(
            replacementBucket,
            folder(replacementPath, MODEL_LINE) + "/$MANAGED_DONE_FILE_NAME",
          )
        }
      }
      val details =
        """
        {
          "blobUri": "gs://$selectedBucket/$dataKey",
          "eventGroupReferenceId": "$INITIAL_EVENT_GROUP_REFERENCE_ID",
          "modelLine": "$selectedLine",
          "interval": {
            "startTime": "2026-01-01T00:00:00Z",
            "endTime": "2026-01-02T00:00:00Z"
          }
        }
      """
          .trimIndent()
      storageEmulator.storage.create(
        BlobInfo.newBuilder(selectedBucket, metadataKey).build(),
        details.toByteArray(),
      )
      storageEmulator.storage.create(
        BlobInfo.newBuilder(selectedBucket, dataKey).build(),
        byteArrayOf(1),
      )
      val done =
        storageEmulator.storage.create(
          BlobInfo.newBuilder(selectedBucket, doneKey)
            .setMetadata(
              if (preSynchronized)
                mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE)
              else emptyMap()
            )
            .build(),
          byteArrayOf(1),
        )
      if (preSynchronized) {
        val metadata = impressionMetadata {
          name = "$DATA_PROVIDER_NAME/impressionMetadata/existing"
          blobUri = "gs://$selectedBucket/$metadataKey"
          blobTypeUrl = BLOB_DETAILS_TYPE_URL
          eventGroupReferenceId = INITIAL_EVENT_GROUP_REFERENCE_ID
          modelLine = selectedLine
          interval = interval {
            startTime = timestamp { seconds = INITIAL_INTERVAL_START_SECONDS }
            endTime = timestamp { seconds = INITIAL_INTERVAL_END_SECONDS }
          }
          outputDoneBlobGeneration = done.generation
          state = ImpressionMetadata.State.ACTIVE
        }
        registered[metadata.blobUri] = metadata
      }
      for (name in extraMarkerNames) {
        storageEmulator.storage.create(
          BlobInfo.newBuilder(selectedBucket, "$selectedFolder/$name").build(),
          byteArrayOf(1),
        )
      }
      val otherFolder = folder(replacementPath, MODEL_LINE + "0")
      if (includeOtherModelLine) {
        storageEmulator.storage.create(
          BlobInfo.newBuilder(replacementBucket, "$otherFolder/done").build(),
          byteArrayOf(1),
        )
      }
      val configFile =
        writeConfigFile(
          validConfig(
            includeActivityPublishing =
              mode == ReplayDataAvailabilitySync.Mode.EVENT_GROUP_ACTIVITY_ONLY
          ) +
            """

        vid_labeled_impression_path: "$INTERNAL_IMPRESSION_PATH"
        model_line_data_sources {
          key: "$MODEL_LINE"
          value {
            storage { gcs { project_id: "test-project" bucket_name: "$replacementBucket" } }
            edp_impression_path: "$replacementPath"
          }
        }
        """
              .trimIndent() +
            if (cutoverEnabled)
              """

        model_line_data_sources {
          key: "$historicalLine"
          value {
            storage { gcs { project_id: "test-project" bucket_name: "$BUCKET_NAME" } }
            edp_impression_path: "$EDP_IMPRESSION_PATH"
          }
        }
        model_line_cutovers {
          external_model_line: "$historicalLine"
          historical_model_line: "$historicalLine"
          replacement_model_line: "$configuredReplacementLine"
          cutover_date { year: 2026 month: 1 day: 1 }
          legacy_required_through_date { year: 2026 month: 1 day: 2 }
          maximum_report_duration { seconds: 86400 }
        }
        """
                .trimIndent()
            else
              """

        model_line_map {
          key: "$MODEL_LINE"
          value { model_lines: "$MODEL_LINE-alias" }
        }
        """
                .trimIndent()
        )
      val selectedUri =
        if (selectOtherModelLine) "gs://$replacementBucket/$otherFolder/done"
        else "gs://$selectedBucket/$doneKey"
      val selectionArgs =
        if (exactUri) {
          List(if (duplicateExactUri) 2 else 1) { "--done-blob-uri=$selectedUri" }.toTypedArray()
        } else {
          val prefix = if (broadPrefix) selectedPath else selectedFolder
          arrayOf("--blob-prefix=gs://$selectedBucket/$prefix")
        }
      val output =
        CommandLineTesting.capturingOutput(
          connectionArgs(configFile, "localhost:${server.port}") +
            "--mode=$mode" +
            selectionArgs +
            storageArgs,
          ::main,
        )
      val rejected = rejectMarker || selectOtherModelLine || expectedConfigurationError != null
      CommandLineTesting.assertThat(output)
        .status()
        .isEqualTo(if (rejected || mutateExternalDuringBounds) 1 else 0)
      if (rejected) {
        CommandLineTesting.assertThat(output)
          .err()
          .contains(
            expectedConfigurationError
              ?: if (selectOtherModelLine) "below a configured impression path"
              else "wrong completion filename"
          )
        assertThat(leaseService.acquiredNames).isEmpty()
        assertThat(registered).isEmpty()
        verifyBlocking(dataProvidersService, never()) { replaceDataAvailabilityIntervals(any()) }
        return
      }
      if (mode == ReplayDataAvailabilitySync.Mode.EVENT_GROUP_ACTIVITY_ONLY) {
        assertThat(listRequests.map { it.filter.blobUriPrefix })
          .containsExactly("gs://$selectedBucket/$selectedFolder/")
        assertThat(leaseService.acquiredNames).isEmpty()
        assertThat(registered).isEmpty()
        verifyBlocking(dataProvidersService, never()) { replaceDataAvailabilityIntervals(any()) }
        verifyBlocking(metadataService, never()) { batchCreateImpressionMetadata(any()) }
        assertThat(storageEmulator.storage.get(selectedBucket, doneKey).metadata.orEmpty())
          .doesNotContainKey(DataAvailabilityBlobs.SYNCED_BY_KEY)
        return
      }
      val expectedSynchronizations =
        if (
          broadPrefix && replacementBucket == BUCKET_NAME && replacementPath == EDP_IMPRESSION_PATH
        )
          3
        else 1
      if (mutateExternalDuringBounds) {
        assertThat(mutated).isTrue()
        verifyBlocking(dataProvidersService, never()) { replaceDataAvailabilityIntervals(any()) }
        assertThat(storageEmulator.storage.get(selectedBucket, doneKey).metadata.orEmpty())
          .doesNotContainKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
      } else {
        val publications = argumentCaptor<ReplaceDataAvailabilityIntervalsRequest>()
        verifyBlocking(dataProvidersService, times(expectedSynchronizations)) {
          replaceDataAvailabilityIntervals(publications.capture())
        }
        for (publication in publications.allValues) {
          val entry = publication.dataAvailabilityIntervalsList.single()
          assertThat(entry.key)
            .isEqualTo(if (cutoverEnabled) historicalLine else "$MODEL_LINE-alias")
          assertThat(entry.value.startTime.seconds)
            .isEqualTo(
              if (cutoverEnabled) INITIAL_INTERVAL_START_SECONDS - 86400
              else INITIAL_INTERVAL_START_SECONDS
            )
          assertThat(entry.value.endTime.seconds).isEqualTo(INITIAL_INTERVAL_END_SECONDS)
        }
        val completion = storageEmulator.storage.get(selectedBucket, doneKey)
        assertThat(completion.generation).isEqualTo(done.generation)
        val completionMetadata = checkNotNull(completion.metadata)
        assertThat(completionMetadata[DataAvailabilityBlobs.SYNCED_BY_KEY])
          .isEqualTo(DataAvailabilityBlobs.SYNCED_BY_VALUE)
        assertThat(completionMetadata[DataAvailabilityBlobs.SYNC_ID_KEY]).isNotEmpty()
        assertThat(completionMetadata[DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY])
          .isEqualTo(completionMetadata[DataAvailabilityBlobs.SYNC_ID_KEY])
      }
      assertThat(registered.keys).containsExactly("gs://$selectedBucket/$metadataKey")
      val metadata = registered.values.single()
      assertThat(metadata.modelLine).isEqualTo(selectedLine)
      assertThat(metadata.outputDoneBlobGeneration).isEqualTo(done.generation)
      if (!mutateExternalDuringBounds) {
        if (historical || !cutoverEnabled) {
          assertThat(metadata.metadataBlobGeneration)
            .isEqualTo(storageEmulator.storage.get(selectedBucket, metadataKey).generation)
          assertThat(metadata.impressionsBlobGeneration)
            .isEqualTo(storageEmulator.storage.get(selectedBucket, dataKey).generation)
        } else {
          assertThat(metadata.metadataBlobGeneration).isEqualTo(0L)
          assertThat(metadata.impressionsBlobGeneration).isEqualTo(0L)
        }
      }
      assertThat(leaseService.acquiredNames).hasSize(expectedSynchronizations)
      assertThat(leaseService.releasedNames).containsExactlyElementsIn(leaseService.acquiredNames)
      verifyBlocking(activitiesService, never()) { batchUpdateEventGroupActivities(any()) }
    } finally {
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      buckets.forEach(storageEmulator::deleteBucketRecursive)
    }
  }

  private class ReplayLeaseService : DataAvailabilitySyncLeaseServiceCoroutineImplBase() {
    val acquiredNames = mutableListOf<String>()
    val releasedNames = mutableListOf<String>()
    var renewalCount = 0
      private set

    private var currentLease: DataAvailabilitySyncLease? = null

    override suspend fun acquireDataAvailabilitySyncLease(
      request: AcquireDataAvailabilitySyncLeaseRequest
    ): DataAvailabilitySyncLease {
      assertThat(currentLease).isNull()
      assertThat(request.requestId).isNotEmpty()
      acquiredNames += request.name
      return dataAvailabilitySyncLease {
          name = request.name
          state = DataAvailabilitySyncLease.State.ACTIVE
          etag = "lease-0"
        }
        .also { currentLease = it }
    }

    override suspend fun renewDataAvailabilitySyncLease(
      request: RenewDataAvailabilitySyncLeaseRequest
    ): DataAvailabilitySyncLease {
      val lease = checkNotNull(currentLease)
      assertThat(request.name).isEqualTo(lease.name)
      assertThat(request.etag).isEqualTo(lease.etag)
      assertThat(request.requestId).isNotEmpty()
      renewalCount++
      return lease.copy { etag = "lease-$renewalCount" }.also { currentLease = it }
    }

    override suspend fun releaseDataAvailabilitySyncLease(
      request: ReleaseDataAvailabilitySyncLeaseRequest
    ): DataAvailabilitySyncLease {
      val lease = checkNotNull(currentLease)
      assertThat(request.name).isEqualTo(lease.name)
      assertThat(request.etag).isEqualTo(lease.etag)
      assertThat(request.requestId).isNotEmpty()
      releasedNames += request.name
      currentLease = null
      return lease.copy { state = DataAvailabilitySyncLease.State.RELEASED }
    }
  }

  private val storageArgs: Array<String>
    get() =
      arrayOf(
        "--storage-api-endpoint=${storageEmulator.storage.options.host}",
        "--throttler-minimum-interval=0s",
      )

  private fun seedInitialFinalizedFolder() {
    writeBlobDetails(
      METADATA_BLOB_KEY,
      IMPRESSIONS_BLOB_KEY,
      INITIAL_EVENT_GROUP_REFERENCE_ID,
      "2026-01-01T00:00:00Z",
      "2026-01-02T00:00:00Z",
    )
    storageEmulator.storage.create(
      BlobInfo.newBuilder(BUCKET_NAME, IMPRESSIONS_BLOB_KEY).build(),
      byteArrayOf(1),
    )
    storageEmulator.storage.create(
      BlobInfo.newBuilder(BUCKET_NAME, DONE_BLOB_KEY).build(),
      byteArrayOf(1),
    )
  }

  private fun overwriteAndAddBlobsWithoutCompletionBlob() {
    writeBlobDetails(
      METADATA_BLOB_KEY,
      IMPRESSIONS_BLOB_KEY,
      UPDATED_EVENT_GROUP_REFERENCE_ID,
      "2026-01-01T06:00:00Z",
      "2026-01-02T06:00:00Z",
    )
    storageEmulator.storage.create(
      BlobInfo.newBuilder(BUCKET_NAME, IMPRESSIONS_BLOB_KEY).build(),
      byteArrayOf(2),
    )
    writeBlobDetails(
      NEW_METADATA_BLOB_KEY,
      NEW_IMPRESSIONS_BLOB_KEY,
      NEW_EVENT_GROUP_REFERENCE_ID,
      "2026-01-02T00:00:00Z",
      "2026-01-03T00:00:00Z",
    )
    storageEmulator.storage.create(
      BlobInfo.newBuilder(BUCKET_NAME, NEW_IMPRESSIONS_BLOB_KEY).build(),
      byteArrayOf(3),
    )
  }

  private fun writeBlobDetails(
    metadataBlobKey: String,
    impressionsBlobKey: String,
    eventGroupReferenceId: String,
    startTime: String,
    endTime: String,
  ) {
    val blobDetailsJson =
      """
      {
        "blobUri": "gs://$BUCKET_NAME/$impressionsBlobKey",
        "eventGroupReferenceId": "$eventGroupReferenceId",
        "modelLine": "$MODEL_LINE",
        "interval": {
          "startTime": "$startTime",
          "endTime": "$endTime"
        }
      }
      """
        .trimIndent()
    storageEmulator.storage.create(
      BlobInfo.newBuilder(BUCKET_NAME, metadataBlobKey).build(),
      blobDetailsJson.toByteArray(),
    )
  }

  private fun connectionArgs(configFile: File, apiTarget: String): Array<String> =
    arrayOf(
      "--config-file=${configFile.path}",
      "--kingdom-public-api-target=$apiTarget",
      "--impression-metadata-api-target=$apiTarget",
    )

  private fun requiredArgs(
    configFile: File,
    mode: ReplayDataAvailabilitySync.Mode,
    apiTarget: String,
  ): Array<String> =
    connectionArgs(configFile, apiTarget) +
      arrayOf("--mode=$mode", "--blob-prefix=$EDP_IMPRESSION_PATH")

  private fun validConfig(includeActivityPublishing: Boolean): String =
    buildString {
        appendLine(
          """
          data_provider: "$DATA_PROVIDER_NAME"
          data_availability_storage {
            gcs {
              project_id: "test-project"
              bucket_name: "$BUCKET_NAME"
            }
          }
          cmms_connection {
            cert_file_path: "$SECRETS_DIR/kingdom_tls.pem"
            private_key_file_path: "$SECRETS_DIR/kingdom_tls.key"
            cert_collection_file_path: "$SECRETS_DIR/kingdom_root.pem"
          }
          impression_metadata_storage_connection {
            cert_file_path: "$SECRETS_DIR/kingdom_tls.pem"
            private_key_file_path: "$SECRETS_DIR/kingdom_tls.key"
            cert_collection_file_path: "$SECRETS_DIR/kingdom_root.pem"
          }
          edp_impression_path: "$EDP_IMPRESSION_PATH"
          """
            .trimIndent()
        )
        if (includeActivityPublishing) {
          appendLine(
            """
            event_group_activity_publishing {
              market_time_zone { id: "UTC" }
              event_group_map_blob_uri: "gs://$BUCKET_NAME/event-group-map.binpb"
              event_group_map_storage {
                gcs {
                  project_id: "test-project"
                  bucket_name: "$BUCKET_NAME"
                }
              }
            }
            """
              .trimIndent()
          )
        }
      }
      .trim()

  private fun writeConfigFile(contents: String): File =
    tempDir
      .newFile("data-availability-sync-config-${tempDir.root.listFiles().size}.textproto")
      .apply { writeText(contents) }

  @Suppress("DEPRECATION")
  companion object {
    private const val MANAGED_RAW_INPUT_URI = "gs://raw/managed.parquet"
    private val MANAGED_DONE_FILE_NAME =
      LabeledImpressionsBlobKeys.forDone(
          "$DATA_PROVIDER_NAME/rawImpressionUploads/managed",
          MODEL_LINE,
          LocalDate.of(2026, 1, 1),
        )
        .substringAfterLast('/')

    // CommandLineTesting uses this security manager to observe the real main function's exit code.
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()

    private const val MODULE_REPO_NAME = "wfa_measurement_system"
    private const val BUCKET_NAME = "replay-test"
    private const val CONTRIBUTOR_BUCKET_NAME = "replay-contributor-test"
    private const val DATA_PROVIDER_NAME = "dataProviders/test-provider"
    private const val EDP_IMPRESSION_PATH = "edp/test/vid-labeled-impressions"
    private const val INTERNAL_IMPRESSION_PATH = "vid-labeled-impressions/edp/test"
    private const val CONTRIBUTOR_IMPRESSION_PATH = "edp/test/legacy-impressions"
    private const val DATE_FOLDER = "$EDP_IMPRESSION_PATH/model-line/model-line-1/2026-01-01"
    private const val METADATA_BLOB_KEY = "$DATE_FOLDER/metadata.json"
    private const val IMPRESSIONS_BLOB_KEY = "$DATE_FOLDER/data-campaign-1"
    private const val NEW_METADATA_BLOB_KEY = "$DATE_FOLDER/metadata-new.json"
    private const val NEW_IMPRESSIONS_BLOB_KEY = "$DATE_FOLDER/data-campaign-2"
    private const val DONE_BLOB_KEY = "$DATE_FOLDER/done"
    private const val MODEL_LINE =
      "modelProviders/model-provider-1/modelSuites/model-suite-1/modelLines/model-line-1"
    private const val BLOB_DETAILS_TYPE_URL =
      "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
    private const val INITIAL_EVENT_GROUP_REFERENCE_ID = "initial-reference-id"
    private const val UPDATED_EVENT_GROUP_REFERENCE_ID = "updated-reference-id"
    private const val NEW_EVENT_GROUP_REFERENCE_ID = "new-reference-id"
    private const val INITIAL_INTERVAL_START_SECONDS = 1767225600L
    private const val INITIAL_INTERVAL_END_SECONDS = 1767312000L
    private const val UPDATED_INTERVAL_START_SECONDS = 1767247200L
    private const val NEW_INTERVAL_END_SECONDS = 1767398400L
    private const val UNREACHABLE_API_TARGET = "localhost:1"
    private val RUNFILES = Runfiles.preload().withSourceRepository("")
    private val SECRETS_DIR: Path =
      checkNotNull(
        RUNFILES.getRuntimePath(
          Paths.get(MODULE_REPO_NAME, "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private val serverCerts: SigningCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("kingdom_root.pem").toFile(),
      )
  }
}
