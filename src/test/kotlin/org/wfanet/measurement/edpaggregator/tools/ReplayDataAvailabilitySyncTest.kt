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
import io.grpc.netty.NettyServerBuilder
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit.SECONDS
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
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchUpdateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchUpdateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule

@RunWith(JUnit4::class)
class ReplayDataAvailabilitySyncTest {
  @get:Rule val tempDir = TemporaryFolder()

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
  fun `main prefix replay processes changes written after completion blob`() {
    runDataAvailabilityOnlyReplay(arrayOf("--blob-prefix=$EDP_IMPRESSION_PATH"))
  }

  @Test
  fun `main explicit completion blob replay processes changes written after completion blob`() {
    runDataAvailabilityOnlyReplay(arrayOf("--done-blob-uri=gs://$BUCKET_NAME/$DONE_BLOB_KEY"))
  }

  private fun runDataAvailabilityOnlyReplay(selectionArgs: Array<String>) {
    val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
      onBlocking {
          replaceDataAvailabilityIntervals(any<ReplaceDataAvailabilityIntervalsRequest>())
        }
        .thenReturn(DataProvider.getDefaultInstance())
    }
    val existingMetadata = impressionMetadata {
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
        .thenReturn(listImpressionMetadataResponse { impressionMetadata += existingMetadata })
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
    val server: Server =
      NettyServerBuilder.forPort(0)
        .sslContext(serverCerts.toServerTlsContext())
        .addService(dataProvidersServiceMock)
        .addService(impressionMetadataServiceMock)
        .addService(activitiesServiceMock)
        .build()
        .start()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      seedInitialFinalizedFolder()
      val doneGeneration =
        checkNotNull(storageEmulator.storage.get(BUCKET_NAME, DONE_BLOB_KEY)).generation
      overwriteAndAddBlobsWithoutCompletionBlob()
      assertThat(storageEmulator.storage.get(BUCKET_NAME, DONE_BLOB_KEY).generation)
        .isEqualTo(doneGeneration)
      val configFile = writeConfigFile(validConfig(includeActivityPublishing = true))
      val apiTarget = "localhost:${server.port}"

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          connectionArgs(configFile, apiTarget) +
            "--mode=${ReplayDataAvailabilitySync.Mode.DATA_AVAILABILITY_ONLY}" +
            selectionArgs +
            storageArgs,
          ::main,
        )

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
    // CommandLineTesting uses this security manager to observe the real main function's exit code.
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()

    private const val MODULE_REPO_NAME = "wfa_measurement_system"
    private const val BUCKET_NAME = "replay-test"
    private const val DATA_PROVIDER_NAME = "dataProviders/test-provider"
    private const val EDP_IMPRESSION_PATH = "edp/test/vid-labeled-impressions"
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
