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
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit.SECONDS
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.edpaggregator.v1alpha.AcquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata.State.ACTIVE
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ReleaseDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RenewDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ValidateDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule

@RunWith(JUnit4::class)
class RecoverMissingImpressionMetadataTest {
  @get:Rule val tempDir = TemporaryFolder()

  private val leaseServiceMock: DataAvailabilitySyncLeaseServiceCoroutineImplBase = mockService {
    onBlocking { acquireDataAvailabilitySyncLease(any<AcquireDataAvailabilitySyncLeaseRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<AcquireDataAvailabilitySyncLeaseRequest>(0)
        dataAvailabilitySyncLease {
          name = request.name
          state = DataAvailabilitySyncLease.State.ACTIVE
          etag = "etag"
        }
      }
    onBlocking { renewDataAvailabilitySyncLease(any<RenewDataAvailabilitySyncLeaseRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<RenewDataAvailabilitySyncLeaseRequest>(0)
        dataAvailabilitySyncLease {
          name = request.name
          state = DataAvailabilitySyncLease.State.ACTIVE
          etag = "renewed-etag"
        }
      }
    onBlocking {
        validateDataAvailabilitySyncLease(any<ValidateDataAvailabilitySyncLeaseRequest>())
      }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<ValidateDataAvailabilitySyncLeaseRequest>(0)
        dataAvailabilitySyncLease {
          name = request.name
          state = DataAvailabilitySyncLease.State.ACTIVE
          etag = request.etag
        }
      }
    onBlocking { releaseDataAvailabilitySyncLease(any<ReleaseDataAvailabilitySyncLeaseRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<ReleaseDataAvailabilitySyncLeaseRequest>(0)
        dataAvailabilitySyncLease {
          name = request.name
          state = DataAvailabilitySyncLease.State.RELEASED
          etag = "released-etag"
        }
      }
  }

  @Test
  fun `main rejects equal or nested impression paths`() {
    for (internalPath in listOf(EDP_IMPRESSION_PATH, "$EDP_IMPRESSION_PATH/internal", "edp")) {
      val configFile =
        writeConfigFile(validConfig() + "\nvid_labeled_impression_path: \"$internalPath\"\n")
      val output =
        CommandLineTesting.capturingOutput(
          requiredArgs(configFile, apiTarget = "localhost:1", endDaysAgo = 0),
          ::main,
        )
      CommandLineTesting.assertThat(output).status().isNotEqualTo(0)
      CommandLineTesting.assertThat(output)
        .err()
        .contains("Configured impression paths must not overlap")
    }
  }

  @Test
  fun `main exits nonzero when flag is invalid`() {
    val capturedOutput = CommandLineTesting.capturingOutput(arrayOf("--invalid-option"), ::main)

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  @Test
  fun `main exits nonzero when end days ago is not specified`() {
    val configFile = writeConfigFile(validConfig())

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        connectionArgs(configFile, apiTarget = "localhost:1"),
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  @Test
  fun `main exits nonzero when exact dates and date range are both specified`() {
    val configFile = writeConfigFile(validConfig())

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        connectionArgs(configFile, apiTarget = "localhost:1") +
          arrayOf("--data-date=2000-01-01", "--end-days-ago=0"),
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  @Test
  fun `main exits nonzero when end days ago is outside lookback horizon`() {
    val configFile = writeConfigFile(validConfig())

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        requiredArgs(configFile, apiTarget = "localhost:1", endDaysAgo = 90),
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  @Test
  fun `main exits nonzero when impression metadata batch size is greater than 100`() {
    val configFile = writeConfigFile(validConfig())

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        requiredArgs(configFile, apiTarget = "localhost:1", endDaysAgo = 0) +
          "--impression-metadata-batch-size=101",
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
    CommandLineTesting.assertThat(capturedOutput)
      .err()
      .contains("--impression-metadata-batch-size must be between 1 and 100")
  }

  @Test
  fun `main exits nonzero when impression metadata batch size is zero`() {
    val configFile = writeConfigFile(validConfig())

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        requiredArgs(configFile, apiTarget = "localhost:1", endDaysAgo = 0) +
          "--impression-metadata-batch-size=0",
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
    CommandLineTesting.assertThat(capturedOutput)
      .err()
      .contains("--impression-metadata-batch-size must be between 1 and 100")
  }

  @Test
  fun `main exits nonzero when configuration is invalid`() {
    val configFile =
      writeConfigFile(
        """
        data_availability_storage {
          gcs { bucket_name: "$BUCKET_NAME" }
        }
        edp_impression_path: "$EDP_IMPRESSION_PATH"
        """
          .trimIndent()
      )

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        requiredArgs(configFile, apiTarget = "localhost:1", endDaysAgo = 0),
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  @Test
  fun `main exits nonzero when activity publishing config is missing`() {
    val configFile = writeConfigFile(validConfig())

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        requiredArgs(configFile, apiTarget = "localhost:1", endDaysAgo = 0) +
          "--publish-event-group-activities",
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
    CommandLineTesting.assertThat(capturedOutput)
      .err()
      .contains("event_group_activity_publishing must be set")
  }

  @Test
  fun `main exits zero when no date folders exist`() {
    val server = newServer()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val configFile = writeConfigFile(validConfig())

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          requiredArgs(configFile, apiTarget = "localhost:${server.port}", endDaysAgo = 0) +
            storageArgs,
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    } finally {
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main excludes date folders newer than end days ago`() {
    val server = newServer()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val futureFolderPrefix =
        "$EDP_IMPRESSION_PATH/model-line/model-line-1/${LocalDate.now(ZoneOffset.UTC).plusDays(1)}"
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$futureFolderPrefix/metadata-invalid.json").build(),
        "{".toByteArray(),
      )
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$futureFolderPrefix/done").build(),
        byteArrayOf(),
      )
      val configFile = writeConfigFile(validConfig())

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          requiredArgs(configFile, apiTarget = "localhost:${server.port}", endDaysAgo = 1) +
            arrayOf(
              "--storage-api-endpoint=${storageEmulator.storage.options.host}",
              "--lookback-days=3",
              "--throttler-minimum-interval=0s",
            ),
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    } finally {
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main includes date folder at end days ago boundary`() {
    val server = newServer()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val yesterdayFolderPrefix =
        "$EDP_IMPRESSION_PATH/model-line/model-line-1/${LocalDate.now(ZoneOffset.UTC).minusDays(1)}"
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$yesterdayFolderPrefix/metadata-invalid.json").build(),
        "{".toByteArray(),
      )
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$yesterdayFolderPrefix/done").build(),
        byteArrayOf(),
      )
      val configFile = writeConfigFile(validConfig())

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          requiredArgs(configFile, apiTarget = "localhost:${server.port}", endDaysAgo = 1) +
            arrayOf(
              "--storage-api-endpoint=${storageEmulator.storage.options.host}",
              "--lookback-days=2",
              "--throttler-minimum-interval=0s",
            ),
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
    } finally {
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main includes repeated exact dates and excludes unselected dates`() {
    val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase = mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenReturn(listImpressionMetadataResponse {})
    }
    val server: Server =
      NettyServerBuilder.forPort(0)
        .sslContext(serverCerts.toServerTlsContext())
        .addService(impressionMetadataServiceMock)
        .addService(leaseServiceMock)
        .build()
        .start()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      for (date in listOf("2000-01-01", "2000-01-02", "2000-01-03")) {
        storageEmulator.storage.create(
          BlobInfo.newBuilder(
              BUCKET_NAME,
              "$EDP_IMPRESSION_PATH/model-line/model-line-1/$date/placeholder",
            )
            .build(),
          byteArrayOf(),
        )
      }
      val configFile = writeConfigFile(validConfig())

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          connectionArgs(configFile, apiTarget = "localhost:${server.port}") +
            arrayOf(
              "--data-date=2000-01-01",
              "--data-date=2000-01-03",
              "--storage-api-endpoint=${storageEmulator.storage.options.host}",
              "--throttler-minimum-interval=0s",
            ),
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
      verifyBlocking(impressionMetadataServiceMock, times(2)) { listImpressionMetadata(any()) }
    } finally {
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main exits nonzero when recovery cannot register metadata`() {
    val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase = mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenReturn(listImpressionMetadataResponse {})
    }
    val server: Server =
      NettyServerBuilder.forPort(0)
        .sslContext(serverCerts.toServerTlsContext())
        .addService(impressionMetadataServiceMock)
        .addService(leaseServiceMock)
        .build()
        .start()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$DATE_FOLDER_PREFIX/metadata-invalid.json").build(),
        "{".toByteArray(),
      )
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$DATE_FOLDER_PREFIX/done").build(),
        byteArrayOf(),
      )
      val configFile = writeConfigFile(validConfig())

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          requiredArgs(configFile, apiTarget = "localhost:${server.port}", endDaysAgo = 0) +
            storageArgs,
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
      verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    } finally {
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `recovery registers both configured paths with exact done generations`() {
    runRecoveryAcrossConfiguredPaths(ExternalMutation.NONE)
  }

  @Test
  fun `recovery blocks a missing date across both impression paths`() {
    runRecoveryAcrossConfiguredPaths(ExternalMutation.NONE, blockedByGaps = true)
  }

  @Test
  fun `recovery leaves synchronized generation-pinned neighbours unchanged`() {
    runRecoveryAcrossConfiguredPaths(ExternalMutation.NONE, includeRegisteredNeighbour = true)
  }

  @Test
  fun `recovery scans an external override and the internal root`() {
    runRecoveryAcrossConfiguredPaths(ExternalMutation.NONE, externalOverride = true)
  }

  @Test
  fun `recovery checks gaps across an external override and the internal root`() {
    runRecoveryAcrossConfiguredPaths(
      ExternalMutation.NONE,
      blockedByGaps = true,
      externalOverride = true,
    )
  }

  @Test
  fun `recovery rejects an impressions overwrite under the old external done marker`() {
    runRecoveryAcrossConfiguredPaths(ExternalMutation.OVERWRITTEN_IMPRESSIONS)
  }

  @Test
  fun `recovery rejects metadata added outside the selected external snapshot`() {
    runRecoveryAcrossConfiguredPaths(ExternalMutation.ADDED_METADATA)
  }

  private enum class ExternalMutation {
    NONE,
    OVERWRITTEN_IMPRESSIONS,
    ADDED_METADATA,
  }

  @Test
  fun `legacy recovery validates an already synchronized managed peer without changing its marker`() {
    runRecoveryAcrossConfiguredPaths(ExternalMutation.NONE, managedPeer = true)
  }

  private fun runRecoveryAcrossConfiguredPaths(
    externalMutation: ExternalMutation,
    blockedByGaps: Boolean = false,
    includeRegisteredNeighbour: Boolean = false,
    externalOverride: Boolean = false,
    managedPeer: Boolean = false,
  ) {
    var externalMutationApplied = false
    val recoveredMetadata = mutableListOf<ImpressionMetadata>()
    val registeredNeighbours = mutableListOf<ImpressionMetadata>()
    val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase = mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          listImpressionMetadataResponse {
            impressionMetadata +=
              recoveredMetadata.filter {
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
          if (!externalMutationApplied && externalMutation != ExternalMutation.NONE) {
            val metadata = request.requestsList.first().impressionMetadata
            val metadataKey = metadata.blobUri.removePrefix("gs://$BUCKET_NAME/")
            if (metadataKey.startsWith("$EDP_IMPRESSION_PATH/")) {
              externalMutationApplied = true
              val metadataBytes = storageEmulator.storage.readAllBytes(BUCKET_NAME, metadataKey)
              when (externalMutation) {
                ExternalMutation.OVERWRITTEN_IMPRESSIONS -> {
                  val details =
                    org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails.parseFrom(
                      metadataBytes
                    )
                  storageEmulator.storage.create(
                    BlobInfo.newBuilder(
                        BUCKET_NAME,
                        details.blobUri.removePrefix("gs://$BUCKET_NAME/"),
                      )
                      .build(),
                    byteArrayOf(1),
                  )
                }
                ExternalMutation.ADDED_METADATA ->
                  storageEmulator.storage.create(
                    BlobInfo.newBuilder(BUCKET_NAME, "$metadataKey.late.metadata.binpb").build(),
                    metadataBytes,
                  )
                ExternalMutation.NONE -> error("No mutation requested")
              }
            }
          }
          val created =
            request.requestsList.mapIndexed { index, createRequest ->
              createRequest.impressionMetadata.copy {
                name =
                  "${request.parent}/impressionMetadata/recovered-${recoveredMetadata.size + index}"
                state = ACTIVE
              }
            }
          recoveredMetadata += created
          batchCreateImpressionMetadataResponse { impressionMetadata += created }
        }
      onBlocking { computeModelLineBounds(any<ComputeModelLineBoundsRequest>()) }
        .thenReturn(
          computeModelLineBoundsResponse {
            modelLineBounds += modelLineBoundMapEntry {
              key = "modelProviders/mp/modelSuites/ms/modelLines/model-line-1"
              value = interval {
                startTime = timestamp {
                  seconds =
                    if (managedPeer)
                      LocalDate.of(2000, 1, 1).atStartOfDay(ZoneOffset.UTC).toEpochSecond()
                    else 1
                }
                endTime = timestamp {
                  seconds =
                    if (managedPeer)
                      LocalDate.of(2000, 1, 2).atStartOfDay(ZoneOffset.UTC).toEpochSecond()
                    else 2
                }
              }
            }
          }
        )
    }
    val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
      onBlocking {
          replaceDataAvailabilityIntervals(any<ReplaceDataAvailabilityIntervalsRequest>())
        }
        .thenReturn(DataProvider.getDefaultInstance())
    }
    val server: Server =
      ManagedDoneObjectTestServices(
          "dataProviders/test-provider",
          listOf(MANAGED_MODEL_LINE),
          LocalDate.of(1999, 12, 31),
          MANAGED_RAW_INPUT_URI,
        )
        .apply {
          ownerState =
            org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine.State
              .COMPLETED
        }
        .addTo(NettyServerBuilder.forPort(0))
        .sslContext(serverCerts.toServerTlsContext())
        .addService(impressionMetadataServiceMock)
        .addService(dataProvidersServiceMock)
        .addService(leaseServiceMock)
        .build()
        .start()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val paths =
        if (externalMutation == ExternalMutation.NONE) {
          listOf(EDP_IMPRESSION_PATH, INTERNAL_IMPRESSION_PATH)
        } else {
          listOf(EDP_IMPRESSION_PATH)
        }
      val doneGenerations =
        paths.associate { path ->
          val date =
            if (blockedByGaps && path == INTERNAL_IMPRESSION_PATH) "2000-01-03" else "2000-01-01"
          val folder = "$path/model-line/model-line-1/$date"
          val impressionsKey = "$folder/impressions"
          val metadataKey = "$impressionsKey.metadata.binpb"
          storageEmulator.storage.create(
            BlobInfo.newBuilder(BUCKET_NAME, impressionsKey).build(),
            if (managedPeer) byteArrayOf(1) else byteArrayOf(),
          )
          storageEmulator.storage.create(
            BlobInfo.newBuilder(BUCKET_NAME, metadataKey).build(),
            blobDetails {
                blobUri = "gs://$BUCKET_NAME/$impressionsKey"
                eventGroupReferenceId = "event-group"
                modelLine = "modelProviders/mp/modelSuites/ms/modelLines/model-line-1"
                interval = interval {
                  startTime = timestamp {
                    seconds =
                      if (managedPeer)
                        LocalDate.of(2000, 1, 1).atStartOfDay(ZoneOffset.UTC).toEpochSecond()
                      else 1
                  }
                  endTime = timestamp {
                    seconds =
                      if (managedPeer)
                        LocalDate.of(2000, 1, 2).atStartOfDay(ZoneOffset.UTC).toEpochSecond()
                      else 2
                  }
                }
              }
              .toByteArray(),
          )
          val neighbourData =
            if (includeRegisteredNeighbour) {
              storageEmulator.storage.create(
                BlobInfo.newBuilder(BUCKET_NAME, "$folder/registered-impressions").build(),
                byteArrayOf(1),
              )
            } else null
          val neighbourMetadata =
            neighbourData?.let { data ->
              storageEmulator.storage.create(
                BlobInfo.newBuilder(BUCKET_NAME, "${data.name}.metadata.binpb")
                  .setMetadata(
                    mapOf(
                      DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE
                    )
                  )
                  .build(),
                blobDetails {
                    blobUri = "gs://$BUCKET_NAME/${data.name}"
                    eventGroupReferenceId = "event-group"
                    modelLine = "modelProviders/mp/modelSuites/ms/modelLines/model-line-1"
                    interval = interval {
                      startTime = timestamp {
                        seconds =
                          if (managedPeer)
                            LocalDate.of(2000, 1, 1).atStartOfDay(ZoneOffset.UTC).toEpochSecond()
                          else 1
                      }
                      endTime = timestamp {
                        seconds =
                          if (managedPeer)
                            LocalDate.of(2000, 1, 2).atStartOfDay(ZoneOffset.UTC).toEpochSecond()
                          else 2
                      }
                    }
                  }
                  .toByteArray(),
              )
            }
          val doneBlob =
            storageEmulator.storage.create(
              BlobInfo.newBuilder(BUCKET_NAME, "$folder/done")
                .setMetadata(
                  if (includeRegisteredNeighbour)
                    mapOf(
                      DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE,
                      DataAvailabilityBlobs.SYNC_ID_KEY to "original-sync",
                      DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY to "original-sync",
                    )
                  else emptyMap()
                )
                .build(),
              byteArrayOf(),
            )
          if (neighbourMetadata != null) {
            val existing = impressionMetadata {
              name =
                "dataProviders/test-provider/impressionMetadata/registered-${registeredNeighbours.size}"
              blobUri = "gs://$BUCKET_NAME/${neighbourMetadata.name}"
              blobTypeUrl = "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.BlobDetails"
              eventGroupReferenceId = "event-group"
              modelLine = "modelProviders/mp/modelSuites/ms/modelLines/model-line-1"
              interval = interval {
                startTime = timestamp {
                  seconds =
                    if (managedPeer)
                      LocalDate.of(2000, 1, 1).atStartOfDay(ZoneOffset.UTC).toEpochSecond()
                    else 1
                }
                endTime = timestamp {
                  seconds =
                    if (managedPeer)
                      LocalDate.of(2000, 1, 2).atStartOfDay(ZoneOffset.UTC).toEpochSecond()
                    else 2
                }
              }
              state = ACTIVE
              metadataBlobGeneration = neighbourMetadata.generation
              impressionsBlobGeneration = checkNotNull(neighbourData).generation
              outputDoneBlobGeneration = doneBlob.generation
            }
            registeredNeighbours += existing
            recoveredMetadata += existing
          }
          "gs://$BUCKET_NAME/$metadataKey" to doneBlob.generation
        }
      val managedPeerKey =
        "$INTERNAL_IMPRESSION_PATH/" +
          LabeledImpressionsBlobKeys.forDone(
            "dataProviders/test-provider/rawImpressionUploads/managed",
            MANAGED_MODEL_LINE,
            LocalDate.of(1999, 12, 31),
          )
      val peerMetadata =
        mapOf(
          DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE,
          DataAvailabilityBlobs.SYNC_ID_KEY to "zero-peer-sync",
          DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY to "zero-peer-sync",
        )
      val initialPeerBlob =
        if (managedPeer) {
          storageEmulator.storage.create(
            BlobInfo.newBuilder(BUCKET_NAME, managedPeerKey).setMetadata(peerMetadata).build(),
            byteArrayOf(),
          )
          storageEmulator.storage.get(BUCKET_NAME, managedPeerKey)
        } else null
      val initialPeerMetadata = initialPeerBlob?.metadata?.toMap().orEmpty()
      val initialPeerGeneration = initialPeerBlob?.generation
      val configFile =
        writeConfigFile(
          (if (externalOverride) {
            validConfig()
              .replace(
                "edp_impression_path: \"$EDP_IMPRESSION_PATH\"",
                "edp_impression_path: \"unused/external\"",
              ) + "\nexternal_impression_path: \"$EDP_IMPRESSION_PATH\"\n"
          } else validConfig()) +
            "\nvid_labeled_impression_path: \"$INTERNAL_IMPRESSION_PATH\"\n" +
            "\nerror_if_gaps_exist: $blockedByGaps\n"
        )

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          connectionArgs(configFile, apiTarget = "localhost:${server.port}") +
            arrayOf(
              "--data-date=2000-01-01",
              "--data-date=2000-01-03",
              "--storage-api-endpoint=${storageEmulator.storage.options.host}",
              "--throttler-minimum-interval=0s",
            ),
          ::main,
        )

      if (managedPeer) {
        val currentPeer = storageEmulator.storage.get(BUCKET_NAME, managedPeerKey)
        assertThat(currentPeer.metadata).containsExactlyEntriesIn(initialPeerMetadata)
        assertThat(currentPeer.generation).isEqualTo(initialPeerGeneration)
      }
      val failed = blockedByGaps || externalMutation != ExternalMutation.NONE
      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(if (failed) 1 else 0)
      verifyBlocking(dataProvidersServiceMock, times(if (failed) 0 else 2)) {
        replaceDataAvailabilityIntervals(any())
      }
      val request = argumentCaptor<BatchCreateImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(doneGenerations.size)) {
        batchCreateImpressionMetadata(request.capture())
      }
      val createdMetadata =
        request.allValues.flatMap { it.requestsList }.map { it.impressionMetadata }
      assertThat(createdMetadata.map { it.blobUri }).containsExactlyElementsIn(doneGenerations.keys)
      for (metadata in createdMetadata) {
        assertThat(metadata.outputDoneBlobGeneration)
          .isEqualTo(doneGenerations.getValue(metadata.blobUri))
      }
      for (metadata in
        createdMetadata.filter {
          it.blobUri.startsWith("gs://$BUCKET_NAME/$EDP_IMPRESSION_PATH/")
        }) {
        assertThat(metadata.metadataBlobGeneration).isGreaterThan(0L)
        assertThat(metadata.impressionsBlobGeneration).isGreaterThan(0L)
      }
      if (externalMutation != ExternalMutation.NONE) {
        assertThat(externalMutationApplied).isTrue()
        val externalDone =
          storageEmulator.storage.get(
            BUCKET_NAME,
            "$EDP_IMPRESSION_PATH/model-line/model-line-1/2000-01-01/done",
          )
        assertThat(externalDone.metadata.orEmpty())
          .doesNotContainKey(DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY)
      }
      for (existing in registeredNeighbours) {
        assertThat(recoveredMetadata.single { it.name == existing.name }).isEqualTo(existing)
      }
      verifyBlocking(impressionMetadataServiceMock, times(0)) {
        batchUpdateImpressionMetadata(any())
      }
      val acquired = argumentCaptor<AcquireDataAvailabilitySyncLeaseRequest>()
      verifyBlocking(leaseServiceMock, times(1)) {
        acquireDataAvailabilitySyncLease(acquired.capture())
      }
      val released = argumentCaptor<ReleaseDataAvailabilitySyncLeaseRequest>()
      verifyBlocking(leaseServiceMock, times(1)) {
        releaseDataAvailabilitySyncLease(released.capture())
      }
      assertThat(released.firstValue.name).isEqualTo(acquired.firstValue.name)
      assertThat(request.allValues.map { it.dataAvailabilitySyncLease }.distinct())
        .containsExactly(acquired.firstValue.name)
    } finally {
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main reports internal recovery failure after scanning both paths`() {
    val metadataService: ImpressionMetadataServiceCoroutineImplBase = mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenReturn(listImpressionMetadataResponse {})
    }
    val server =
      NettyServerBuilder.forPort(0)
        .sslContext(serverCerts.toServerTlsContext())
        .addService(metadataService)
        .addService(leaseServiceMock)
        .build()
        .start()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$DATE_FOLDER_PREFIX/placeholder").build(),
        byteArrayOf(),
      )
      val internalFolder = "$INTERNAL_IMPRESSION_PATH/model-line/model-line-1/2000-01-01"
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$internalFolder/metadata-invalid.json").build(),
        "{".toByteArray(),
      )
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$internalFolder/done").build(),
        byteArrayOf(),
      )
      val configFile =
        writeConfigFile(
          validConfig() + "\nvid_labeled_impression_path: \"$INTERNAL_IMPRESSION_PATH\"\n"
        )
      val output =
        CommandLineTesting.capturingOutput(
          connectionArgs(configFile, "localhost:${server.port}") +
            arrayOf(
              "--data-date=2000-01-01",
              "--throttler-minimum-interval=0s",
              "--storage-api-endpoint=${storageEmulator.storage.options.host}",
            ),
          ::main,
        )
      CommandLineTesting.assertThat(output).status().isNotEqualTo(0)
      val requests = argumentCaptor<ListImpressionMetadataRequest>()
      verifyBlocking(metadataService, times(2)) { listImpressionMetadata(requests.capture()) }
      for (path in listOf(EDP_IMPRESSION_PATH, INTERNAL_IMPRESSION_PATH)) {
        assertThat(
            requests.allValues.any {
              it.filter.blobUriPrefix.startsWith("gs://$BUCKET_NAME/$path/")
            }
          )
          .isTrue()
      }
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
        "--lookback-days=100000",
        "--throttler-minimum-interval=0s",
      )

  private fun newServer(): Server =
    NettyServerBuilder.forPort(0)
      .sslContext(serverCerts.toServerTlsContext())
      .addService(leaseServiceMock)
      .build()
      .start()

  private fun connectionArgs(configFile: File, apiTarget: String): Array<String> =
    arrayOf(
      "--config-file=${configFile.path}",
      "--kingdom-public-api-target=$apiTarget",
      "--impression-metadata-api-target=$apiTarget",
    )

  private fun requiredArgs(configFile: File, apiTarget: String, endDaysAgo: Int): Array<String> =
    connectionArgs(configFile, apiTarget) + "--end-days-ago=$endDaysAgo"

  private fun validConfig(): String =
    """
    data_provider: "dataProviders/test-provider"
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

  private fun writeConfigFile(contents: String): File =
    tempDir
      .newFile("data-availability-sync-config-${tempDir.root.listFiles().size}.textproto")
      .apply { writeText(contents) }

  companion object {
    private const val MANAGED_RAW_INPUT_URI = "gs://raw/managed.parquet"
    private const val MANAGED_MODEL_LINE =
      "modelProviders/mp/modelSuites/ms/modelLines/model-line-1"

    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()

    private const val MODULE_REPO_NAME = "wfa_measurement_system"
    private const val BUCKET_NAME = "recovery-test"
    private const val EDP_IMPRESSION_PATH = "edp/test/vid-labeled-impressions"
    private const val INTERNAL_IMPRESSION_PATH = "vid-labeled-impressions/edp/test"
    private const val DATE_FOLDER_PREFIX = "$EDP_IMPRESSION_PATH/model-line/model-line-1/2000-01-01"
    private val SECRETS_DIR: Path =
      getRuntimePath(Paths.get(MODULE_REPO_NAME, "src", "main", "k8s", "testing", "secretfiles"))!!
    private val serverCerts: SigningCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("kingdom_root.pem").toFile(),
      )
  }
}
