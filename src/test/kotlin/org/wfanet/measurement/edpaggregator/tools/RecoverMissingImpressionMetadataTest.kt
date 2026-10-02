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
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule

@RunWith(JUnit4::class)
class RecoverMissingImpressionMetadataTest {
  @get:Rule val tempDir = TemporaryFolder()

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
  fun `main exits zero when no date folders exist`() {
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val configFile = writeConfigFile(validConfig())

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          requiredArgs(configFile, apiTarget = "localhost:1", endDaysAgo = 0) + storageArgs,
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    } finally {
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main excludes date folders newer than end days ago`() {
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
          requiredArgs(configFile, apiTarget = "localhost:1", endDaysAgo = 1) +
            arrayOf(
              "--storage-api-endpoint=${storageEmulator.storage.options.host}",
              "--lookback-days=3",
              "--throttler-minimum-interval=0s",
            ),
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    } finally {
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main includes date folder at end days ago boundary`() {
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
          requiredArgs(configFile, apiTarget = "localhost:1", endDaysAgo = 1) +
            arrayOf(
              "--storage-api-endpoint=${storageEmulator.storage.options.host}",
              "--lookback-days=2",
              "--throttler-minimum-interval=0s",
            ),
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
    } finally {
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
  fun `main preserves cutover availability when resynchronizing metadata`() {
    var registeredMetadata: ImpressionMetadata? = null
    val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase = mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer {
          val metadata = registeredMetadata
          if (metadata == null) {
            listImpressionMetadataResponse {}
          } else {
            listImpressionMetadataResponse { impressionMetadata += metadata }
          }
        }
      onBlocking { batchCreateImpressionMetadata(any<BatchCreateImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchCreateImpressionMetadataRequest>(0)
          val recoveredMetadata =
            request.requestsList.single().impressionMetadata.copy {
              name = "${request.parent}/impressionMetadata/recovered"
              state = ImpressionMetadata.State.ACTIVE
            }
          registeredMetadata = recoveredMetadata
          batchCreateImpressionMetadataResponse { impressionMetadata += recoveredMetadata }
        }
      onBlocking { computeModelLineBounds(any<ComputeModelLineBoundsRequest>()) }
        .thenReturn(
          computeModelLineBoundsResponse {
            modelLineBounds += modelLineBoundMapEntry {
              key = HISTORICAL_MODEL_LINE
              value = interval {
                startTime = timestamp { seconds = HISTORICAL_START_SECONDS }
                endTime = timestamp { seconds = CUTOVER_SECONDS }
              }
            }
            modelLineBounds += modelLineBoundMapEntry {
              key = REPLACEMENT_MODEL_LINE
              value = interval {
                startTime = timestamp { seconds = CUTOVER_SECONDS }
                endTime = timestamp { seconds = REPLACEMENT_END_SECONDS }
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
    val server =
      NettyServerBuilder.forPort(0)
        .sslContext(serverCerts.toServerTlsContext())
        .addService(impressionMetadataServiceMock)
        .addService(dataProvidersServiceMock)
        .build()
        .start()
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val dateFolderPrefix = "$EDP_IMPRESSION_PATH/model-line/replacement/2000-01-02"
      val impressionBlobKey = "$dateFolderPrefix/impressions"
      val metadataBlobKey = "$dateFolderPrefix/metadata.binpb"
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, impressionBlobKey).build(),
        byteArrayOf(1),
      )
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, metadataBlobKey).build(),
        blobDetails {
            blobUri = "gs://$BUCKET_NAME/$impressionBlobKey"
            modelLine = REPLACEMENT_MODEL_LINE
            eventGroupReferenceId = "event-group"
            interval = interval {
              startTime = timestamp { seconds = CUTOVER_SECONDS }
              endTime = timestamp { seconds = REPLACEMENT_END_SECONDS }
            }
          }
          .toByteArray(),
      )
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$dateFolderPrefix/done").build(),
        byteArrayOf(),
      )
      val configFile = writeConfigFile(cutoverConfig())

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          connectionArgs(configFile, apiTarget = "localhost:${server.port}") +
            arrayOf(
              "--data-date=2000-01-02",
              "--storage-api-endpoint=${storageEmulator.storage.options.host}",
              "--throttler-minimum-interval=0s",
            ),
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
      val requestCaptor = argumentCaptor<ReplaceDataAvailabilityIntervalsRequest>()
      verifyBlocking(dataProvidersServiceMock) {
        replaceDataAvailabilityIntervals(requestCaptor.capture())
      }
      val externalInterval =
        requestCaptor.firstValue.dataAvailabilityIntervalsList
          .single { it.key == EXTERNAL_MODEL_LINE }
          .value
      assertThat(externalInterval.startTime.seconds).isEqualTo(HISTORICAL_START_SECONDS)
      assertThat(externalInterval.endTime.seconds).isEqualTo(REPLACEMENT_END_SECONDS)
    } finally {
      server.shutdown()
      server.awaitTermination(1, SECONDS)
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  private fun cutoverConfig(): String =
    validConfig() +
      """

      model_line_cutovers {
        external_model_line: "$EXTERNAL_MODEL_LINE"
        historical_model_line: "$HISTORICAL_MODEL_LINE"
        replacement_model_line: "$REPLACEMENT_MODEL_LINE"
        cutover_date { year: 2000 month: 1 day: 2 }
      }
      """
        .trimIndent()

  private val storageArgs: Array<String>
    get() =
      arrayOf(
        "--storage-api-endpoint=${storageEmulator.storage.options.host}",
        "--lookback-days=100000",
        "--throttler-minimum-interval=0s",
      )

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
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()

    private const val MODULE_REPO_NAME = "wfa_measurement_system"
    private const val BUCKET_NAME = "recovery-test"
    private const val EDP_IMPRESSION_PATH = "edp/test/vid-labeled-impressions"
    private const val DATE_FOLDER_PREFIX = "$EDP_IMPRESSION_PATH/model-line/model-line-1/2000-01-01"
    private const val EXTERNAL_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/external"
    private const val HISTORICAL_MODEL_LINE =
      "modelProviders/mp1/modelSuites/ms1/modelLines/historical"
    private const val REPLACEMENT_MODEL_LINE =
      "modelProviders/mp1/modelSuites/ms1/modelLines/replacement"
    private const val HISTORICAL_START_SECONDS = 946684800L
    private const val CUTOVER_SECONDS = 946771200L
    private const val REPLACEMENT_END_SECONDS = 946857600L
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
