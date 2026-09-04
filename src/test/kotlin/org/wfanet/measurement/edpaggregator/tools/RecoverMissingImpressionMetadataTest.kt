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
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
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
      val todayFolderPrefix =
        "$EDP_IMPRESSION_PATH/model-line/model-line-1/${LocalDate.now(ZoneOffset.UTC)}"
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$todayFolderPrefix/metadata-invalid.json").build(),
        "{".toByteArray(),
      )
      storageEmulator.storage.create(
        BlobInfo.newBuilder(BUCKET_NAME, "$todayFolderPrefix/done").build(),
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

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    } finally {
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
