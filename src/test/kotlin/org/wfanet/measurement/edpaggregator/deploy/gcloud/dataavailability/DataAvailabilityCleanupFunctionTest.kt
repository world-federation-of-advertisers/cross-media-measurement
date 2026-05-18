/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.TextFormat
import com.google.protobuf.timestamp
import com.google.type.interval
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigKt.modelLineList
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.fileSystemStorage
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfigs
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.config.edpaggregator.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.BatchDeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess

@RunWith(JUnit4::class)
class DataAvailabilityCleanupFunctionTest {

  private lateinit var grpcServer: CommonServer
  private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          val blobUriPrefix = request.filter.blobUriPrefix
          if (blobUriPrefix.isNotEmpty()) {
            listImpressionMetadataResponse {
              impressionMetadata += impressionMetadata {
                name = "dataProviders/edp123/impressionMetadata/im-1"
                modelLine = "modelLine1"
                blobUri = blobUriPrefix
                interval = interval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 200 }
                }
                state = ImpressionMetadata.State.ACTIVE
              }
            }
          } else {
            val blobUrisList = request.filter.blobUrisList
            listImpressionMetadataResponse {
              impressionMetadata +=
                blobUrisList.mapIndexed { index, uri ->
                  impressionMetadata {
                    name = "dataProviders/edp123/impressionMetadata/im-${index + 1}"
                    modelLine = "modelLine1"
                    blobUri = uri
                    interval = interval {
                      startTime = timestamp { seconds = 100 }
                      endTime = timestamp { seconds = 200 }
                    }
                    state = ImpressionMetadata.State.ACTIVE
                  }
                }
            }
          }
        }
      onBlocking { deleteImpressionMetadata(any<DeleteImpressionMetadataRequest>()) }
        .thenAnswer { ImpressionMetadata.getDefaultInstance() }
      onBlocking { batchDeleteImpressionMetadata(any<BatchDeleteImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchDeleteImpressionMetadataRequest>(0)
          batchDeleteImpressionMetadataResponse {
            impressionMetadata +=
              request.namesList.map {
                impressionMetadata {
                  name = it
                  state = ImpressionMetadata.State.DELETED
                }
              }
          }
        }
    }

  @get:Rule val tempFolder = TemporaryFolder()

  @Before
  fun startInfra() {
    grpcServer =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "CleanupTestServers",
          services = listOf(impressionMetadataServiceMock.bindService()),
        )
        .start()
    functionProcess =
      FunctionsFrameworkInvokerProcess(
        javaBinaryPath = FUNCTION_BINARY_PATH,
        classTarget = GCF_TARGET,
      )
    logger.info("Started gRPC server on port ${grpcServer.port}")
  }

  @After
  fun cleanUp() {
    grpcServer.shutdown()
  }

  private fun startFunction(bufferEnabled: Boolean = false, bufferBatchSize: Int = 100): Int {
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    val runtimeConfig = dataAvailabilitySyncConfigs {
      configs += fileSystemDataAvailabilitySyncConfig()
    }
    File(configBucketDir, "config.textproto")
      .writeText(TextFormat.printer().printToString(runtimeConfig))

    return runBlocking {
      functionProcess.start(
        mapOf(
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "IMPRESSION_METADATA_TARGET" to "localhost:${grpcServer.port}",
          "IMPRESSION_METADATA_CERT_HOST" to "localhost",
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "config.textproto",
          "CLEANUP_BUFFER_ENABLED" to bufferEnabled.toString(),
          "CLEANUP_BUFFER_BATCH_SIZE" to bufferBatchSize.toString(),
          "CLEANUP_BUFFER_FLUSH_INTERVAL_SECONDS" to "300",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }
  }

  @Test
  fun `unbuffered cleanup deletes impression metadata by resource ID`() {
    val port = startFunction(bufferEnabled = false)

    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .header("X-DataWatcher-Path", "file:///edp/edp_name/timestamp/blob-1")
        .header("X-Impression-Metadata-Resource-Id", "dataProviders/edp123/impressionMetadata/im-1")
        .POST(HttpRequest.BodyPublishers.ofString(fileSystemDataAvailabilitySyncConfig().toJson()))
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    logger.info("Response: ${response.statusCode()} ${response.body()}")
    assertThat(response.statusCode()).isEqualTo(200)

    verifyBlocking(impressionMetadataServiceMock, times(1)) { deleteImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, never()) { batchDeleteImpressionMetadata(any()) }
  }

  @Test
  fun `buffered cleanup flushes synchronously on each request`() {
    val port = startFunction(bufferEnabled = true)

    Mockito.clearInvocations(impressionMetadataServiceMock)

    val client = HttpClient.newHttpClient()
    val config = fileSystemDataAvailabilitySyncConfig()

    for (i in 1..4) {
      val request =
        HttpRequest.newBuilder()
          .uri(URI.create("http://localhost:$port"))
          .header("X-DataWatcher-Path", "file:///edp/edp_name/timestamp/blob-$i")
          .header(
            "X-Impression-Metadata-Resource-Id",
            "dataProviders/edp123/impressionMetadata/im-$i",
          )
          .POST(HttpRequest.BodyPublishers.ofString(config.toJson()))
          .build()
      val response = client.send(request, HttpResponse.BodyHandlers.ofString())
      logger.info("Request $i response: ${response.statusCode()} ${response.body()}")
      assertThat(response.statusCode()).isEqualTo(200)
    }

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, atLeast(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    val totalDeleted = captor.allValues.sumOf { it.namesList.size }
    assertThat(totalDeleted).isEqualTo(4)
    verifyBlocking(impressionMetadataServiceMock, never()) { deleteImpressionMetadata(any()) }
  }

  @Test
  fun `concurrent buffered requests coalesce via flush lock`() {
    val port = startFunction(bufferEnabled = true)

    Mockito.clearInvocations(impressionMetadataServiceMock)

    val client = HttpClient.newHttpClient()
    val config = fileSystemDataAvailabilitySyncConfig()
    val requestCount = 12
    val executor = Executors.newFixedThreadPool(requestCount)

    val futures =
      (1..requestCount).map { i ->
        executor.submit<HttpResponse<String>> {
          val request =
            HttpRequest.newBuilder()
              .uri(URI.create("http://localhost:$port"))
              .header("X-DataWatcher-Path", "file:///edp/edp_name/timestamp/blob-$i")
              .header(
                "X-Impression-Metadata-Resource-Id",
                "dataProviders/edp123/impressionMetadata/im-$i",
              )
              .POST(HttpRequest.BodyPublishers.ofString(config.toJson()))
              .build()
          client.send(request, HttpResponse.BodyHandlers.ofString())
        }
      }

    executor.shutdown()
    executor.awaitTermination(30, TimeUnit.SECONDS)

    for ((i, future) in futures.withIndex()) {
      val response = future.get()
      logger.info("Concurrent request ${i + 1} response: ${response.statusCode()}")
      assertThat(response.statusCode()).isEqualTo(200)
    }

    val captor = argumentCaptor<BatchDeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, atLeast(1)) {
      batchDeleteImpressionMetadata(captor.capture())
    }
    val totalDeleted = captor.allValues.sumOf { it.namesList.size }
    assertThat(totalDeleted).isEqualTo(requestCount)
    for (request in captor.allValues) {
      assertThat(request.namesList.size).isAtMost(100)
    }
    verifyBlocking(impressionMetadataServiceMock, never()) { deleteImpressionMetadata(any()) }
  }

  private fun fileSystemDataAvailabilitySyncConfig(): DataAvailabilitySyncConfig =
    dataAvailabilitySyncConfig {
      dataProvider = "dataProviders/edp123"
      cmmsConnection = transportLayerSecurityParams {
        certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
        privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
        certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
      }
      impressionMetadataStorageConnection = transportLayerSecurityParams {
        certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
        privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
        certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
      }
      dataAvailabilityStorage = storageParams { fileSystem = fileSystemStorage {} }
      edpImpressionPath = "edp/edp_name"
      modelLineMap["modelProviders/mp1/modelSuites/ms1/modelLines/some-model-line"] =
        modelLineList {
          modelLines += "some-model-line-mapped"
        }
    }

  companion object {
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!
    private val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("edp7_root.pem").toFile(),
      )
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val FUNCTION_BINARY_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "edpaggregator",
        "deploy",
        "gcloud",
        "dataavailability",
        "testing",
        "InvokeDataAvailabilityCleanupFunction",
      )
    private const val GCF_TARGET =
      "org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability.DataAvailabilityCleanupFunction"
  }
}
