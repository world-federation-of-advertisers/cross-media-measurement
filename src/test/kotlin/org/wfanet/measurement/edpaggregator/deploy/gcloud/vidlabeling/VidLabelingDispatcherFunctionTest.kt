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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.TextFormat
import com.google.protobuf.util.Timestamps
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths
import java.util.logging.Logger
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt.ModelShardsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.listModelShardsResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.gcsStorage
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigKt.modelLineConfig
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.config.edpaggregator.transportLayerSecurityParams
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfigs
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingDispatcherParams
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class VidLabelingDispatcherFunctionTest {

  private lateinit var grpcServer: CommonServer
  private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

  private val rawImpressionUploadServiceMock: RawImpressionUploadServiceCoroutineImplBase =
    mockService {
      onBlocking { createRawImpressionUpload(any<CreateRawImpressionUploadRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<CreateRawImpressionUploadRequest>(0)
          RawImpressionUpload.newBuilder()
            .setName("${request.parent}/rawImpressionUploads/upload-1")
            .setDoneBlobUri("file:////edp/edp_name/timestamp/done")
            .build()
        }
      // The fast-path sequencer lists uploads after registration; return none so it cleanly
      // no-ops in this Function-wiring test (dispatch behavior is covered by the unit tests).
      onBlocking { listRawImpressionUploads(any()) }.thenReturn(listRawImpressionUploadsResponse {})
    }

  private val rawImpressionUploadFileServiceMock: RawImpressionUploadFileServiceCoroutineImplBase =
    mockService {
      onBlocking { batchCreateRawImpressionUploadFiles(any()) }
        .thenReturn(batchCreateRawImpressionUploadFilesResponse {})
    }

  private val rawImpressionUploadModelLineServiceMock:
    RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService {
      onBlocking { batchCreateRawImpressionUploadModelLines(any()) }
        .thenReturn(batchCreateRawImpressionUploadModelLinesResponse {})
    }

  private val modelLinesServiceMock: ModelLinesCoroutineImplBase = mockService {
    onBlocking { listModelLines(any()) }
      .thenReturn(
        listModelLinesResponse {
          modelLines += modelLine {
            name = MODEL_LINE
            type = ModelLine.Type.PROD
            activeStartTime = Timestamps.fromMillis(0)
            activeEndTime = Timestamps.fromMillis(System.currentTimeMillis() + 86400000)
          }
        }
      )
  }

  private val modelRolloutsServiceMock: ModelRolloutsCoroutineImplBase = mockService {
    onBlocking { listModelRollouts(any()) }
      .thenReturn(
        listModelRolloutsResponse { modelRollouts += modelRollout { modelRelease = MODEL_RELEASE } }
      )
  }

  private val modelShardsServiceMock: ModelShardsCoroutineImplBase = mockService {
    onBlocking { listModelShards(any()) }
      .thenReturn(
        listModelShardsResponse {
          modelShards += modelShard {
            name = "$DATA_PROVIDER/modelShards/ms1"
            modelRelease = MODEL_RELEASE
            modelBlob = modelBlob { modelBlobPath = "gs://models/model.pb" }
          }
        }
      )
  }

  @get:Rule val tempFolder = TemporaryFolder()

  @Before
  fun startInfra() {
    grpcServer =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "VidLabelingTestServers",
          services =
            listOf(
              rawImpressionUploadServiceMock.bindService(),
              rawImpressionUploadFileServiceMock.bindService(),
              rawImpressionUploadModelLineServiceMock.bindService(),
              modelLinesServiceMock.bindService(),
              modelRolloutsServiceMock.bindService(),
              modelShardsServiceMock.bindService(),
            ),
        )
        .start()
    functionProcess =
      FunctionsFrameworkInvokerProcess(
        javaBinaryPath = FUNCTION_BINARY_PATH,
        classTarget = FUNCTION_CLASS_TARGET,
      )
    logger.info("Started gRPC server on port ${grpcServer.port}")
  }

  @After
  fun cleanUp() {
    grpcServer.shutdown()
  }

  private fun startFunction(): Int {
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    val runtimeConfig = vidLabelingConfigs { configs += fileSystemVidLabelingConfig() }
    File(configBucketDir, "config.textproto")
      .writeText(TextFormat.printer().printToString(runtimeConfig))

    return runBlocking {
      functionProcess.start(
        mapOf(
          "MODEL_LINES_TARGET" to "localhost:${grpcServer.port}",
          "MODEL_LINES_CERT_HOST" to "localhost",
          "MODEL_ROLLOUTS_TARGET" to "localhost:${grpcServer.port}",
          "MODEL_ROLLOUTS_CERT_HOST" to "localhost",
          "MODEL_SHARDS_TARGET" to "localhost:${grpcServer.port}",
          "MODEL_SHARDS_CERT_HOST" to "localhost",
          "RAW_IMPRESSION_UPLOAD_TARGET" to "localhost:${grpcServer.port}",
          "RAW_IMPRESSION_UPLOAD_CERT_HOST" to "localhost",
          "CONTROL_PLANE_TARGET" to "localhost:${grpcServer.port}",
          "CONTROL_PLANE_CERT_HOST" to "localhost",
          "VID_LABELER_QUEUE_NAME" to "queues/vid-labeler",
          "POOL_ASSIGNER_QUEUE_NAME" to "queues/pool-assigner",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "VID_LABELING_DISPATCHER_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "config.textproto",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }
  }

  @Test
  fun `upload registers upload files and model lines`() {
    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val storageClient = FileSystemStorageClient(tempFolder.root)
    runBlocking {
      storageClient.writeBlob("edp/edp_name/timestamp/done", emptyFlow())
      storageClient.writeBlob("edp/edp_name/timestamp/impressions_001", emptyFlow())
      storageClient.writeBlob("edp/edp_name/timestamp/impressions_002", emptyFlow())
    }

    val port = startFunction()

    val dispatcherParams = vidLabelingDispatcherParams { dataProvider = DATA_PROVIDER }
    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .header("X-DataWatcher-Path", "file:////edp/edp_name/timestamp/done")
        .header("X-DataWatcher-Generation", "12345")
        .POST(HttpRequest.BodyPublishers.ofString(dispatcherParams.toJson()))
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(200)

    verifyBlocking(rawImpressionUploadServiceMock, times(1)) { createRawImpressionUpload(any()) }
    verifyBlocking(rawImpressionUploadFileServiceMock, times(1)) {
      batchCreateRawImpressionUploadFiles(any())
    }
    verifyBlocking(rawImpressionUploadModelLineServiceMock, times(1)) {
      batchCreateRawImpressionUploadModelLines(any())
    }
    verifyBlocking(modelLinesServiceMock, times(1)) { listModelLines(any()) }
  }

  @Test
  fun `upload returns error when X-DataWatcher-Path header is missing`() {
    val port = startFunction()

    val dispatcherParams = vidLabelingDispatcherParams { dataProvider = DATA_PROVIDER }
    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .POST(HttpRequest.BodyPublishers.ofString(dispatcherParams.toJson()))
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(400)
    verifyBlocking(rawImpressionUploadServiceMock, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `upload returns error when X-DataWatcher-Generation header is missing`() {
    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val storageClient = FileSystemStorageClient(tempFolder.root)
    runBlocking {
      storageClient.writeBlob("edp/edp_name/timestamp/done", emptyFlow())
      storageClient.writeBlob("edp/edp_name/timestamp/impressions_001", emptyFlow())
    }

    val port = startFunction()

    val dispatcherParams = vidLabelingDispatcherParams { dataProvider = DATA_PROVIDER }
    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .header("X-DataWatcher-Path", "file:////edp/edp_name/timestamp/done")
        .POST(HttpRequest.BodyPublishers.ofString(dispatcherParams.toJson()))
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(400)
    verifyBlocking(rawImpressionUploadServiceMock, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `upload returns bad request for unknown data provider`() {
    val port = startFunction()

    val dispatcherParams = vidLabelingDispatcherParams { dataProvider = "dataProviders/unknown" }
    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .header("X-DataWatcher-Path", "file:////edp/edp_name/timestamp/done")
        .header("X-DataWatcher-Generation", "12345")
        .POST(HttpRequest.BodyPublishers.ofString(dispatcherParams.toJson()))
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(400)
    verifyBlocking(rawImpressionUploadServiceMock, never()) { createRawImpressionUpload(any()) }
  }

  private fun fileSystemVidLabelingConfig() = vidLabelingConfig {
    dataProvider = DATA_PROVIDER
    rawImpressionsStorageParams = storageParams {
      gcs = gcsStorage {
        projectId = "test-project"
        bucketName = "raw-impressions-bucket"
      }
    }
    vidLabeledImpressionsStorageParams = storageParams {
      gcs = gcsStorage {
        projectId = "test-project"
        bucketName = "vid-labeled-bucket"
      }
    }
    vidRepoConnection = transportLayerSecurityParams {
      certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
      privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
      certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
    }
    rawImpressionMetadataStorageConnection = transportLayerSecurityParams {
      certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
      privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
      certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
    }
    controlPlaneConnection = transportLayerSecurityParams {
      certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
      privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
      certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
    }
    modelLinesConnection = transportLayerSecurityParams {
      certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
      privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
      certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
    }
    modelShardsConnection = transportLayerSecurityParams {
      certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
      privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
      certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
    }
    modelRolloutsConnection = transportLayerSecurityParams {
      certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
      privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
      certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
    }
    modelLineConfigs[MODEL_LINE] = modelLineConfig {
      labelerInputFieldMapping +=
        LabelerInputFieldMapping.newBuilder()
          .setFieldPath("event_id.id")
          .setScalar(ScalarColumn.newBuilder().setColumn("event_id_col"))
          .build()
    }
    modelSuite = MODEL_SUITE
    numberOfShards = 2
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

    private const val DATA_PROVIDER = "dataProviders/edp123"
    private const val MODEL_SUITE = "modelProviders/mp1/modelSuites/ms1"
    private const val MODEL_LINE = "$MODEL_SUITE/modelLines/ml1"
    private const val MODEL_RELEASE = "$MODEL_SUITE/modelReleases/mr1"

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
        "vidlabeling",
        "testing",
        "InvokeVidLabelingDispatcherFunction",
      )
    private const val FUNCTION_CLASS_TARGET =
      "org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling.VidLabelingDispatcherFunction"
  }
}
