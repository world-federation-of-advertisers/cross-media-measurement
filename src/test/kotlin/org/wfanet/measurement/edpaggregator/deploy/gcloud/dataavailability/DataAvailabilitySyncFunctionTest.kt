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
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Collections
import java.util.logging.Logger
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.fileSystemStorage
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.config.edpaggregator.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class DataAvailabilitySyncFunctionTest {

  private lateinit var grpcServer: CommonServer
  private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

  private val capturedTraceparentHeaders = Collections.synchronizedList(mutableListOf<String>())
  private val capturedGrpcTraceBinHeaders = Collections.synchronizedList(mutableListOf<ByteArray>())
  private val traceparentKey = Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER)
  private val grpcTraceBinKey = Metadata.Key.of("grpc-trace-bin", Metadata.BINARY_BYTE_MARSHALLER)
  private val metadataCaptureInterceptor =
    object : ServerInterceptor {
      override fun <ReqT, RespT> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
      ): ServerCall.Listener<ReqT> {
        headers[traceparentKey]?.let { capturedTraceparentHeaders.add(it) }
        headers[grpcTraceBinKey]?.let { capturedGrpcTraceBinHeaders.add(it) }
        return next.startCall(call, headers)
      }
    }

  private val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { replaceDataAvailabilityIntervals(any<ReplaceDataAvailabilityIntervalsRequest>()) }
      .thenAnswer { DataProvider.getDefaultInstance() }
  }

  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { batchCreateImpressionMetadata(any<BatchCreateImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchCreateImpressionMetadataRequest>(0)
          batchCreateImpressionMetadataResponse {
            impressionMetadata += request.requestsList.map { it.impressionMetadata }
          }
        }
      onBlocking { computeModelLineBounds(any<ComputeModelLineBoundsRequest>()) }
        .thenAnswer { invocation ->
          computeModelLineBoundsResponse {
            modelLineBounds += modelLineBoundMapEntry {
              key = "some-model-line"
              value = interval {
                startTime = timestamp { seconds = 1735689600 } // 2025-01-01T00:00:00Z
                endTime = timestamp { seconds = 1736467200 } // 2025-01-10T00:00:00Z
              }
            }
          }
        }
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(ServerInterceptors.intercept(dataProvidersServiceMock, metadataCaptureInterceptor))
    addService(
      ServerInterceptors.intercept(impressionMetadataServiceMock, metadataCaptureInterceptor)
    )
  }

  @get:Rule val tempFolder = TemporaryFolder()

  @Before
  fun startInfra() {
    capturedTraceparentHeaders.clear()
    capturedGrpcTraceBinHeaders.clear()
    /** Start gRPC server with mock EventGroups service */
    grpcServer =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "DataAvailabilityServers",
          services =
            listOf(
              ServerInterceptors.intercept(
                dataProvidersServiceMock.bindService(),
                metadataCaptureInterceptor,
              ),
              ServerInterceptors.intercept(
                impressionMetadataServiceMock.bindService(),
                metadataCaptureInterceptor,
              ),
            ),
        )
        .start()
    functionProcess =
      FunctionsFrameworkInvokerProcess(
        javaBinaryPath = FUNCTION_BINARY_PATH,
        classTarget = GCG_TARGET,
      )
    logger.info("Started gRPC server on port ${grpcServer.port}")
  }

  @After
  fun cleanUp() {
    grpcServer.shutdown()
  }

  @Test
  fun `sync registersUnregisteredImpressionMetadata`() {

    val localImpressionBlobKey = "edp/edp_name/timestamp/impressions"
    val localImpressionBlobUri = "file:////edp/edp_name/timestamp/impressions"
    val localMetadataBlobKey = "edp/edp_name/timestamp/metadata.binpb"
    val localDoneBlobUri = "file:////edp/edp_name/timestamp/done"

    val blobDetails = blobDetails {
      blobUri = localImpressionBlobUri
      eventGroupReferenceId = "reference-id"
      modelLine = "some-model-line"
      interval = interval {
        startTime = timestamp { seconds = 1735689600 }
        endTime = timestamp { seconds = 1736467200 }
      }
    }

    val dataAvailabilitySyncConfig = fileSystemDataAvailabilitySyncConfig()
    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val port = runBlocking {
      functionProcess.start(
        mapOf(
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "IMPRESSION_METADATA_TARGET" to "localhost:${grpcServer.port}",
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }

    val url = "http://localhost:$port"
    logger.info("Testing Cloud Function at: $url")

    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    runBlocking {
      storageClient.writeBlob(localImpressionBlobKey, emptyFlow())
      storageClient.writeBlob(localMetadataBlobKey, flowOf(blobDetails.toByteString()))
    }
    // In practice, the DataWatcher makes this HTTP call
    val client = HttpClient.newHttpClient()
    val getRequest =
      HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("X-DataWatcher-Path", localDoneBlobUri)
        .POST(HttpRequest.BodyPublishers.ofString(dataAvailabilitySyncConfig.toJson()))
        .build()
    val getResponse = client.send(getRequest, HttpResponse.BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")

    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `sync propagates traceparent header to outgoing grpc calls`() {
    val localImpressionBlobKey = "edp/edp_name/timestamp/impressions"
    val localImpressionBlobUri = "file:////edp/edp_name/timestamp/impressions"
    val localMetadataBlobKey = "edp/edp_name/timestamp/metadata.binpb"
    val localDoneBlobUri = "file:////edp/edp_name/timestamp/done"

    val blobDetails = blobDetails {
      blobUri = localImpressionBlobUri
      eventGroupReferenceId = "reference-id"
      modelLine = "some-model-line"
      interval = interval {
        startTime = timestamp { seconds = 1735689600 }
        endTime = timestamp { seconds = 1736467200 }
      }
    }

    val dataAvailabilitySyncConfig = fileSystemDataAvailabilitySyncConfig()
    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val port = runBlocking {
      functionProcess.start(
        mapOf(
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "IMPRESSION_METADATA_TARGET" to "localhost:${grpcServer.port}",
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }

    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    runBlocking {
      storageClient.writeBlob(localImpressionBlobKey, emptyFlow())
      storageClient.writeBlob(localMetadataBlobKey, flowOf(blobDetails.toByteString()))
    }

    val traceId = "1af7651916cd43dd8448eb211c80319c"
    val traceParentHeader = "00-$traceId-0123456789abcdef-01"

    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .header("X-DataWatcher-Path", localDoneBlobUri)
        .header("traceparent", traceParentHeader)
        .POST(HttpRequest.BodyPublishers.ofString(dataAvailabilitySyncConfig.toJson()))
        .build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    logger.info("Trace propagation response status: ${response.statusCode()}")
    logger.info("Trace propagation response body: ${response.body()}")

    verifyBlocking(dataProvidersServiceMock, times(1)) { replaceDataAvailabilityIntervals(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }

    logger.info("Captured traceparent headers: $capturedTraceparentHeaders")
    logger.info(
      "Captured grpc-trace-bin headers: ${capturedGrpcTraceBinHeaders.map { it.toHexString() }}"
    )

    val recordedTraceIds =
      synchronized(capturedTraceparentHeaders) {
        val w3cIds = capturedTraceparentHeaders.mapNotNull { parseTraceparentTraceId(it) }
        val grpcTraceIds = capturedGrpcTraceBinHeaders.mapNotNull { parseGrpcTraceBinTraceId(it) }
        (w3cIds + grpcTraceIds).toSet()
      }
    assertThat(recordedTraceIds).isNotEmpty()
    assertThat(recordedTraceIds).contains(traceId)
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
        // TODO(@marcopremier): Replace with ImpressionMetadata cert when available
        certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
      }
      dataAvailabilityStorage = storageParams { fileSystem = fileSystemStorage {} }
      edpImpressionPath = "edp/edp_name"
    }

  private fun parseTraceparentTraceId(header: String?): String? {
    header ?: return null
    val parts = header.split('-')
    return if (parts.size >= 2) parts[1].lowercase() else null
  }

  private fun parseGrpcTraceBinTraceId(bytes: ByteArray?): String? {
    bytes ?: return null
    if (bytes.size < 18) {
      return null
    }
    if (bytes[0] != 0.toByte()) {
      return null
    }
    val traceIdBytes = bytes.copyOfRange(1, 17)
    return traceIdBytes.joinToString(separator = "") { "%02x".format(it) }
  }

  private fun ByteArray.toHexString(): String = joinToString(separator = "") { "%02x".format(it) }

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
        "InvokeDataAvailabilitySyncFunction",
      )
    private const val GCG_TARGET =
      "org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability.DataAvailabilitySyncFunction"
  }
}
