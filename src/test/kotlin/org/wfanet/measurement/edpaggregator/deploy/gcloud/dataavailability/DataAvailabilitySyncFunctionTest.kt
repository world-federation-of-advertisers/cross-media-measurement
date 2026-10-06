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
import com.google.protobuf.Any
import com.google.protobuf.TextFormat
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import com.google.type.date
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
import kotlin.test.assertFailsWith
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
import org.mockito.kotlin.argumentCaptor
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
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigKt.modelLineList
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.fileSystemStorage
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfigs
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.config.edpaggregator.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.AcquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.EventGroupSyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ReleaseDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RenewDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ValidateDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.eventGroupSyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.RenewWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams.DataPathParams.StorageEventType
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt
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
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { listImpressionMetadataResponse {} }
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
              key = "modelProviders/mp1/modelSuites/ms1/modelLines/some-model-line"
              value = interval {
                startTime = timestamp { seconds = 1735689600 } // 2025-01-01T00:00:00Z
                endTime = timestamp { seconds = 1736467200 } // 2025-01-10T00:00:00Z
              }
            }
          }
        }
    }

  private val dataAvailabilitySyncLeaseServiceMock:
    DataAvailabilitySyncLeaseServiceCoroutineImplBase =
    mockService {
      onBlocking {
          acquireDataAvailabilitySyncLease(any<AcquireDataAvailabilitySyncLeaseRequest>())
        }
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
      onBlocking {
          releaseDataAvailabilitySyncLease(any<ReleaseDataAvailabilitySyncLeaseRequest>())
        }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ReleaseDataAvailabilitySyncLeaseRequest>(0)
          dataAvailabilitySyncLease {
            name = request.name
            state = DataAvailabilitySyncLease.State.RELEASED
            etag = "released-etag"
          }
        }
    }

  private val workItemAttemptsServiceMock: WorkItemAttemptsCoroutineImplBase = mockService {
    onBlocking { createWorkItemAttempt(any<CreateWorkItemAttemptRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<CreateWorkItemAttemptRequest>(0)
        workItemAttempt {
          name = "${request.parent}/workItemAttempts/${request.workItemAttemptId}"
          state = WorkItemAttempt.State.ACTIVE
          attemptNumber = 1
        }
      }
    onBlocking { renewWorkItemAttempt(any<RenewWorkItemAttemptRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<RenewWorkItemAttemptRequest>(0)
        workItemAttempt {
          name = request.name
          state = WorkItemAttempt.State.ACTIVE
          attemptNumber = 1
        }
      }
    onBlocking { completeWorkItemAttempt(any<CompleteWorkItemAttemptRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<CompleteWorkItemAttemptRequest>(0)
        workItemAttempt {
          name = request.name
          state = WorkItemAttempt.State.SUCCEEDED
          attemptNumber = 1
        }
      }
    onBlocking { failWorkItemAttempt(any<FailWorkItemAttemptRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<FailWorkItemAttemptRequest>(0)
        workItemAttempt {
          name = request.name
          state = WorkItemAttempt.State.FAILED
          attemptNumber = 1
          errorMessage = request.errorMessage
        }
      }
  }

  private val workItemsServiceMock: WorkItemsCoroutineImplBase = mockService {
    onBlocking { failWorkItem(any<FailWorkItemRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<FailWorkItemRequest>(0)
        workItem {
          name = request.name
          state = WorkItem.State.FAILED
          generation = request.expectedWorkItemGeneration
        }
      }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(ServerInterceptors.intercept(dataProvidersServiceMock, metadataCaptureInterceptor))
    addService(
      ServerInterceptors.intercept(impressionMetadataServiceMock, metadataCaptureInterceptor)
    )
    addService(
      ServerInterceptors.intercept(dataAvailabilitySyncLeaseServiceMock, metadataCaptureInterceptor)
    )
    addService(
      ServerInterceptors.intercept(workItemAttemptsServiceMock, metadataCaptureInterceptor)
    )
    addService(ServerInterceptors.intercept(workItemsServiceMock, metadataCaptureInterceptor))
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
              ServerInterceptors.intercept(
                dataAvailabilitySyncLeaseServiceMock.bindService(),
                metadataCaptureInterceptor,
              ),
              ServerInterceptors.intercept(
                workItemAttemptsServiceMock.bindService(),
                metadataCaptureInterceptor,
              ),
              ServerInterceptors.intercept(
                workItemsServiceMock.bindService(),
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
  fun `DataWatcher generation must be present and positive`() {
    for (value in listOf<String?>(null, "invalid", "0", "-1")) {
      assertFailsWith<IllegalArgumentException> { parseDataWatcherGeneration(value) }
    }
    assertThat(parseDataWatcherGeneration(DONE_BLOB_GENERATION.toString()))
      .isEqualTo(DONE_BLOB_GENERATION)
  }

  @Test
  fun `WorkItem synchronizes availability and completes attempt`() {
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    File(configBucketDir, "config.textproto")
      .writeText(
        TextFormat.printer()
          .printToString(
            dataAvailabilitySyncConfigs { configs += fileSystemDataAvailabilitySyncConfig() }
          )
      )
    val outputDirectory = "edp/edp_name/model-line/some-model-line/2025-01-05"
    File(tempFolder.root, outputDirectory).mkdirs()
    runBlocking {
      val storageClient = FileSystemStorageClient(tempFolder.root)
      storageClient.writeBlob("$outputDirectory/impressions", emptyFlow())
      storageClient.writeBlob(
        "$outputDirectory/metadata.binpb",
        flowOf(
          blobDetails {
              blobUri = "file:////$outputDirectory/impressions"
              eventGroupReferenceId = "reference-id"
              modelLine = MODEL_LINE
              rawImpressionUpload = RAW_UPLOAD
              interval = interval {
                startTime = timestamp { seconds = 1736035200 }
                endTime = timestamp { seconds = 1736121600 }
              }
            }
            .toByteString()
        ),
      )
    }
    val port = runBlocking {
      functionProcess.start(
        mapOf(
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "IMPRESSION_METADATA_TARGET" to "localhost:${grpcServer.port}",
          "IMPRESSION_METADATA_CERT_HOST" to "localhost",
          "SECURE_COMPUTATION_CONTROL_PLANE_TARGET" to "localhost:${grpcServer.port}",
          "SECURE_COMPUTATION_CERT_HOST" to "localhost",
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "config.textproto",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }
    val workItem = workItem {
      name = WORK_ITEM_NAME
      queue = "data-availability-sync-queue"
      generation = 1L
      workItemParams =
        Any.pack(
          workItemParams {
            appParams =
              Any.pack(
                dataAvailabilitySyncParams {
                  dataProvider = DATA_PROVIDER
                  triggeringRawImpressionUpload = RAW_UPLOAD
                  modelLine = MODEL_LINE
                  eventDate = date {
                    year = 2025
                    month = 1
                    day = 5
                  }
                }
              )
            dataPathParams = dataPathParams {
              dataPath = "file:////$outputDirectory/done"
              generation = 123L
              eventType = StorageEventType.FINALIZED
            }
            traceContext["traceparent"] = "00-1af7651916cd43dd8448eb211c80319c-0123456789abcdef-01"
          }
        )
    }
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .header("Content-Type", "application/octet-stream")
        .POST(HttpRequest.BodyPublishers.ofByteArray(workItem.toByteArray()))
        .build()

    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(200)
    val propagatedTraceIds =
      synchronized(capturedTraceparentHeaders) {
        val w3cIds = capturedTraceparentHeaders.mapNotNull { parseTraceparentTraceId(it) }
        val grpcTraceIds = capturedGrpcTraceBinHeaders.mapNotNull { parseGrpcTraceBinTraceId(it) }
        (w3cIds + grpcTraceIds).toSet()
      }
    assertThat(propagatedTraceIds).contains("1af7651916cd43dd8448eb211c80319c")
    val createAttempt = argumentCaptor<CreateWorkItemAttemptRequest>()
    verifyBlocking(workItemAttemptsServiceMock) { createWorkItemAttempt(createAttempt.capture()) }
    assertThat(createAttempt.firstValue.parent).isEqualTo(WORK_ITEM_NAME)
    assertThat(createAttempt.firstValue.expectedWorkItemGeneration).isEqualTo(1L)
    assertThat(createAttempt.firstValue.supportsAttemptLease).isTrue()
    verifyBlocking(workItemAttemptsServiceMock) { completeWorkItemAttempt(any()) }
    verifyBlocking(workItemAttemptsServiceMock, times(0)) { failWorkItemAttempt(any()) }
    verifyBlocking(workItemsServiceMock, times(0)) { failWorkItem(any()) }
    val metadataRequest = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock) {
      batchCreateImpressionMetadata(metadataRequest.capture())
    }
    assertThat(
        metadataRequest.firstValue.requestsList.single().impressionMetadata.outputDoneBlobGeneration
      )
      .isEqualTo(123L)
    assertThat(
        metadataRequest.firstValue.requestsList.single().impressionMetadata.rawImpressionUpload
      )
      .isEqualTo(RAW_UPLOAD)
  }

  @Test
  fun `sync registersUnregisteredImpressionMetadata with legacy config sent over the wire as params`() {

    val localImpressionBlobKey = "edp/edp_name/timestamp/impressions"
    val localImpressionBlobUri = "file:////edp/edp_name/timestamp/impressions"
    val localMetadataBlobKey = "edp/edp_name/timestamp/metadata.binpb"
    val localDoneBlobUri = "file:////edp/edp_name/timestamp/done"

    val blobDetails = blobDetails {
      blobUri = localImpressionBlobUri
      eventGroupReferenceId = "reference-id"
      modelLine = "modelProviders/mp1/modelSuites/ms1/modelLines/some-model-line"
      interval = interval {
        startTime = timestamp { seconds = 1735689600 }
        endTime = timestamp { seconds = 1736467200 }
      }
    }

    val dataAvailabilitySyncConfig = fileSystemDataAvailabilitySyncConfig()

    // Write runtime config to the config bucket
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    val runtimeConfig = dataAvailabilitySyncConfigs { configs += dataAvailabilitySyncConfig }
    File(configBucketDir, "config.textproto")
      .writeText(TextFormat.printer().printToString(runtimeConfig))

    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val port = runBlocking {
      functionProcess.start(
        mapOf(
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "IMPRESSION_METADATA_TARGET" to "localhost:${grpcServer.port}",
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "config.textproto",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }

    val url = "http://localhost:$port"
    logger.info("Testing Cloud Function at: $url")

    // Set up model-line date paths for gap monitor (single date = no gaps)
    val modelLineDatePath = "edp/edp_name/model-line/some-model-line/2025-01-05"
    File(tempFolder.root, "$modelLineDatePath/").mkdirs()
    runBlocking {
      val fsClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      fsClient.writeBlob("$modelLineDatePath/done", emptyFlow())
      fsClient.writeBlob("$modelLineDatePath/data_file", emptyFlow())
    }

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
        .header("X-DataWatcher-Generation", DONE_BLOB_GENERATION.toString())
        .POST(HttpRequest.BodyPublishers.ofString(dataAvailabilitySyncConfig.toJson()))
        .build()
    val getResponse = client.send(getRequest, HttpResponse.BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")

    val requestCaptor = argumentCaptor<ReplaceDataAvailabilityIntervalsRequest>()
    verifyBlocking(dataProvidersServiceMock, times(1)) {
      replaceDataAvailabilityIntervals(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.name).isEqualTo("dataProviders/edp123")
    assertThat(requestCaptor.firstValue.dataAvailabilityIntervalsList.map { it.key })
      .contains("some-model-line-mapped")
    val metadataRequest = argumentCaptor<BatchCreateImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      batchCreateImpressionMetadata(metadataRequest.capture())
    }
    assertThat(
        metadataRequest.firstValue.requestsList.single().impressionMetadata.outputDoneBlobGeneration
      )
      .isEqualTo(DONE_BLOB_GENERATION)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
    verifyBlocking(dataAvailabilitySyncLeaseServiceMock, times(1)) {
      acquireDataAvailabilitySyncLease(any())
    }
    verifyBlocking(dataAvailabilitySyncLeaseServiceMock, times(1)) {
      releaseDataAvailabilitySyncLease(any())
    }
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
      modelLine = "modelProviders/mp1/modelSuites/ms1/modelLines/some-model-line"
      interval = interval {
        startTime = timestamp { seconds = 1735689600 }
        endTime = timestamp { seconds = 1736467200 }
      }
    }

    val dataAvailabilitySyncConfig = fileSystemDataAvailabilitySyncConfig()

    // Write runtime config to the config bucket
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    val runtimeConfig = dataAvailabilitySyncConfigs { configs += dataAvailabilitySyncConfig }
    File(configBucketDir, "config.textproto")
      .writeText(TextFormat.printer().printToString(runtimeConfig))

    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val port = runBlocking {
      functionProcess.start(
        mapOf(
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "IMPRESSION_METADATA_TARGET" to "localhost:${grpcServer.port}",
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "config.textproto",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }

    // Set up model-line date paths for gap monitor (single date = no gaps)
    val modelLineDatePath = "edp/edp_name/model-line/some-model-line/2025-01-05"
    File(tempFolder.root, "$modelLineDatePath/").mkdirs()
    runBlocking {
      val fsClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      fsClient.writeBlob("$modelLineDatePath/done", emptyFlow())
      fsClient.writeBlob("$modelLineDatePath/data_file", emptyFlow())
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
        .header("X-DataWatcher-Generation", DONE_BLOB_GENERATION.toString())
        .header("traceparent", traceParentHeader)
        .POST(HttpRequest.BodyPublishers.ofString(dataAvailabilitySyncConfig.toJson()))
        .build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    logger.info("Trace propagation response status: ${response.statusCode()}")
    logger.info("Trace propagation response body: ${response.body()}")

    val requestCaptor = argumentCaptor<ReplaceDataAvailabilityIntervalsRequest>()
    verifyBlocking(dataProvidersServiceMock, times(1)) {
      replaceDataAvailabilityIntervals(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.name).isEqualTo("dataProviders/edp123")
    assertThat(requestCaptor.firstValue.dataAvailabilityIntervalsList.map { it.key })
      .contains("some-model-line-mapped")
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

  @Test
  fun `sync registersUnregisteredImpressionMetadata with Any-wrapped v1alpha params`() {

    val localImpressionBlobKey = "edp/edp_name/timestamp/impressions"
    val localImpressionBlobUri = "file:////edp/edp_name/timestamp/impressions"
    val localMetadataBlobKey = "edp/edp_name/timestamp/metadata.binpb"
    val localDoneBlobUri = "file:////edp/edp_name/timestamp/done"

    val blobDetails = blobDetails {
      blobUri = localImpressionBlobUri
      eventGroupReferenceId = "reference-id"
      modelLine = "modelProviders/mp1/modelSuites/ms1/modelLines/some-model-line"
      interval = interval {
        startTime = timestamp { seconds = 1735689600 }
        endTime = timestamp { seconds = 1736467200 }
      }
    }

    // Write runtime config to the config bucket
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    val runtimeConfig = dataAvailabilitySyncConfigs {
      configs += fileSystemDataAvailabilitySyncConfig()
    }
    File(configBucketDir, "config.textproto")
      .writeText(TextFormat.printer().printToString(runtimeConfig))

    // Build the Any-wrapped params JSON
    val params = dataAvailabilitySyncParams { dataProvider = "dataProviders/edp123" }
    val any = Any.pack(params)
    val anyTypeRegistry =
      TypeRegistry.newBuilder().add(DataAvailabilitySyncParams.getDescriptor()).build()
    val anyJson = JsonFormat.printer().usingTypeRegistry(anyTypeRegistry).print(any)

    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val port = runBlocking {
      functionProcess.start(
        mapOf(
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "IMPRESSION_METADATA_TARGET" to "localhost:${grpcServer.port}",
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "config.textproto",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }

    val url = "http://localhost:$port"
    logger.info("Testing Cloud Function at: $url")

    // Set up model-line date paths for gap monitor (single date = no gaps)
    val modelLineDatePath = "edp/edp_name/model-line/some-model-line/2025-01-05"
    File(tempFolder.root, "$modelLineDatePath/").mkdirs()
    runBlocking {
      val fsClient = FileSystemStorageClient(File(tempFolder.root.toString()))
      fsClient.writeBlob("$modelLineDatePath/done", emptyFlow())
      fsClient.writeBlob("$modelLineDatePath/data_file", emptyFlow())
    }

    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    runBlocking {
      storageClient.writeBlob(localImpressionBlobKey, emptyFlow())
      storageClient.writeBlob(localMetadataBlobKey, flowOf(blobDetails.toByteString()))
    }
    val client = HttpClient.newHttpClient()
    val getRequest =
      HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("X-DataWatcher-Path", localDoneBlobUri)
        .header("X-DataWatcher-Generation", DONE_BLOB_GENERATION.toString())
        .POST(HttpRequest.BodyPublishers.ofString(anyJson))
        .build()
    val getResponse = client.send(getRequest, HttpResponse.BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")

    val requestCaptor = argumentCaptor<ReplaceDataAvailabilityIntervalsRequest>()
    verifyBlocking(dataProvidersServiceMock, times(1)) {
      replaceDataAvailabilityIntervals(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.name).isEqualTo("dataProviders/edp123")
    assertThat(requestCaptor.firstValue.dataAvailabilityIntervalsList.map { it.key })
      .contains("some-model-line-mapped")
    verifyBlocking(impressionMetadataServiceMock, times(1)) { batchCreateImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(1)) { computeModelLineBounds(any()) }
  }

  @Test
  fun `sync returns error for Any-wrapped params with invalid data provider`() {
    // Write runtime config to the config bucket
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    val runtimeConfig = dataAvailabilitySyncConfigs {
      configs += fileSystemDataAvailabilitySyncConfig()
    }
    File(configBucketDir, "config.textproto")
      .writeText(TextFormat.printer().printToString(runtimeConfig))

    // Build Any-wrapped params with a data provider that doesn't match any config
    val params = dataAvailabilitySyncParams { dataProvider = "dataProviders/nonexistent" }
    val any = Any.pack(params)
    val anyTypeRegistry =
      TypeRegistry.newBuilder().add(DataAvailabilitySyncParams.getDescriptor()).build()
    val anyJson = JsonFormat.printer().usingTypeRegistry(anyTypeRegistry).print(any)

    val port = runBlocking {
      functionProcess.start(
        mapOf(
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "IMPRESSION_METADATA_TARGET" to "localhost:${grpcServer.port}",
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "config.textproto",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }

    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .POST(HttpRequest.BodyPublishers.ofString(anyJson))
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(500)
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `sync returns error for Any-wrapped params with unsupported type`() {
    // Write runtime config to the config bucket
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    val runtimeConfig = dataAvailabilitySyncConfigs {
      configs += fileSystemDataAvailabilitySyncConfig()
    }
    File(configBucketDir, "config.textproto")
      .writeText(TextFormat.printer().printToString(runtimeConfig))

    // Build Any-wrapped params with a type that is not DataAvailabilitySyncParams
    val params = eventGroupSyncParams { dataProvider = "dataProviders/edp123" }
    val any = Any.pack(params)
    val anyTypeRegistry =
      TypeRegistry.newBuilder().add(EventGroupSyncParams.getDescriptor()).build()
    val invalidAnyJson = JsonFormat.printer().usingTypeRegistry(anyTypeRegistry).print(any)

    val port = runBlocking {
      functionProcess.start(
        mapOf(
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "IMPRESSION_METADATA_TARGET" to "localhost:${grpcServer.port}",
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "config.textproto",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }

    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .POST(HttpRequest.BodyPublishers.ofString(invalidAnyJson))
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(500)
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
  }

  @Test
  fun `sync returns error when date gaps are detected`() {
    val modelLineResourceName = "modelProviders/mp1/modelSuites/ms1/modelLines/modelLine1"
    val modelLineId = "modelLine1"
    val localImpressionBlobKey = "edp/edp_name/timestamp/impressions"
    val localImpressionBlobUri = "file:////edp/edp_name/timestamp/impressions"
    val localMetadataBlobKey = "edp/edp_name/timestamp/metadata.binpb"
    val localDoneBlobUri = "file:////edp/edp_name/timestamp/done"

    val blobDetails = blobDetails {
      blobUri = localImpressionBlobUri
      eventGroupReferenceId = "reference-id"
      modelLine = modelLineResourceName
      interval = interval {
        startTime = timestamp { seconds = 1735689600 }
        endTime = timestamp { seconds = 1736467200 }
      }
    }

    val dataAvailabilitySyncConfig = dataAvailabilitySyncConfig {
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
      modelLineMap[modelLineResourceName] = modelLineList { modelLines += "some-model-line-mapped" }
      errorIfGapsExist = true
    }

    // Write runtime config to the config bucket
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    val runtimeConfig = dataAvailabilitySyncConfigs { configs += dataAvailabilitySyncConfig }
    File(configBucketDir, "config.textproto")
      .writeText(TextFormat.printer().printToString(runtimeConfig))

    // Set up model-line date paths with a gap (missing 2026-03-14)
    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    for (date in listOf("2026-03-13", "2026-03-15")) {
      val donePath = "edp/edp_name/model-line/$modelLineId/$date/done"
      File(tempFolder.root, donePath).parentFile.mkdirs()
      runBlocking {
        storageClient.writeBlob(donePath, emptyFlow())
        storageClient.writeBlob(
          "edp/edp_name/model-line/$modelLineId/$date/data_campaign_1",
          emptyFlow(),
        )
      }
    }

    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val port = runBlocking {
      functionProcess.start(
        mapOf(
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "IMPRESSION_METADATA_TARGET" to "localhost:${grpcServer.port}",
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "config.textproto",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        )
      )
    }

    runBlocking {
      storageClient.writeBlob(localImpressionBlobKey, emptyFlow())
      storageClient.writeBlob(localMetadataBlobKey, flowOf(blobDetails.toByteString()))
    }

    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .header("X-DataWatcher-Path", localDoneBlobUri)
        .header("X-DataWatcher-Generation", DONE_BLOB_GENERATION.toString())
        .POST(HttpRequest.BodyPublishers.ofString(dataAvailabilitySyncConfig.toJson()))
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    logger.info("Gap detection response status: ${response.statusCode()}")
    logger.info("Gap detection response body: ${response.body()}")

    assertThat(response.statusCode()).isEqualTo(200)
    verifyBlocking(dataProvidersServiceMock, times(0)) { replaceDataAvailabilityIntervals(any()) }
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
      modelLineMap["modelProviders/mp1/modelSuites/ms1/modelLines/some-model-line"] =
        modelLineList {
          modelLines += "some-model-line-mapped"
        }
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

    private const val DONE_BLOB_GENERATION = 77L
    private const val DATA_PROVIDER = "dataProviders/edp123"
    private const val RAW_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/upload"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/some-model-line"
    private const val WORK_ITEM_NAME = "workItems/das-123"

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
