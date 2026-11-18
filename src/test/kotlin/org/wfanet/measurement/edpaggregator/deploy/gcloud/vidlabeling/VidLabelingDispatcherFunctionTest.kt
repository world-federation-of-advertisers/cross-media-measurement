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
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.junit.After
import org.junit.Before
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
import org.mockito.kotlin.wheneverBlocking
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
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.gcsStorage
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigKt.modelLineConfig
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.config.edpaggregator.transportLayerSecurityParams
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfigs
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadRegistrationCompleteRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingDispatcherParams
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class VidLabelingDispatcherFunctionTest {

  private lateinit var grpcServer: CommonServer
  private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

  private val loseLeaseOnMetadataRead = AtomicBoolean(false)
  private val leaseLost = AtomicBoolean(false)
  private val activeAttempt = workItemAttempt {
    name = "workItems/dispatch/workItemAttempts/attempt"
    state = WorkItemAttempt.State.ACTIVE
    workItemGeneration = 7L
    leaseExpirationTime = Instant.parse("2099-01-01T00:00:00Z").toProtoTime()
  }
  private val attempts: WorkItemAttemptsCoroutineImplBase = mockService {
    onBlocking { createWorkItemAttempt(any()) }.thenReturn(activeAttempt)
    onBlocking { renewWorkItemAttempt(any()) }
      .thenAnswer {
        if (leaseLost.get()) throw io.grpc.Status.FAILED_PRECONDITION.asException()
        activeAttempt
      }
    onBlocking { failWorkItemAttempt(any()) }
      .thenReturn(activeAttempt.toBuilder().setState(WorkItemAttempt.State.FAILED).build())
    onBlocking { completeWorkItemAttempt(any()) }
      .thenReturn(activeAttempt.toBuilder().setState(WorkItemAttempt.State.SUCCEEDED).build())
  }
  private val items: WorkItemsCoroutineImplBase = mockService {
    onBlocking { failWorkItem(any()) }
      .thenReturn(
        workItem {
          name = "workItems/dispatch"
          generation = 7L
          state = WorkItem.State.FAILED
        }
      )
  }

  private val rawImpressionUploadServiceMock: RawImpressionUploadServiceCoroutineImplBase =
    mockService {
      onBlocking { createRawImpressionUpload(any<CreateRawImpressionUploadRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<CreateRawImpressionUploadRequest>(0)
          RawImpressionUpload.newBuilder()
            .setName("${request.parent}/rawImpressionUploads/upload-1")
            .setUploadHealingOperation(request.rawImpressionUpload.uploadHealingOperation)
            .setDoneBlobUri("file:////edp/edp_name/timestamp/done")
            .setEtag(UPLOAD_ETAG)
            .build()
        }
      onBlocking { markRawImpressionUploadRegistrationComplete(any()) }
        .thenReturn(
          RawImpressionUpload.newBuilder()
            .setName("$DATA_PROVIDER/rawImpressionUploads/upload-1")
            .setDoneBlobUri("file:////edp/edp_name/timestamp/done")
            .setRegistrationComplete(true)
            .setEtag("completed-$UPLOAD_ETAG")
            .build()
        )
      // The fast-path sequencer lists uploads after registration; return none so it cleanly
      // no-ops in this Function-wiring test (dispatch behavior is covered by the unit tests).
      onBlocking { listRawImpressionUploads(any()) }
        .thenAnswer {
          if (loseLeaseOnMetadataRead.get()) leaseLost.set(true)
          listRawImpressionUploadsResponse {}
        }
    }

  private val rawImpressionUploadFileServiceMock: RawImpressionUploadFileServiceCoroutineImplBase =
    mockService {
      onBlocking { batchCreateRawImpressionUploadFiles(any()) }
        .thenReturn(batchCreateRawImpressionUploadFilesResponse {})
      onBlocking { listRawImpressionUploadFiles(any()) }
        .thenReturn(listRawImpressionUploadFilesResponse {})
    }

  private val rawImpressionUploadModelLineServiceMock:
    RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService {
      onBlocking { batchCreateRawImpressionUploadModelLines(any()) }
        .thenReturn(batchCreateRawImpressionUploadModelLinesResponse {})
    }

  private val rankIndexBlobServiceMock: RankIndexBlobServiceCoroutineImplBase = mockService()
  private var activeHealingOperation:
    org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation? =
    null
  private val healingOperationService =
    object :
      org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase() {
      override suspend fun getUploadHealingOperation(
        request: org.wfanet.measurement.edpaggregator.v1alpha.GetUploadHealingOperationRequest
      ): org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation {
        val operation = activeHealingOperation ?: throw io.grpc.Status.NOT_FOUND.asException()
        check(request.name == operation.name)
        return operation
      }
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
              items.bindService(),
              attempts.bindService(),
              rawImpressionUploadFileServiceMock.bindService(),
              rawImpressionUploadModelLineServiceMock.bindService(),
              rankIndexBlobServiceMock.bindService(),
              healingOperationService.bindService(),
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

  private fun startFunction(useFileSystem: Boolean = true): Int {
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    val runtimeConfig = vidLabelingConfigs {
      configs += fileSystemVidLabelingConfig()
      configs +=
        fileSystemVidLabelingConfig()
          .toBuilder()
          .setDataProvider(SECOND_DATA_PROVIDER)
          .setRawImpressionsBlobPrefix("other-provider")
          .setRawImpressionsStorageParams(
            storageParams {
              gcs = gcsStorage {
                projectId = "test-project"
                bucketName = "other-raw-bucket"
              }
            }
          )
          .build()
    }
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
          "CORRECTION_DETECTION_CERT_HOST" to "localhost",
          "CONTROL_PLANE_TARGET" to "localhost:${grpcServer.port}",
          "CONTROL_PLANE_CERT_HOST" to "localhost",
          "VID_LABELER_QUEUE_NAME" to "queues/vid-labeler",
          "POOL_ASSIGNER_QUEUE_NAME" to "queues/pool-assigner",
          "CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "VID_LABELING_KINGDOM_RPC_MIN_INTERVAL" to "1ms",
          "VID_LABELING_METADATA_READ_RPC_MIN_INTERVAL" to "1ms",
          "VID_LABELING_METADATA_WRITE_RPC_MIN_INTERVAL" to "1ms",
          "VID_LABELING_CONTROL_PLANE_RPC_MIN_INTERVAL" to "1ms",
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "config.textproto",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
          "OTEL_PROPAGATORS" to "tracecontext,baggage",
        ) +
          if (useFileSystem)
            mapOf("VID_LABELING_DISPATCHER_FILE_SYSTEM_PATH" to tempFolder.root.path)
          else emptyMap()
      )
    }
  }

  private fun queuedRequest(
    port: Int,
    provider: String,
    sourceUri: String,
    generation: Long = 11L,
  ): Int {
    val task = workItem {
      name = "workItems/dispatch"
      queue = "vid-labeling-dispatch-queue"
      this.generation = 7L
      workItemParams =
        workItemParams {
            appParams =
              vidLabelingDispatchWorkItemParams {
                  dataProvider = provider
                  source = dataPathParams {
                    dataPath = sourceUri
                    this.generation = generation
                    createTime = Timestamps.fromMillis(1L)
                    eventType = WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
                  }
                }
                .pack()
          }
          .pack()
    }
    return HttpClient.newHttpClient()
      .send(
        HttpRequest.newBuilder(URI("http://localhost:$port"))
          .header("Content-Type", "application/octet-stream")
          .POST(HttpRequest.BodyPublishers.ofByteArray(task.toByteArray()))
          .build(),
        HttpResponse.BodyHandlers.ofString(),
      )
      .statusCode()
  }

  private fun assertNoMetadataMutations() {
    verifyBlocking(rawImpressionUploadServiceMock, never()) { createRawImpressionUpload(any()) }
    verifyBlocking(rawImpressionUploadFileServiceMock, never()) {
      batchCreateRawImpressionUploadFiles(any())
    }
    verifyBlocking(rawImpressionUploadModelLineServiceMock, never()) {
      batchCreateRawImpressionUploadModelLines(any())
    }
    verifyBlocking(rawImpressionUploadServiceMock, never()) {
      markRawImpressionUploadRegistrationComplete(any())
    }
    verifyBlocking(items, never()) { createWorkItem(any()) }
  }

  @Test
  fun `queued function rejects another configured provider raw root after claiming`() {
    val port = startFunction(useFileSystem = false)
    assertThat(
        queuedRequest(
          port,
          SECOND_DATA_PROVIDER,
          "gs://raw-impressions-bucket/edp/edp_name/timestamp/done",
        )
      )
      .isEqualTo(200)
    verifyBlocking(attempts, times(1)) { createWorkItemAttempt(any()) }
    verifyBlocking(attempts, times(1)) { failWorkItemAttempt(any()) }
    val failure =
      argumentCaptor<
        org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemRequest
      >()
    verifyBlocking(items, times(1)) { failWorkItem(failure.capture()) }
    assertThat(failure.firstValue.name).isEqualTo("workItems/dispatch")
    assertThat(failure.firstValue.expectedWorkItemGeneration).isEqualTo(7L)
    assertNoMetadataMutations()
  }

  @Test
  fun `queued function rejects a path outside the configured provider prefix`() {
    val port = startFunction(useFileSystem = false)
    assertThat(
        queuedRequest(
          port,
          DATA_PROVIDER,
          "gs://raw-impressions-bucket/other-provider/timestamp/done",
        )
      )
      .isEqualTo(200)
    verifyBlocking(items, times(1)) { failWorkItem(any()) }
    assertNoMetadataMutations()
  }

  @Test
  fun `queued function loses its lease after metadata read and cannot publish writes`() {
    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val parquetConf =
      Configuration().apply { set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") }
    val parquetStorageClient =
      ParquetStorageClient(parquetConf, org.apache.hadoop.fs.Path(tempFolder.root.path))
    runBlocking {
      parquetStorageClient.writeBlob(
        "edp/edp_name/timestamp/impressions_001",
        emptyFlow(),
        mapOf("event_group_reference_id" to "eg-1", "event_date" to "2026-06-01"),
      )
      storageClient.writeBlob("edp/edp_name/timestamp/done", emptyFlow())
    }
    loseLeaseOnMetadataRead.set(true)
    val port = startFunction()
    val generation = File(tempFolder.root, "edp/edp_name/timestamp/done").lastModified()
    assertThat(queuedRequest(port, DATA_PROVIDER, DONE_BLOB_URI, generation)).isEqualTo(503)
    assertThat(leaseLost.get()).isTrue()
    verifyBlocking(attempts, times(2)) { renewWorkItemAttempt(any()) }
    verifyBlocking(attempts, times(1)) { failWorkItemAttempt(any()) }
    verifyBlocking(attempts, never()) { completeWorkItemAttempt(any()) }
    verifyBlocking(items, never()) { failWorkItem(any()) }
    assertNoMetadataMutations()
  }

  @Test
  fun `upload registers upload files and model lines`() {
    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val storageClient = FileSystemStorageClient(tempFolder.root)
    // Use RawLocalFileSystem so writes don't emit ".crc" checksum sidecars, which the dispatcher's
    // directory listing would otherwise pick up as bogus raw-impression files.
    val parquetConf =
      Configuration().apply { set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") }
    val parquetStorageClient =
      ParquetStorageClient(parquetConf, org.apache.hadoop.fs.Path(tempFolder.root.path))
    // The dispatcher reads each raw-impression file's event_date from its plaintext Parquet footer,
    // so the fixtures must be real Parquet files carrying the RawImpressionFileMetadata footer
    // metadata.
    val footer = mapOf("event_group_reference_id" to "eg-1", "event_date" to "2026-06-01")
    runBlocking {
      parquetStorageClient.writeBlob("edp/edp_name/timestamp/impressions_001", emptyFlow(), footer)
      parquetStorageClient.writeBlob("edp/edp_name/timestamp/impressions_002", emptyFlow(), footer)
      // Finalize the upload only after every raw file has been written.
      storageClient.writeBlob("edp/edp_name/timestamp/done", emptyFlow())
    }
    val doneGeneration = File(tempFolder.root, "edp/edp_name/timestamp/done").lastModified()

    val port = startFunction()

    val dispatcherParams = vidLabelingDispatcherParams { dataProvider = DATA_PROVIDER }
    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .header("X-DataWatcher-Path", "file:////edp/edp_name/timestamp/done")
        .header("X-DataWatcher-Generation", doneGeneration.toString())
        .POST(HttpRequest.BodyPublishers.ofString(dispatcherParams.toJson()))
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(200)

    verifyBlocking(rawImpressionUploadServiceMock, times(1)) { createRawImpressionUpload(any()) }
    val requestCaptor = argumentCaptor<BatchCreateRawImpressionUploadFilesRequest>()
    verifyBlocking(rawImpressionUploadFileServiceMock, times(1)) {
      batchCreateRawImpressionUploadFiles(requestCaptor.capture())
    }
    assertThat(
        requestCaptor.firstValue.requestsList.all { it.rawImpressionUploadFile.blobGeneration > 0L }
      )
      .isTrue()
    verifyBlocking(rawImpressionUploadModelLineServiceMock, times(1)) {
      batchCreateRawImpressionUploadModelLines(any())
    }
    val registrationRequestCaptor =
      argumentCaptor<MarkRawImpressionUploadRegistrationCompleteRequest>()
    verifyBlocking(rawImpressionUploadServiceMock, times(1)) {
      markRawImpressionUploadRegistrationComplete(registrationRequestCaptor.capture())
    }
    assertThat(registrationRequestCaptor.firstValue.name)
      .isEqualTo("$DATA_PROVIDER/rawImpressionUploads/upload-1")
    assertThat(registrationRequestCaptor.firstValue.etag).isEqualTo(UPLOAD_ETAG)
    assertThat(registrationRequestCaptor.firstValue.requestId).isNotEmpty()
    verifyBlocking(modelLinesServiceMock, times(1)) { listModelLines(any()) }
  }

  @Test
  fun `upload parses and forwards recovery headers`() {
    File("${tempFolder.root}/edp/edp_name/timestamp").mkdirs()
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val parquetConf =
      Configuration().apply { set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") }
    val parquetStorageClient =
      ParquetStorageClient(parquetConf, org.apache.hadoop.fs.Path(tempFolder.root.path))
    val footer = mapOf("event_group_reference_id" to "eg-1", "event_date" to "2026-06-01")
    runBlocking {
      parquetStorageClient.writeBlob("edp/edp_name/timestamp/impressions_001", emptyFlow(), footer)
      storageClient.writeBlob("edp/edp_name/timestamp/done", emptyFlow())
    }
    val doneGeneration = File(tempFolder.root, "edp/edp_name/timestamp/done").lastModified()
    val sourceUpload = rawImpressionUpload {
      name = RECOVERY_SOURCE_UPLOAD
      state = RawImpressionUpload.State.FAILED
      doneBlobCreateTime = Timestamps.fromMillis(1L)
      doneBlobUri = DONE_BLOB_URI
      doneBlobGeneration = doneGeneration + 1L
    }
    val predecessorUpload = rawImpressionUpload {
      name = RECOVERY_PREDECESSOR_UPLOAD
      doneBlobUri = RECOVERY_PREDECESSOR_DONE_BLOB_URI
      createTime = Timestamps.fromMillis(1L)
    }
    val predecessorReplacement = rawImpressionUpload {
      name = RECOVERY_PREDECESSOR_REPLACEMENT
      state = RawImpressionUpload.State.COMPLETED
      registrationComplete = true
      uploadHealingOperation = "$DATA_PROVIDER/uploadHealingOperations/$EVICTION_OPERATION_ID"
      doneBlobUri = RECOVERY_PREDECESSOR_DONE_BLOB_URI
      createTime = Timestamps.fromMillis(2L)
      replacesRawImpressionUpload = predecessorUpload.name
    }
    activeHealingOperation =
      org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation {
        name = "$DATA_PROVIDER/uploadHealingOperations/$EVICTION_OPERATION_ID"
        state = org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation.State.RECOVERING
        steps +=
          org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep {
            name = "${this@uploadHealingOperation.name}/uploadHealingSteps/1"
            sourceRawImpressionUpload = sourceUpload.name
            cmmsModelLine = MODEL_LINE
            recoveryTarget = true
            state =
              org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep.State.RECOVERY_STARTED
            recoveryDoneBlobGeneration = doneGeneration
            recoveryAction =
              RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
          }
        steps +=
          org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep {
            name = "${this@uploadHealingOperation.name}/uploadHealingSteps/2"
            sourceRawImpressionUpload = predecessorUpload.name
            cmmsModelLine = MODEL_LINE
            recoveryTarget = true
            state = org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep.State.COMPLETE
            replacementRawImpressionUpload = predecessorReplacement.name
          }
      }
    wheneverBlocking { rawImpressionUploadServiceMock.getRawImpressionUpload(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<GetRawImpressionUploadRequest>(0)
        when (request.name) {
          sourceUpload.name -> sourceUpload
          predecessorUpload.name -> predecessorUpload
          else -> error("Unexpected upload: ${request.name}")
        }
      }
    wheneverBlocking { rawImpressionUploadServiceMock.listRawImpressionUploads(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<ListRawImpressionUploadsRequest>(0)
        listRawImpressionUploadsResponse {
          when (request.filter.doneBlobUri) {
            DONE_BLOB_URI -> rawImpressionUploads += sourceUpload
            RECOVERY_PREDECESSOR_DONE_BLOB_URI -> {
              rawImpressionUploads += predecessorUpload
              rawImpressionUploads += predecessorReplacement
            }
          }
        }
      }
    wheneverBlocking {
        rawImpressionUploadModelLineServiceMock.listRawImpressionUploadModelLines(any())
      }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<ListRawImpressionUploadModelLinesRequest>(0)
        listRawImpressionUploadModelLinesResponse {
          when (request.parent) {
            sourceUpload.name -> rawImpressionUploadModelLines += rawImpressionUploadModelLine {
                name = "${sourceUpload.name}/rawImpressionUploadModelLines/source-model-line"
                cmmsModelLine = MODEL_LINE
                state = RawImpressionUploadModelLine.State.FAILED
                failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
                recoveryAction =
                  RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
                evictionOperationId = EVICTION_OPERATION_ID
                recoveryPredecessorRawImpressionUpload = predecessorUpload.name
              }
            predecessorReplacement.name -> rawImpressionUploadModelLines +=
                rawImpressionUploadModelLine {
                  name =
                    "${predecessorReplacement.name}/rawImpressionUploadModelLines/replacement-model-line"
                  cmmsModelLine = MODEL_LINE
                  state = RawImpressionUploadModelLine.State.COMPLETED
                }
          }
        }
      }
    wheneverBlocking { rankIndexBlobServiceMock.listRankIndexBlobs(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<ListRankIndexBlobsRequest>(0)
        listRankIndexBlobsResponse {
          rankIndexBlobs += rankIndexBlob {
            name = "${request.parent}/rankIndexBlobs/snapshot"
            blobType = RankIndexBlob.BlobType.SNAPSHOT
            cmmsModelLine = MODEL_LINE
            if (request.parent == sourceUpload.name) {
              deleteTime = Timestamps.fromMillis(1L)
            }
          }
        }
      }

    val port = startFunction()
    val dispatcherParams = vidLabelingDispatcherParams { dataProvider = DATA_PROVIDER }
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:$port"))
        .header("X-DataWatcher-Path", DONE_BLOB_URI)
        .header("X-DataWatcher-Generation", doneGeneration.toString())
        .header("X-Override-Model-Lines", MODEL_LINE)
        .header("X-Recovery-Source-Upload", sourceUpload.name)
        .header("X-Eviction-Operation-Id", EVICTION_OPERATION_ID)
        .POST(HttpRequest.BodyPublishers.ofString(dispatcherParams.toJson()))
        .build()

    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(200)
    verifyBlocking(rawImpressionUploadServiceMock) { createRawImpressionUpload(any()) }
    val modelLineRequest = argumentCaptor<BatchCreateRawImpressionUploadModelLinesRequest>()
    verifyBlocking(rawImpressionUploadModelLineServiceMock) {
      batchCreateRawImpressionUploadModelLines(modelLineRequest.capture())
    }
    assertThat(
        modelLineRequest.firstValue.requestsList.map {
          it.rawImpressionUploadModelLine.cmmsModelLine
        }
      )
      .containsExactly(MODEL_LINE)
    verifyBlocking(modelLinesServiceMock, never()) { listModelLines(any()) }
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
    rawImpressionsBlobPrefix = "edp/edp_name"
    edpImpressionPath = "edp7"
    maxFileBatchSizeBytes = 1_000_000_000
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
    edpImpressionPath = "edp/edp7"
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
      eventTemplateDescriptorBlobUri = "gs://descriptors/event-template-set.binpb"
      eventTemplateType = "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
      requiredEntityKeyFieldMapping["person"] = "person_col"
      populationSpecBlobUri = "gs://configs/population-spec.textproto"
      eventTemplateVersion = 1
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
    private const val SECOND_DATA_PROVIDER = "dataProviders/edp456"
    private const val MODEL_SUITE = "modelProviders/mp1/modelSuites/ms1"
    private const val MODEL_LINE = "$MODEL_SUITE/modelLines/ml1"
    private const val MODEL_RELEASE = "$MODEL_SUITE/modelReleases/mr1"
    private const val UPLOAD_ETAG = "upload-etag"
    private const val DONE_BLOB_URI = "file:////edp/edp_name/timestamp/done"
    private const val RECOVERY_SOURCE_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/source-upload"
    private const val RECOVERY_PREDECESSOR_UPLOAD =
      "$DATA_PROVIDER/rawImpressionUploads/predecessor-upload"
    private const val RECOVERY_PREDECESSOR_REPLACEMENT =
      "$DATA_PROVIDER/rawImpressionUploads/predecessor-replacement"
    private const val RECOVERY_PREDECESSOR_DONE_BLOB_URI = "file:////edp/edp_name/predecessor/done"
    private const val EVICTION_OPERATION_ID = "123e4567-e89b-42d3-a456-426614174000"

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
