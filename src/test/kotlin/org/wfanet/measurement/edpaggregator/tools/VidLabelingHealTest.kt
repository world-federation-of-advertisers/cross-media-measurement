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

import com.google.common.truth.Truth.assertThat
import com.google.devtools.build.runfiles.Runfiles
import com.google.protobuf.Any
import com.google.protobuf.Struct
import com.google.protobuf.TextFormat
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import io.cloudevents.core.builder.CloudEventBuilder
import io.grpc.BindableService
import io.grpc.Status
import io.grpc.netty.NettyServerBuilder
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.TimeUnit.SECONDS
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.edpaggregator.DataWatcherConfigTypes
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.controlPlaneQueueSink
import org.wfanet.measurement.config.securecomputation.dataWatcherConfig
import org.wfanet.measurement.config.securecomputation.watchedPath
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.ApproveUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParamsKt.recoveryParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingDispatcherParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidRankBuilderParams
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GetWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.RetryWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.copy
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs
import org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcherFunction
import picocli.CommandLine

@RunWith(JUnit4::class)
class VidLabelingHealTest {
  @Test
  fun `command exposes only supported operator actions`() {
    assertThat(CommandLine(VidLabelingHeal()).subcommands.keys)
      .containsExactly(
        "get-dispatch-work-item",
        "retry-dispatch-work-item",
        "preflight-dispatch-cutover",
        "smoke-dispatch-cutover",
        "prepare-dispatch-canary-config",
        "inspect-dispatch-cutover-config",
        "mark-failed",
        "retry-failed",
        "backfill-model-line",
        "list-correction-candidates",
        "get-correction-candidate",
        "list-correction-plans",
        "get-correction-plan",
        "approve-correction-plan",
        "retry-correction-plan",
        "redeliver-dlq",
        "help",
      )
  }

  @Test
  fun `command line rejects negative RPC interval`() {
    val exception =
      assertFailsWith<CommandLine.ParameterException> {
        CommandLine(VidLabelingHeal())
          .parseArgs("retry-failed", "--metadata-read-rpc-min-interval=-1s")
      }

    assertThat(exception).hasMessageThat().contains("complete human-readable duration")
  }

  @Test
  fun `command line rejects partially malformed RPC interval`() {
    val exception =
      assertFailsWith<CommandLine.ParameterException> {
        CommandLine(VidLabelingHeal())
          .parseArgs("retry-failed", "--control-plane-rpc-min-interval=500msjunk")
      }

    assertThat(exception).hasMessageThat().contains("complete human-readable duration")
  }

  @Test
  fun `retry failed reads owner metadata from EDPA and republishes through the control plane`() {
    val upload = "dataProviders/dp/rawImpressionUploads/upload"
    val model = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    val row = rawImpressionUploadModelLine {
      name = "$upload/rawImpressionUploadModelLines/ml"
      cmmsModelLine = model
      state = RawImpressionUploadModelLine.State.FAILED
      failureReason = RawImpressionUploadModelLine.FailureReason.PROCESSING_FAILURE
      failureAttemptId = "failed-attempt"
    }
    val models: RawImpressionUploadModelLineServiceCoroutineImplBase = mockService {
      onBlocking { listRawImpressionUploadModelLines(any()) }
        .thenReturn(
          listRawImpressionUploadModelLinesResponse { rawImpressionUploadModelLines += row }
        )
      onBlocking { markRawImpressionUploadModelLineRanking(any()) }
        .thenReturn(row.copy { state = RawImpressionUploadModelLine.State.RANKING })
    }
    val uploads: RawImpressionUploadServiceCoroutineImplBase = mockService {
      onBlocking { getRawImpressionUpload(any()) }
        .thenReturn(
          rawImpressionUpload {
            name = upload
            registrationComplete = true
            state = RawImpressionUpload.State.ACTIVE
          }
        )
      onBlocking { listRawImpressionUploads(any()) }.thenReturn(listRawImpressionUploadsResponse {})
    }
    val ranks: RankerJobServiceCoroutineImplBase = mockService {
      onBlocking { listRankerJobs(any()) }
        .thenReturn(
          listRankerJobsResponse {
            rankerJobs += rankerJob {
              name = "$upload/rankerJobs/rank"
              state = RankerJob.State.CREATED
            }
          }
        )
    }
    val labels: VidLabelingJobServiceCoroutineImplBase = mockService {
      onBlocking { listVidLabelingJobs(any()) }.thenReturn(listVidLabelingJobsResponse {})
    }
    val original = workItem {
      name = "workItems/${WorkItemIds.forVidRankBuilder("$upload/rankerJobs/rank")}"
      queue = "rank-queue"
      state = WorkItem.State.FAILED
      generation = 1L
      workItemParams =
        Any.pack(
          workItemParams {
            appParams =
              Any.pack(
                vidRankBuilderParams {
                  modelLine = model
                  allowedPriorSnapshotRawImpressionUploads += "stale-owner"
                }
              )
          }
        )
    }
    val workItems: WorkItemsCoroutineImplBase = mockService {
      onBlocking { getWorkItem(any()) }
        .thenAnswer { invocation ->
          if (invocation.getArgument<GetWorkItemRequest>(0).name != original.name) {
            throw Status.NOT_FOUND.asRuntimeException()
          }
          original
        }
      onBlocking { createWorkItem(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<CreateWorkItemRequest>(0)
          request.workItem.copy {
            name = "workItems/${request.workItemId}"
            state = WorkItem.State.QUEUED
            generation = 1L
          }
        }
    }
    val secrets =
      checkNotNull(
        Runfiles.preload()
          .withSourceRepository("")
          .getRuntimePath(
            Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
          )
      )
    val certs =
      SigningCerts.fromPemFiles(
        certificateFile = secrets.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = secrets.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = secrets.resolve("edp7_root.pem").toFile(),
      )
    val edpa =
      NettyServerBuilder.forPort(0)
        .sslContext(certs.toServerTlsContext())
        .addService(models)
        .addService(uploads)
        .addService(ranks)
        .addService(labels)
        .build()
        .start()
    val controlPlane =
      NettyServerBuilder.forPort(0)
        .sslContext(certs.toServerTlsContext())
        .addService(workItems)
        .build()
        .start()
    try {
      val code =
        CommandLine(VidLabelingHeal())
          .execute(
            "retry-failed",
            "--edpa-public-api-target=localhost:${edpa.port}",
            "--edpa-public-api-cert-host=localhost",
            "--control-plane-api-target=localhost:${controlPlane.port}",
            "--control-plane-api-cert-host=localhost",
            "--raw-impression-upload=$upload",
            "--model-line=$model",
            "--tls-cert-file=${secrets.resolve("edp7_tls.pem")}",
            "--tls-key-file=${secrets.resolve("edp7_tls.key")}",
            "--cert-collection-file=${secrets.resolve("kingdom_root.pem")}",
            "--control-plane-tls-cert-file=${secrets.resolve("edp7_tls.pem")}",
            "--control-plane-tls-key-file=${secrets.resolve("edp7_tls.key")}",
            "--control-plane-cert-collection-file=${secrets.resolve("kingdom_root.pem")}",
            "--metadata-read-rpc-min-interval=1ms",
            "--metadata-write-rpc-min-interval=1ms",
            "--control-plane-rpc-min-interval=1ms",
          )
      assertThat(code).isEqualTo(0)
      verifyBlocking(uploads) { getRawImpressionUpload(any()) }
      val requests = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItems) { createWorkItem(requests.capture()) }
      val params =
        requests.firstValue.workItem.workItemParams
          .unpack(WorkItem.WorkItemParams::class.java)
          .appParams
          .unpack(VidRankBuilderParams::class.java)
      assertThat(params.allowedPriorSnapshotRawImpressionUploadsList).isEmpty()
    } finally {
      edpa.shutdownNow().awaitTermination(1, SECONDS)
      controlPlane.shutdownNow().awaitTermination(1, SECONDS)
    }
  }

  @Test
  fun `retry failed accepts separate EDPA and control plane TLS files`() {
    val result =
      CommandLine(VidLabelingHeal())
        .parseArgs(
          "retry-failed",
          "--edpa-public-api-target=edpa.example:443",
          "--control-plane-api-target=control-plane.example:443",
          "--raw-impression-upload=dataProviders/dp/rawImpressionUploads/upload",
          "--model-line=modelProviders/mp/modelSuites/ms/modelLines/ml",
          "--tls-cert-file=edpa.pem",
          "--tls-key-file=edpa.key",
          "--cert-collection-file=edpa-roots.pem",
          "--control-plane-tls-cert-file=control-plane.pem",
          "--control-plane-tls-key-file=control-plane.key",
          "--control-plane-cert-collection-file=control-plane-roots.pem",
        )

    val retryFailed = checkNotNull(result.subcommand())
    assertThat(retryFailed.hasMatchedOption("--tls-cert-file")).isTrue()
    assertThat(retryFailed.hasMatchedOption("--control-plane-tls-cert-file")).isTrue()
  }

  private fun runProductionMain(vararg args: String): Int {
    val captured = CommandLineTesting.capturingOutput(arrayOf(*args), ::main)
    print(captured.out)
    System.err.print(captured.err)
    return captured.status
  }

  @Suppress("DEPRECATION")
  companion object {
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }
  }

  private class DispatchService(var current: WorkItem) : WorkItemsCoroutineImplBase() {
    var getCalls = 0
    val retries = mutableListOf<RetryWorkItemRequest>()
    var rejectRetry = false

    override suspend fun getWorkItem(request: GetWorkItemRequest): WorkItem {
      getCalls++
      require(request.name == current.name)
      return current
    }

    override suspend fun retryWorkItem(request: RetryWorkItemRequest): WorkItem {
      retries += request
      if (rejectRetry)
        throw Status.FAILED_PRECONDITION.withDescription("generation changed").asRuntimeException()
      require(request.expectedWorkItemGeneration == current.generation)
      current =
        current.copy {
          generation = current.generation + 1
          state = WorkItem.State.QUEUED
        }
      return current
    }
  }

  private fun dispatchItem(
    state: WorkItem.State = WorkItem.State.FAILED,
    executionGeneration: Long = 7L,
  ): WorkItem = workItem {
    name = "workItems/dispatch"
    queue = "vid-labeling-dispatch-queue"
    this.state = state
    generation = executionGeneration
    workItemParams =
      Any.pack(
        workItemParams {
          appParams =
            Any.pack(vidLabelingDispatchWorkItemParams { dataProvider = "dataProviders/dp" })
          dataPathParams = dataPathParams {
            dataPath = "gs://isolated/raw/done"
            generation = 123456789L
            createTime = timestamp { seconds = 123L }
          }
        }
      )
  }

  private fun withOperatorServer(
    services: List<BindableService>,
    block: (List<String>, List<String>) -> Unit,
  ) {
    val secrets =
      checkNotNull(
        Runfiles.preload()
          .withSourceRepository("")
          .getRuntimePath(
            Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
          )
      )
    val certs =
      SigningCerts.fromPemFiles(
        secrets.resolve("kingdom_tls.pem").toFile(),
        secrets.resolve("kingdom_tls.key").toFile(),
        secrets.resolve("edp7_root.pem").toFile(),
      )
    val builder = NettyServerBuilder.forPort(0).sslContext(certs.toServerTlsContext())
    services.forEach(builder::addService)
    val server = builder.build().start()
    try {
      val controlFlags =
        listOf(
          "--control-plane-api-target=localhost:${server.port}",
          "--control-plane-api-cert-host=localhost",
          "--control-plane-tls-cert-file=${secrets.resolve("edp7_tls.pem")}",
          "--control-plane-tls-key-file=${secrets.resolve("edp7_tls.key")}",
          "--control-plane-cert-collection-file=${secrets.resolve("kingdom_root.pem")}",
        )
      val edpaFlags =
        listOf(
          "--edpa-public-api-target=localhost:${server.port}",
          "--edpa-public-api-cert-host=localhost",
          "--tls-cert-file=${secrets.resolve("edp7_tls.pem")}",
          "--tls-key-file=${secrets.resolve("edp7_tls.key")}",
          "--cert-collection-file=${secrets.resolve("kingdom_root.pem")}",
        )
      block(controlFlags, edpaFlags)
    } finally {
      server.shutdownNow().awaitTermination(1, SECONDS)
    }
  }

  @Test
  fun `retry dispatch sends the inspected execution generation and preserves parameters`() {
    val original = dispatchItem()
    val service = DispatchService(original)
    val bytes = ByteArrayOutputStream()
    val previousOut = System.out
    try {
      System.setOut(PrintStream(bytes))
      withOperatorServer(listOf(service)) { flags, _ ->
        val code =
          runProductionMain(
            *(listOf(
                "retry-dispatch-work-item",
                original.name,
                "--expected-work-item-generation=7",
              ) + flags)
              .toTypedArray()
          )
        assertThat(code).isEqualTo(0)
      }
    } finally {
      System.setOut(previousOut)
    }
    assertThat(bytes.toString()).contains("execution_generation: 8")
    assertThat(bytes.toString()).contains("state: QUEUED")
    assertThat(service.getCalls).isEqualTo(1)
    assertThat(service.retries.single().name).isEqualTo(original.name)
    assertThat(service.retries.single().expectedWorkItemGeneration).isEqualTo(7L)
    assertThat(service.current.generation).isEqualTo(8L)
    assertThat(service.current.state).isEqualTo(WorkItem.State.QUEUED)
    assertThat(service.current.workItemParams).isEqualTo(original.workItemParams)
  }

  @Test
  fun `retry dispatch refuses running queued succeeded and unknown states without retry RPC`() {
    for (state in
      listOf(
        WorkItem.State.RUNNING,
        WorkItem.State.QUEUED,
        WorkItem.State.SUCCEEDED,
        WorkItem.State.STATE_UNSPECIFIED,
      )) {
      val service = DispatchService(dispatchItem(state))
      withOperatorServer(listOf(service)) { flags, _ ->
        assertThat(
            runProductionMain(
              *(listOf(
                  "retry-dispatch-work-item",
                  service.current.name,
                  "--expected-work-item-generation=7",
                ) + flags)
                .toTypedArray()
            )
          )
          .isNotEqualTo(0)
      }
      assertThat(service.retries).isEmpty()
    }
  }

  @Test
  fun `retry dispatch rejects a stale inspected generation before retry RPC`() {
    val service = DispatchService(dispatchItem(executionGeneration = 8L))
    withOperatorServer(listOf(service)) { flags, _ ->
      assertThat(
          runProductionMain(
            *(listOf(
                "retry-dispatch-work-item",
                service.current.name,
                "--expected-work-item-generation=7",
              ) + flags)
              .toTypedArray()
          )
        )
        .isNotEqualTo(0)
    }
    assertThat(service.retries).isEmpty()
  }

  @Test
  fun `retry dispatch requires an explicit positive generation`() {
    val service = DispatchService(dispatchItem())
    withOperatorServer(listOf(service)) { flags, _ ->
      for (generationFlag in
        listOf(
          emptyList(),
          listOf("--expected-work-item-generation=0"),
          listOf("--expected-work-item-generation=-1"),
        )) {
        assertThat(
            runProductionMain(
              *(listOf("retry-dispatch-work-item", service.current.name) + generationFlag + flags)
                .toTypedArray()
            )
          )
          .isNotEqualTo(0)
      }
    }
    assertThat(service.getCalls).isEqualTo(0)
    assertThat(service.retries).isEmpty()
  }

  @Test
  fun `retry dispatch propagates a concurrent server generation rejection without resubmitting`() {
    val service = DispatchService(dispatchItem()).apply { rejectRetry = true }
    withOperatorServer(listOf(service)) { flags, _ ->
      assertThat(
          runProductionMain(
            *(listOf(
                "retry-dispatch-work-item",
                service.current.name,
                "--expected-work-item-generation=7",
              ) + flags)
              .toTypedArray()
          )
        )
        .isNotEqualTo(0)
    }
    assertThat(service.retries).hasSize(1)
    assertThat(service.current.generation).isEqualTo(7L)
  }

  @Test
  fun `inspect dispatch prints source identity separately from execution generation`() {
    val service = DispatchService(dispatchItem())
    val bytes = ByteArrayOutputStream()
    val previousOut = System.out
    try {
      System.setOut(PrintStream(bytes))
      withOperatorServer(listOf(service)) { flags, _ ->
        assertThat(
            runProductionMain(
              *(listOf("get-dispatch-work-item", service.current.name) + flags).toTypedArray()
            )
          )
          .isEqualTo(0)
      }
    } finally {
      System.setOut(previousOut)
    }
    assertThat(bytes.toString()).contains("execution_generation: 7")
    assertThat(bytes.toString()).contains("123456789")
    assertThat(bytes.toString()).contains("gs://isolated/raw/done")
    assertThat(service.retries).isEmpty()
  }

  @Test
  fun `preflight rejects incomplete registration on a later page`() {
    val tokens = mutableListOf<String>()
    val uploads =
      object : RawImpressionUploadServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploads(request: ListRawImpressionUploadsRequest) =
          listRawImpressionUploadsResponse {
            tokens += request.pageToken
            rawImpressionUploads += rawImpressionUpload {
              registrationComplete = request.pageToken.isEmpty()
            }
            if (request.pageToken.isEmpty()) nextPageToken = "second"
          }
      }
    val operations =
      object : UploadHealingOperationServiceCoroutineImplBase() {
        override suspend fun listUploadHealingOperations(
          request: ListUploadHealingOperationsRequest
        ) = listUploadHealingOperationsResponse {}
      }
    withOperatorServer(listOf(uploads, operations)) { _, flags ->
      assertThat(
          runProductionMain(
            *(listOf("preflight-dispatch-cutover", "--data-provider=dataProviders/dp") + flags)
              .toTypedArray()
          )
        )
        .isNotEqualTo(0)
    }
    assertThat(tokens).containsExactly("", "second").inOrder()
  }

  @Test
  fun `preflight rejects active or unknown healing without mutations`() {
    val uploads =
      object : RawImpressionUploadServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploads(request: ListRawImpressionUploadsRequest) =
          listRawImpressionUploadsResponse {}
      }
    val operations =
      object : UploadHealingOperationServiceCoroutineImplBase() {
        override suspend fun listUploadHealingOperations(
          request: ListUploadHealingOperationsRequest
        ) = listUploadHealingOperationsResponse {
          uploadHealingOperations +=
            org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation {}
        }
      }
    withOperatorServer(listOf(uploads, operations)) { _, flags ->
      assertThat(
          runProductionMain(
            *(listOf("preflight-dispatch-cutover", "--data-provider=dataProviders/dp") + flags)
              .toTypedArray()
          )
        )
        .isNotEqualTo(0)
    }
  }

  @Test
  fun `smoke discovers ordinary and recovery work admitted through storage CloudEvents`() {
    EdpaTelemetry.ensureInitialized()
    val provider = "dataProviders/test"
    val operationName = "$provider/uploadHealingOperations/operation"
    val creations = mutableListOf<WorkItem>()
    val stored = mutableMapOf<String, WorkItem>()
    val order = mutableListOf<String>()
    val getCalls = mutableListOf<String>()
    val processingStorageEvent = java.util.concurrent.atomic.AtomicBoolean(false)
    val denyIntake = java.util.concurrent.atomic.AtomicBoolean(false)
    val items =
      object : WorkItemsCoroutineImplBase() {
        override suspend fun createWorkItem(request: CreateWorkItemRequest): WorkItem {
          check(processingStorageEvent.get()) { "Smoke CLI must never create WorkItems directly" }
          if (denyIntake.get()) throw Status.PERMISSION_DENIED.asException()
          creations += request.workItem
          order += "storage-create"
          return request.workItem
            .copy {
              name = "workItems/${request.workItemId}"
              generation = 1L
              state = WorkItem.State.QUEUED
            }
            .also { stored[it.name] = it }
        }

        override suspend fun getWorkItem(request: GetWorkItemRequest): WorkItem {
          getCalls += request.name
          return (stored[request.name] ?: throw Status.NOT_FOUND.asException()).copy {
            state = WorkItem.State.SUCCEEDED
          }
        }
      }
    var approved = false
    var selectedAction =
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
    var recordedGeneration = 0L
    var controllerFinalize: (() -> Unit)? = null
    val operations =
      object : UploadHealingOperationServiceCoroutineImplBase() {
        override suspend fun getUploadHealingOperation(
          request: GetUploadHealingOperationRequest
        ): UploadHealingOperation = uploadHealingOperation {
          name = operationName
          etag = "etag"
          state =
            if (approved) UploadHealingOperation.State.RECOVERING
            else UploadHealingOperation.State.APPROVAL_REQUIRED
          planFormatVersion = 2
          rawImpressionUploadCorrectionCandidates +=
            "dataProviders/test/rawImpressionUploadCorrectionCandidates/candidate"
          steps += uploadHealingStep {
            sourceRawImpressionUpload = "dataProviders/test/rawImpressionUploads/source"
            recoveryTarget = true
            recoveryAction = selectedAction
            state =
              if (approved) UploadHealingStep.State.RECOVERY_STARTED
              else UploadHealingStep.State.PENDING_EVICTION
            recoveryDoneBlobGeneration = recordedGeneration
          }
        }

        override suspend fun approveUploadHealingOperation(
          request: ApproveUploadHealingOperationRequest
        ): UploadHealingOperation {
          check(request.name == operationName && request.etag == "etag")
          approved = true
          order += "approve"
          checkNotNull(controllerFinalize).invoke()
          return uploadHealingOperation {
            name = operationName
            state = UploadHealingOperation.State.APPROVED
          }
        }
      }
    val directory = Files.createTempDirectory("dispatch-intake-test")
    val ordinaryPath = directory.resolve("ordinary.json")
    val recoveryPath = directory.resolve("recovery.json")
    val approvalPath = directory.resolve("approval.json")
    val configPath = directory.resolve("final.textproto")
    val canaryPath = directory.resolve("canary.textproto")
    val printer =
      JsonFormat.printer()
        .usingTypeRegistry(
          TypeRegistry.newBuilder()
            .add(WorkItem.WorkItemParams.getDescriptor())
            .add(VidLabelingDispatchWorkItemParams.getDescriptor())
            .build()
        )
    val source =
      dispatchItem()
        .workItemParams
        .unpack(WorkItem.WorkItemParams::class.java)
        .dataPathParams
        .toBuilder()
        .setDataPath("gs://isolated/raw/ordinary/done")
        .build()
    val historicalRecoverySource =
      source
        .toBuilder()
        .setDataPath("gs://isolated/raw/recovery/done")
        .setGeneration(source.generation + 1)
        .build()
    val recoverySource =
      historicalRecoverySource
        .toBuilder()
        .setGeneration(historicalRecoverySource.generation + 1)
        .build()
    val ordinary = workItem {
      queue = "vid-labeling-dispatch-queue"
      workItemParams =
        Any.pack(
          workItemParams {
            dataPathParams = source
            appParams = Any.pack(vidLabelingDispatchWorkItemParams { dataProvider = provider })
          }
        )
    }
    val recovery = workItem {
      queue = ordinary.queue
      workItemParams =
        Any.pack(
          workItemParams {
            appParams =
              Any.pack(
                vidLabelingDispatchWorkItemParams {
                  dataProvider = provider
                  recovery = recoveryParams {
                    this.source = historicalRecoverySource
                    sourceUpload = "$provider/rawImpressionUploads/source"
                    uploadHealingOperation = operationName
                  }
                }
              )
          }
        )
    }
    Files.writeString(ordinaryPath, printer.print(ordinary))
    Files.writeString(recoveryPath, printer.print(recovery))
    Files.writeString(approvalPath, """{"name":"$operationName","etag":"etag"}""")
    val finalConfig = dataWatcherConfig {
      watchedPaths += watchedPath {
        identifier = "vid-labeling-dispatch"
        sourcePathRegex = "^gs://isolated/raw/.+/done$"
        eventTypes += "google.cloud.storage.object.v1.finalized"
        controlPlaneQueueSink = controlPlaneQueueSink {
          queue = ordinary.queue
          appParams = Any.pack(vidLabelingDispatcherParams { dataProvider = provider })
        }
      }
      watchedPaths += watchedPath {
        identifier = "vid-labeling-dispatch"
        sourcePathRegex = "^gs://live/raw/.+/done$"
        controlPlaneQueueSink = controlPlaneQueueSink {
          queue = ordinary.queue
          appParams = Any.pack(vidLabelingDispatcherParams { dataProvider = "dataProviders/live" })
        }
      }
    }
    Files.writeString(
      configPath,
      TextFormat.printer()
        .usingTypeRegistry(DataWatcherConfigTypes.registry)
        .printToString(finalConfig),
    )
    try {
      assertThat(
          runProductionMain(
            "prepare-dispatch-canary-config",
            "--data-watcher-config=$configPath",
            "--output=$canaryPath",
            "--isolated-data-provider=$provider",
            "--dispatcher-endpoint=https://dispatcher.test",
          )
        )
        .isEqualTo(0)
      val canaryBuilder = DataWatcherConfig.newBuilder()
      TextFormat.Parser.newBuilder()
        .setTypeRegistry(DataWatcherConfigTypes.registry)
        .build()
        .merge(Files.readString(canaryPath), canaryBuilder)
      val canary = canaryBuilder.build()
      assertThat(canary.watchedPathsList[0].hasControlPlaneQueueSink()).isTrue()
      assertThat(canary.watchedPathsList[1].hasHttpEndpointSink()).isTrue()
      val uploads =
        object : RawImpressionUploadServiceCoroutineImplBase() {
          override suspend fun getRawImpressionUpload(
            request: GetRawImpressionUploadRequest
          ): RawImpressionUpload = rawImpressionUpload {
            name = "$provider/rawImpressionUploads/source"
            doneBlobUri = recoverySource.dataPath
            doneBlobGeneration = historicalRecoverySource.generation
          }
        }
      withOperatorServer(listOf(items, operations, uploads)) { controlFlags, edpaFlags ->
        fun flag(name: String) =
          controlFlags.single { it.startsWith("--$name=") }.substringAfter('=')
        val channel =
          buildMutualTlsChannel(
            flag("control-plane-api-target"),
            SigningCerts.fromPemFiles(
              java.io.File(flag("control-plane-tls-cert-file")),
              java.io.File(flag("control-plane-tls-key-file")),
              java.io.File(flag("control-plane-cert-collection-file")),
            ),
            "localhost",
          )
        try {
          val watcher = DataWatcher(WorkItemsCoroutineStub(channel), canary.watchedPathsList)
          val function = DataWatcherFunction(watcher::receivePath)
          fun finalizeEvent(
            recoveryEvent: Boolean,
            eventType: String = "google.cloud.storage.object.v1.finalized",
            receiver: DataWatcherFunction = function,
          ) {
            val selected = if (recoveryEvent) recoverySource else source
            val metadata =
              if (recoveryEvent)
                ""","metadata":{"${WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY}":"$provider/rawImpressionUploads/source","${WatchedBlobs.UPLOAD_HEALING_OPERATION_KEY}":"$operationName"}"""
              else ""
            val data =
              """{"bucket":"isolated","name":"${selected.dataPath.substringAfter("gs://isolated/")}","generation":"${selected.generation}","size":"0","timeCreated":"1970-01-01T00:02:03Z"$metadata}"""
            val event =
              CloudEventBuilder.v1()
                .withId("event-${selected.generation}-$eventType")
                .withSource(URI.create("gs://isolated"))
                .withType(eventType)
                .withData(data.toByteArray())
                .build()
            processingStorageEvent.set(true)
            try {
              receiver.accept(event)
            } finally {
              processingStorageEvent.set(false)
            }
          }
          finalizeEvent(false, "google.cloud.storage.object.v1.deleted")
          assertThat(creations).isEmpty()
          val args =
            listOf(
              "smoke-dispatch-cutover",
              "--isolated-data-provider=$provider",
              "--live-data-provider=dataProviders/live",
              "--watcher-identifier=vid-labeling-dispatch",
              "--ordinary-work-item-file=$ordinaryPath",
              "--recovery-work-item-file=$recoveryPath",
              "--approval-file=$approvalPath",
            ) + controlFlags + edpaFlags
          val disabled =
            DataWatcherFunction(
              DataWatcher(
                WorkItemsCoroutineStub(channel),
                canary.watchedPathsList.map { it.toBuilder().setSourcePathRegex("^never$").build() },
              )::receivePath
            )
          finalizeEvent(false, receiver = disabled)
          assertThat(
              runProductionMain(*(args + listOf("--phase=ordinary", "--timeout=1s")).toTypedArray())
            )
            .isNotEqualTo(0)
          assertThat(creations).isEmpty()
          assertThat(approved).isFalse()
          assertThat(getCalls).isNotEmpty()
          getCalls.clear()
          denyIntake.set(true)
          assertFailsWith<RuntimeException> { finalizeEvent(false) }
          assertThat(
              runProductionMain(*(args + listOf("--phase=ordinary", "--timeout=1s")).toTypedArray())
            )
            .isNotEqualTo(0)
          assertThat(creations).isEmpty()
          assertThat(getCalls).isNotEmpty()
          denyIntake.set(false)
          finalizeEvent(false)
          selectedAction =
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
          assertThat(runProductionMain(*(args + "--phase=ordinary").toTypedArray())).isNotEqualTo(0)
          assertThat(approved).isFalse()
          selectedAction =
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
          controllerFinalize = {
            finalizeEvent(true)
            recordedGeneration = recoverySource.generation
          }
          assertThat(runProductionMain(*(args + "--phase=ordinary").toTypedArray())).isEqualTo(0)
          val frame = recovery.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
          val app = frame.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java)
          val fresh =
            recovery.copy {
              workItemParams =
                Any.pack(
                  frame
                    .toBuilder()
                    .setAppParams(
                      Any.pack(
                        app
                          .toBuilder()
                          .setRecovery(app.recovery.toBuilder().setSource(recoverySource))
                          .build()
                      )
                    )
                    .build()
                )
            }
          Files.writeString(recoveryPath, printer.print(fresh))
          assertThat(runProductionMain(*(args + "--phase=recovery").toTypedArray())).isEqualTo(0)
        } finally {
          channel.shutdownNow().awaitTermination(1, SECONDS)
        }
      }
      assertThat(order).containsExactly("storage-create", "approve", "storage-create").inOrder()
      assertThat(creations).hasSize(2)
    } finally {
      for (file in listOf(ordinaryPath, recoveryPath, approvalPath, configPath, canaryPath)) Files
        .deleteIfExists(file)
      Files.deleteIfExists(directory)
    }
  }

  @Test
  fun `preflight accepts complete registrations and inactive approval plans`() {
    val uploads =
      object : RawImpressionUploadServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploads(request: ListRawImpressionUploadsRequest) =
          listRawImpressionUploadsResponse {
            rawImpressionUploads += rawImpressionUpload { registrationComplete = true }
          }
      }
    val operations =
      object : UploadHealingOperationServiceCoroutineImplBase() {
        override suspend fun listUploadHealingOperations(
          request: ListUploadHealingOperationsRequest
        ) = listUploadHealingOperationsResponse {
          uploadHealingOperations += uploadHealingOperation {
            state = UploadHealingOperation.State.COMPLETE
          }
          uploadHealingOperations += uploadHealingOperation {
            state = UploadHealingOperation.State.APPROVAL_REQUIRED
          }
        }
      }
    withOperatorServer(listOf(uploads, operations)) { _, flags ->
      assertThat(
          runProductionMain(
            *(listOf("preflight-dispatch-cutover", "--data-provider=dataProviders/dp") + flags)
              .toTypedArray()
          )
        )
        .isEqualTo(0)
    }
  }

  private fun inspect(vararg args: String): Pair<Int, Struct?> {
    val captured =
      CommandLineTesting.capturingOutput(arrayOf("inspect-dispatch-cutover-config", *args), ::main)
    val line =
      captured.out.lineSequence().singleOrNull {
        it.startsWith(InspectDispatchCutoverConfigCommand.INSPECTION_PREFIX)
      }
    val facts =
      line?.let {
        Struct.newBuilder()
          .apply {
            JsonFormat.parser()
              .merge(it.removePrefix(InspectDispatchCutoverConfigCommand.INSPECTION_PREFIX), this)
          }
          .build()
      }
    return captured.status to facts
  }

  @Test
  fun `inspect reports every provider from valid textproto layouts a line regex misses`() {
    val config = Files.createTempFile("vid-labeling-config", ".textproto")
    try {
      Files.writeString(
        config,
        """
        # proto-message: wfa.measurement.config.edpaggregator.VidLabelingConfigs
        configs { data_provider: "dataProviders/live" }
        configs {
          data_provider : "dataProviders/test"
        }
        """
          .trimIndent(),
      )
      val (status, facts) = inspect("--vid-labeling-config=$config")
      assertThat(status).isEqualTo(0)
      assertThat(
          checkNotNull(facts).fieldsMap.getValue("dataProviders").listValue.valuesList.map {
            it.stringValue
          }
        )
        .containsExactly("dataProviders/live", "dataProviders/test")
        .inOrder()
    } finally {
      Files.deleteIfExists(config)
    }
  }

  @Test
  fun `inspect distinguishes canary routing from fully queued dispatch and rejects malformed config`() {
    val directory = Files.createTempDirectory("dispatch-routing-inspection")
    val finalPath = directory.resolve("final.textproto")
    val canaryPath = directory.resolve("canary.textproto")
    val malformedPath = directory.resolve("malformed.textproto")
    val finalConfig = dataWatcherConfig {
      for (provider in listOf("dataProviders/test", "dataProviders/live")) {
        watchedPaths += watchedPath {
          identifier = "vid-labeling-dispatch"
          sourcePathRegex = "^gs://${provider.substringAfter('/')}/raw/.+/done$"
          controlPlaneQueueSink = controlPlaneQueueSink {
            queue = "vid-labeling-dispatch-queue"
            appParams = Any.pack(vidLabelingDispatcherParams { dataProvider = provider })
          }
        }
      }
    }
    val printed =
      TextFormat.printer()
        .usingTypeRegistry(DataWatcherConfigTypes.registry)
        .printToString(finalConfig)
    // A comment naming the legacy sink must not change the parsed routing.
    Files.writeString(
      finalPath,
      printed + "\n# http_endpoint_sink { endpoint_uri: \"https://legacy\" }\n",
    )
    Files.writeString(malformedPath, printed.dropLast(2))
    try {
      assertThat(
          runProductionMain(
            "prepare-dispatch-canary-config",
            "--data-watcher-config=$finalPath",
            "--output=$canaryPath",
            "--isolated-data-provider=dataProviders/test",
            "--dispatcher-endpoint=https://dispatcher.test",
          )
        )
        .isEqualTo(0)

      val (finalStatus, finalFacts) = inspect("--data-watcher-config=$finalPath")
      assertThat(finalStatus).isEqualTo(0)
      assertThat(checkNotNull(finalFacts).fieldsMap.mapValues { it.value.boolValue })
        .containsExactly(
          "hasDispatchRoutes",
          true,
          "anyDispatchRouteQueued",
          true,
          "allDispatchRoutesQueued",
          true,
        )
      val (canaryStatus, canaryFacts) = inspect("--data-watcher-config=$canaryPath")
      assertThat(canaryStatus).isEqualTo(0)
      assertThat(checkNotNull(canaryFacts).fieldsMap.mapValues { it.value.boolValue })
        .containsExactly(
          "hasDispatchRoutes",
          true,
          "anyDispatchRouteQueued",
          true,
          "allDispatchRoutesQueued",
          false,
        )
      val (malformedStatus, malformedFacts) = inspect("--data-watcher-config=$malformedPath")
      assertThat(malformedStatus).isNotEqualTo(0)
      assertThat(malformedFacts).isNull()
    } finally {
      for (file in listOf(finalPath, canaryPath, malformedPath)) Files.deleteIfExists(file)
      Files.deleteIfExists(directory)
    }
  }

  /** Simulates durable history that grows without bound unless the preflight filters by state. */
  private class HistoryServices(private val activeHealing: Boolean = false) {
    val uploadRequests = mutableListOf<ListRawImpressionUploadsRequest>()
    val healingRequests = mutableListOf<ListUploadHealingOperationsRequest>()

    val uploads =
      object : RawImpressionUploadServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploads(request: ListRawImpressionUploadsRequest) =
          listRawImpressionUploadsResponse {
            uploadRequests += request
            if (request.filter.stateInList.isNotEmpty()) {
              if (RawImpressionUpload.State.CREATED in request.filter.stateInList) {
                rawImpressionUploads += rawImpressionUpload {
                  state = RawImpressionUpload.State.CREATED
                  registrationComplete = true
                }
              }
              return@listRawImpressionUploadsResponse
            }
            // Legacy terminal rows keep registration_complete false; there are many pages.
            repeat(1000) {
              rawImpressionUploads += rawImpressionUpload {
                state = RawImpressionUpload.State.COMPLETED
                registrationComplete = false
              }
            }
            val page = request.pageToken.toIntOrNull() ?: 0
            if (page < HISTORY_PAGES) nextPageToken = "${page + 1}"
          }
      }

    val operations =
      object : UploadHealingOperationServiceCoroutineImplBase() {
        override suspend fun listUploadHealingOperations(
          request: ListUploadHealingOperationsRequest
        ) = listUploadHealingOperationsResponse {
          healingRequests += request
          val states = request.filter.stateInList
          if (states.isNotEmpty()) {
            if (activeHealing && UploadHealingOperation.State.APPROVED in states) {
              uploadHealingOperations += uploadHealingOperation {
                state = UploadHealingOperation.State.APPROVED
              }
            }
            return@listUploadHealingOperationsResponse
          }
          repeat(1000) {
            uploadHealingOperations += uploadHealingOperation {
              state = UploadHealingOperation.State.COMPLETE
            }
          }
          val page = request.pageToken.toIntOrNull() ?: 0
          if (page < HISTORY_PAGES) nextPageToken = "${page + 1}"
        }
      }

    companion object {
      const val HISTORY_PAGES = 5
    }
  }

  @Test
  fun `preflight pages only potentially unfinished registrations and active healing`() {
    val history = HistoryServices()
    withOperatorServer(listOf(history.uploads, history.operations)) { _, flags ->
      assertThat(
          runProductionMain(
            *(listOf("preflight-dispatch-cutover", "--data-provider=dataProviders/dp") + flags)
              .toTypedArray()
          )
        )
        .isEqualTo(0)
    }
    assertThat(history.uploadRequests).hasSize(1)
    assertThat(history.uploadRequests.single().filter.stateInList)
      .containsExactly(RawImpressionUpload.State.CREATED)
    assertThat(history.healingRequests).hasSize(1)
    assertThat(history.healingRequests.single().filter.stateInList)
      .containsExactlyElementsIn(
        UploadHealingOperation.State.values().filter {
          it != UploadHealingOperation.State.STATE_UNSPECIFIED &&
            it != UploadHealingOperation.State.UNRECOGNIZED &&
            it != UploadHealingOperation.State.COMPLETE &&
            it != UploadHealingOperation.State.APPROVAL_REQUIRED
        }
      )
  }

  @Test
  fun `filtered preflight still rejects active healing amid unrelated history`() {
    val history = HistoryServices(activeHealing = true)
    withOperatorServer(listOf(history.uploads, history.operations)) { _, flags ->
      assertThat(
          runProductionMain(
            *(listOf("preflight-dispatch-cutover", "--data-provider=dataProviders/dp") + flags)
              .toTypedArray()
          )
        )
        .isNotEqualTo(0)
    }
    assertThat(history.healingRequests).hasSize(1)
  }
}
