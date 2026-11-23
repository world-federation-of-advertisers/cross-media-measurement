// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import io.grpc.BindableService
import io.grpc.Status
import io.grpc.StatusException
import io.netty.handler.ssl.ClientAuth
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertFailsWith
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.listWorkItemsResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

class VidLabelingRolloutCheckTest {
  private var uploadRows =
    listOf(
      rawImpressionUpload {
        name = UPLOAD
        state = RawImpressionUpload.State.COMPLETED
        registrationComplete = true
        doneBlobUri = DONE
        doneBlobGeneration = 12
      }
    )
  private var items = emptyList<WorkItem>()
  private var operations = emptyList<UploadHealingOperation>()
  private var queryFails = false
  private var stallQuery = false
  private val queryCancelled = AtomicBoolean()
  private val registeredServices = mutableListOf<BindableService>()
  private var repeatedToken = false
  private var uploadReads = 0
  private var shardRows =
    (0..1).map { index ->
      poolAssignmentJob {
        shardIndex = index
        state = PoolAssignmentJob.State.SUCCEEDED
      }
    }
  private var labelFiles = listOf("$UPLOAD/files/a", "$UPLOAD/files/b")
  private var ranked = false
  private var modelState = RawImpressionUploadModelLine.State.COMPLETED
  @get:Rule
  val server = GrpcTestServerRule {
    fun register(service: BindableService) {
      registeredServices += service
      addService(service)
    }
    register(
      object : RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploads(request: ListRawImpressionUploadsRequest) =
          listRawImpressionUploadsResponse {
            uploadReads++
            if (stallQuery) {
              try {
                delay(30_000)
              } finally {
                queryCancelled.set(true)
              }
            }
            if (queryFails) throw Status.UNAVAILABLE.asException()
            val offset = request.pageToken.toIntOrNull() ?: 0
            rawImpressionUploads += uploadRows.drop(offset).take(1)
            if (offset + 1 < uploadRows.size || repeatedToken) nextPageToken = "1"
          }
      }
    )
    register(
      object : WorkItemsGrpcKt.WorkItemsCoroutineImplBase() {
        override suspend fun listWorkItems(request: ListWorkItemsRequest) = listWorkItemsResponse {
          workItems += items
        }
      }
    )
    register(
      object :
        UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase() {
        override suspend fun listUploadHealingOperations(
          request: ListUploadHealingOperationsRequest
        ) = listUploadHealingOperationsResponse { uploadHealingOperations += operations }
      }
    )
    register(
      object :
        RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploadModelLines(
          request: ListRawImpressionUploadModelLinesRequest
        ) = listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            cmmsModelLine = MODEL
            state = modelState
            poolAssignmentComplete = true
          }
        }
      }
    )
    register(
      object : PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase() {
        override suspend fun listPoolAssignmentJobs(request: ListPoolAssignmentJobsRequest) =
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += shardRows
          }
      }
    )
    register(
      object : RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase() {
        override suspend fun listRankerJobs(request: ListRankerJobsRequest) =
          listRankerJobsResponse {
            if (ranked) rankerJobs += rankerJob {}
          }
      }
    )
    register(
      object : VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase() {
        override suspend fun listVidLabelingJobs(request: ListVidLabelingJobsRequest) =
          listVidLabelingJobsResponse {
            vidLabelingJobs += vidLabelingJob {
              state = VidLabelingJob.State.SUCCEEDED
              rawImpressionUploadFiles += labelFiles
            }
          }
      }
    )
    register(
      object :
        RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploadFiles(
          request: ListRawImpressionUploadFilesRequest
        ) = listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += rawImpressionUploadFile { name = "$UPLOAD/files/a" }
          rawImpressionUploadFiles += rawImpressionUploadFile { name = "$UPLOAD/files/b" }
        }
      }
    )
  }

  private fun checker(leaseProbe: (suspend (Set<String>) -> Unit)? = null) =
    VidLabelingRolloutChecker(
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(server.channel),
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        server.channel
      ),
      PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(server.channel),
      RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(server.channel),
      VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(server.channel),
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
        server.channel
      ),
      UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
        server.channel
      ),
      WorkItemsGrpcKt.WorkItemsCoroutineStub(server.channel),
      requireNoSyncLeases = leaseProbe,
    )

  private suspend fun drained() = checker().requireDrained(setOf("dataProviders/dp"), setOf(QUEUE))

  private suspend fun smoke() =
    checker().requireZeroMembership("dataProviders/dp", DONE, 12, MODEL, 2)

  @Test
  fun `maintenance requires authoritative lease evidence`() =
    runBlocking<Unit> {
      assertFailsWith<IllegalStateException> {
        checker().requireMaintenanceDrained(setOf("dataProviders/dp"), setOf(QUEUE))
      }
    }

  @Test
  fun `maintenance verifies leases and stable state twice`() =
    runBlocking<Unit> {
      var probes = 0
      checker { providers ->
          assertThat(providers).containsExactly("dataProviders/dp")
          probes++
        }
        .requireMaintenanceDrained(setOf("dataProviders/dp"), setOf(QUEUE))
      assertThat(probes).isEqualTo(2)
      assertThat(uploadReads).isEqualTo(2)
    }

  @Test
  fun `active lease and state changing during collection fail closed`() =
    runBlocking<Unit> {
      assertFailsWith<IllegalStateException> {
        checker { throw IllegalStateException("active lease") }
          .requireMaintenanceDrained(setOf("dataProviders/dp"), setOf(QUEUE))
      }
      assertFailsWith<IllegalStateException> {
        checker {
            items =
              listOf(
                workItem {
                  queue = QUEUE
                  state = WorkItem.State.QUEUED
                }
              )
          }
          .requireMaintenanceDrained(setOf("dataProviders/dp"), setOf(QUEUE))
      }
    }

  @Test
  fun `drained evidence is read completely without mutation`() =
    runBlocking<Unit> {
      uploadRows = uploadRows + uploadRows.single().copy { name = "$UPLOAD-other" }
      drained()
      assertThat(uploadReads).isEqualTo(2)
    }

  @Test
  fun `active queues block rollout`() =
    runBlocking<Unit> {
      items =
        listOf(
          workItem {
            name = "workItems/active"
            queue = QUEUE
            state = WorkItem.State.RUNNING
          }
        )
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `unknown queue state blocks rollout`() =
    runBlocking<Unit> {
      items =
        listOf(
          WorkItem.newBuilder()
            .setName("workItems/unknown")
            .setQueue(QUEUE)
            .setStateValue(999)
            .build()
        )
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `nonterminal upload on a later page blocks rollout`() =
    runBlocking<Unit> {
      uploadRows =
        uploadRows +
          uploadRows.single().copy {
            name = "$UPLOAD-next"
            state = RawImpressionUpload.State.ACTIVE
          }
      assertFailsWith<IllegalStateException> { drained() }
      assertThat(uploadReads).isEqualTo(2)
    }

  @Test
  fun `active sibling under terminal upload blocks rollout`() =
    runBlocking<Unit> {
      modelState = RawImpressionUploadModelLine.State.LABELING
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `unfinished healing blocks rollout`() =
    runBlocking<Unit> {
      operations =
        listOf(
          uploadHealingOperation {
            name = "operations/pending"
            state = UploadHealingOperation.State.APPROVAL_REQUIRED
          }
        )
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `query failure does not report drained`() =
    runBlocking<Unit> {
      queryFails = true
      assertFailsWith<StatusException> { drained() }
    }

  @Test
  fun `repeated page tokens fail closed`() =
    runBlocking<Unit> {
      repeatedToken = true
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `one exact generation zero-membership smoke verifies successful coverage`() =
    runBlocking<Unit> { smoke() }

  @Test
  fun `wrong smoke generation fails`() =
    runBlocking<Unit> {
      assertFailsWith<IllegalStateException> {
        checker().requireZeroMembership("dataProviders/dp", DONE, 13, MODEL, 2)
      }
    }

  @Test
  fun `missing smoke shard fails`() =
    runBlocking<Unit> {
      shardRows = shardRows.take(1)
      assertFailsWith<IllegalStateException> { smoke() }
    }

  @Test
  fun `ranking in zero smoke fails`() =
    runBlocking<Unit> {
      ranked = true
      assertFailsWith<IllegalStateException> { smoke() }
    }

  @Test
  fun `duplicated smoke file coverage fails`() =
    runBlocking<Unit> {
      labelFiles = labelFiles + labelFiles.first()
      assertFailsWith<IllegalStateException> { smoke() }
    }

  private fun fixture(name: String) =
    Paths.get(
        System.getenv("TEST_SRCDIR"),
        System.getenv("TEST_WORKSPACE"),
        "src/main/k8s/testing/secretfiles",
        name,
      )
      .toFile()

  private fun invokeMain(extra: List<String>, timeoutSeconds: Int = 20): Pair<Int, String> {
    val process =
      ProcessBuilder(
          listOf(
            Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xmx256m",
            "-cp",
            System.getProperty("java.class.path"),
            "org.wfanet.measurement.edpaggregator.tools.VidLabelingRolloutCheckKt",
          ) + extra
        )
        .redirectErrorStream(true)
        .start()
    try {
      check(process.waitFor(timeoutSeconds.toLong(), TimeUnit.SECONDS)) { "CLI did not terminate" }
      return process.exitValue() to process.inputStream.bufferedReader().readText()
    } finally {
      if (process.isAlive) process.destroyForcibly()
    }
  }

  private fun withTlsCli(block: (List<String>) -> Unit) {
    val tlsServer =
      CommonServer.fromParameters(
          nameForLogging = "VidLabelingRolloutCliTest",
          verboseGrpcLogging = false,
          certs =
            SigningCerts.fromPemFiles(
              fixture("edp_aggregator_tls.pem"),
              fixture("edp_aggregator_tls.key"),
              fixture("edp_aggregator_trusted_certs.pem"),
            ),
          clientAuth = ClientAuth.REQUIRE,
          services = registeredServices.map { it.bindService() },
        )
        .start()
    val target = "localhost:${tlsServer.port}"
    val args =
      listOf(
        "--edpa-public-api-target=$target",
        "--edpa-public-api-cert-host=localhost",
        "--control-plane-target=$target",
        "--control-plane-cert-host=localhost",
        "--tls-cert-file=${fixture("edpa_tee_app_tls.pem")}",
        "--tls-key-file=${fixture("edpa_tee_app_tls.key")}",
        "--cert-collection-file=${fixture("edp_aggregator_root.pem")}",
        "--control-plane-tls-cert-file=${fixture("edpa_tee_app_tls.pem")}",
        "--control-plane-tls-key-file=${fixture("edpa_tee_app_tls.key")}",
        "--control-plane-cert-collection-file=${fixture("edp_aggregator_root.pem")}",
        "--data-provider=dataProviders/dp",
        "--timeout-seconds=5",
      )
    try {
      block(args)
    } finally {
      tlsServer.close()
    }
  }

  @Test
  fun `main rejects missing arguments without success`() {
    val (code, output) = invokeMain(listOf("--mode=DRAINED"))
    assertThat(code).isNotEqualTo(0)
    assertThat(output).doesNotContain("PASS:")
  }

  @Test
  fun `main dispatches both modes and prints success`() {
    withTlsCli { args ->
      val drained = invokeMain(args + listOf("--mode=DRAINED", "--queue=$QUEUE"))
      assertThat(drained.first).isEqualTo(0)
      assertThat(drained.second).contains("PASS: DRAINED")
      val zero =
        invokeMain(
          args +
            listOf(
              "--mode=ZERO_MEMBERSHIP",
              "--done-blob-uri=$DONE",
              "--done-blob-generation=12",
              "--model-line=$MODEL",
              "--expected-shards=2",
            )
        )
      assertThat(zero.first).isEqualTo(0)
      assertThat(zero.second).contains("PASS: ZERO_MEMBERSHIP")
    }
  }

  @Test
  fun `main query failure returns nonzero and never success`() {
    queryFails = true
    withTlsCli { args ->
      val (code, output) = invokeMain(args + listOf("--mode=DRAINED", "--queue=$QUEUE"))
      assertThat(code).isNotEqualTo(0)
      assertThat(output).doesNotContain("PASS:")
    }
  }

  @Test
  fun `main configured deadline cancels a stalled RPC`() {
    stallQuery = true
    withTlsCli { args ->
      val (code, output) =
        invokeMain(
          args.filterNot { it.startsWith("--timeout-seconds=") } +
            listOf("--timeout-seconds=1", "--mode=DRAINED", "--queue=$QUEUE"),
          15,
        )
      assertThat(code).isNotEqualTo(0)
      assertThat(output).doesNotContain("PASS:")
      assertThat(queryCancelled.get()).isTrue()
    }
  }

  @Test
  fun `main rejects invalid deadline without success`() {
    withTlsCli { args ->
      val result =
        invokeMain(
          args.filterNot { it.startsWith("--timeout-seconds=") } +
            listOf("--timeout-seconds=0", "--mode=DRAINED", "--queue=$QUEUE")
        )
      assertThat(result.first).isNotEqualTo(0)
      assertThat(result.second).doesNotContain("PASS:")
    }
  }

  companion object {
    private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/upload"
    private const val DONE = "gs://test/vid-labeling-rollout-smoke/run/done"
    private const val MODEL = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val QUEUE = "queues/vid-labeler-queue"
  }
}
