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
import java.time.Duration
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
  private var workItemReads = 0
  private var modelLineReads = 0
  private var healingReads = 0
  private val workItemRequests = mutableListOf<ListWorkItemsRequest>()
  private val uploadRequests = mutableListOf<ListRawImpressionUploadsRequest>()
  private val modelLineRequests = mutableListOf<ListRawImpressionUploadModelLinesRequest>()
  private val healingRequests = mutableListOf<ListUploadHealingOperationsRequest>()
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
  private var ignoreWorkItemFilter = false
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
            uploadRequests += request
            val states = request.filter.stateInList
            val matching = uploadRows.filter { states.isEmpty() || it.state in states }
            val offset = request.pageToken.toIntOrNull() ?: 0
            rawImpressionUploads += matching.drop(offset).take(1)
            if (repeatedToken) {
              nextPageToken = "1"
            } else if (offset + 1 < matching.size) {
              nextPageToken = (offset + 1).toString()
            }
          }
      }
    )
    register(
      object : WorkItemsGrpcKt.WorkItemsCoroutineImplBase() {
        override suspend fun listWorkItems(request: ListWorkItemsRequest) = listWorkItemsResponse {
          workItemReads++
          workItemRequests += request
          // The real service accepts Queue resource names; WorkItem.queue is the bare ID.
          val queues = request.filter.queueInList.map { it.removePrefix("queues/") }
          val states = request.filter.stateInList
          val matching =
            if (ignoreWorkItemFilter) items
            else
              items.filter {
                (queues.isEmpty() || it.queue in queues) && (states.isEmpty() || it.state in states)
              }
          val offset = request.pageToken.toIntOrNull() ?: 0
          workItems += matching.drop(offset).take(request.pageSize)
          if (offset + request.pageSize < matching.size) {
            nextPageToken = (offset + request.pageSize).toString()
          }
        }
      }
    )
    register(
      object :
        UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase() {
        override suspend fun listUploadHealingOperations(
          request: ListUploadHealingOperationsRequest
        ) = listUploadHealingOperationsResponse {
          healingReads++
          healingRequests += request
          val states = request.filter.stateInList
          uploadHealingOperations += operations.filter { states.isEmpty() || it.state in states }
        }
      }
    )
    register(
      object :
        RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploadModelLines(
          request: ListRawImpressionUploadModelLinesRequest
        ) = listRawImpressionUploadModelLinesResponse {
          modelLineReads++
          modelLineRequests += request
          val states = request.filter.stateInList
          if (states.isEmpty() || modelState in states) {
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              cmmsModelLine = MODEL
              state = modelState
              poolAssignmentComplete = true
            }
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

  private fun checker(
    leaseQuiescence: Duration = Duration.ZERO,
    sleep: suspend (Duration) -> Unit = {},
  ) =
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
      leaseQuiescence = leaseQuiescence,
      sleep = sleep,
    )

  private suspend fun drained() = checker().requireDrained(setOf("dataProviders/dp"), setOf(QUEUE))

  private suspend fun smoke() =
    checker().requireZeroMembership("dataProviders/dp", DONE, 12, MODEL, 2)

  @Test
  fun `maintenance requires a lease quiescence period`() =
    runBlocking<Unit> {
      assertFailsWith<IllegalStateException> {
        checker().requireMaintenanceDrained(setOf("dataProviders/dp"), setOf(QUEUE))
      }
    }

  @Test
  fun `maintenance waits for abandoned leases to expire between two drain proofs`() =
    runBlocking<Unit> {
      val sleeps = mutableListOf<Duration>()
      checker(LEASE_QUIESCENCE) {
          assertThat(uploadReads).isEqualTo(1)
          sleeps += it
        }
        .requireMaintenanceDrained(setOf("dataProviders/dp"), setOf(QUEUE))
      assertThat(sleeps).containsExactly(LEASE_QUIESCENCE)
      assertThat(uploadReads).isEqualTo(2)
      assertThat(workItemReads).isEqualTo(2)
    }

  @Test
  fun `work started during the lease wait fails closed`() =
    runBlocking<Unit> {
      assertFailsWith<RolloutWorkInProgressException> {
        checker(LEASE_QUIESCENCE) {
            items =
              listOf(
                workItem {
                  name = "workItems/late"
                  queue = QUEUE_ID
                  state = WorkItem.State.QUEUED
                }
              )
          }
          .requireMaintenanceDrained(setOf("dataProviders/dp"), setOf(QUEUE))
      }
    }

  @Test
  fun `large terminal history is bounded by server-side filters`() =
    runBlocking<Unit> {
      items =
        (0 until 10_000).map { index ->
          workItem {
            name = "workItems/old-$index"
            queue = if (index % 2 == 0) QUEUE_ID else "other-queue"
            state = if (index % 3 == 0) WorkItem.State.FAILED else WorkItem.State.SUCCEEDED
          }
        }
      uploadRows =
        (0 until 5_000).map { index -> uploadRows.first().copy { name = "$UPLOAD-$index" } }
      operations =
        (0 until 1_000).map { index ->
          uploadHealingOperation {
            name = "operations/old-$index"
            state = UploadHealingOperation.State.COMPLETE
          }
        }

      drained()

      assertThat(workItemReads).isEqualTo(1)
      assertThat(uploadReads).isEqualTo(1)
      assertThat(modelLineReads).isEqualTo(1)
      assertThat(healingReads).isEqualTo(1)
      val workItemFilter = workItemRequests.single().filter
      assertThat(workItemFilter.queueInList).containsExactly(QUEUE)
      assertThat(workItemFilter.stateInList)
        .containsExactly(WorkItem.State.QUEUED, WorkItem.State.RUNNING)
      assertThat(uploadRequests.single().filter.stateInList)
        .containsExactly(
          RawImpressionUpload.State.CREATED,
          RawImpressionUpload.State.ACTIVE,
          RawImpressionUpload.State.CORRECTION_REQUIRED,
        )
      assertThat(modelLineRequests.single().parent)
        .isEqualTo("dataProviders/dp/rawImpressionUploads/-")
      assertThat(modelLineRequests.single().filter.stateInList)
        .containsNoneOf(
          RawImpressionUploadModelLine.State.COMPLETED,
          RawImpressionUploadModelLine.State.FAILED,
        )
      assertThat(healingRequests.single().filter.stateInList)
        .doesNotContain(UploadHealingOperation.State.COMPLETE)
    }

  @Test
  fun `deployment queue names filter by name and match WorkItem queue IDs`() =
    runBlocking<Unit> {
      for (ignoreFilter in listOf(false, true)) {
        ignoreWorkItemFilter = ignoreFilter
        workItemRequests.clear()
        items =
          listOf(
            workItem {
              name = "workItems/active"
              queue = QUEUE_ID
              state = WorkItem.State.RUNNING
            }
          )
        assertFailsWith<RolloutWorkInProgressException> { drained() }
        assertThat(workItemRequests.single().filter.queueInList).containsExactly(QUEUE)
      }
    }

  @Test
  fun `queue without deployment prefix is rejected`() =
    runBlocking<Unit> {
      assertFailsWith<IllegalArgumentException> {
        checker().requireDrained(setOf("dataProviders/dp"), setOf(QUEUE_ID))
      }
    }

  @Test
  fun `server that ignores the WorkItem filter still drains correctly`() =
    runBlocking<Unit> {
      ignoreWorkItemFilter = true
      items =
        listOf(
          workItem {
            name = "workItems/done"
            queue = QUEUE_ID
            state = WorkItem.State.SUCCEEDED
          },
          workItem {
            name = "workItems/other"
            queue = "other-queue"
            state = WorkItem.State.RUNNING
          },
        )
      drained()
      items =
        items +
          workItem {
            name = "workItems/active"
            queue = QUEUE_ID
            state = WorkItem.State.RUNNING
          }
      assertFailsWith<RolloutWorkInProgressException> { drained() }
    }

  @Test
  fun `drained evidence is read completely without mutation`() =
    runBlocking<Unit> {
      uploadRows = uploadRows + uploadRows.single().copy { name = "$UPLOAD-other" }
      drained()
      assertThat(uploadReads).isEqualTo(1)
      assertThat(uploadRows.map { it.state })
        .containsNoneOf(RawImpressionUpload.State.CREATED, RawImpressionUpload.State.ACTIVE)
    }

  @Test
  fun `active queues block rollout`() =
    runBlocking<Unit> {
      items =
        listOf(
          workItem {
            name = "workItems/active"
            queue = QUEUE_ID
            state = WorkItem.State.RUNNING
          }
        )
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `unknown queue state blocks rollout`() =
    runBlocking<Unit> {
      // A newer server can return a state this client does not know.
      ignoreWorkItemFilter = true
      items =
        listOf(
          WorkItem.newBuilder()
            .setName("workItems/unknown")
            .setQueue(QUEUE_ID)
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
      assertFailsWith<RolloutWorkInProgressException> { drained() }
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
    private const val QUEUE_ID = "vid-labeler-queue"
    private val LEASE_QUIESCENCE: Duration = Duration.ofMinutes(11)
  }
}
