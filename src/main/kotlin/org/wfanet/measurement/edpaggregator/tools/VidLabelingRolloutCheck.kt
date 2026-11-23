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

import java.io.File
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.listWorkItemsRequest
import picocli.CommandLine

/** Existing-API, read-only rollout checks. All query failures propagate and fail the check. */
class VidLabelingRolloutChecker(
  private val uploads: RawImpressionUploadServiceCoroutineStub,
  private val modelLines: RawImpressionUploadModelLineServiceCoroutineStub,
  private val pools: PoolAssignmentJobServiceCoroutineStub,
  private val rankers: RankerJobServiceCoroutineStub,
  private val labels: VidLabelingJobServiceCoroutineStub,
  private val files: RawImpressionUploadFileServiceCoroutineStub,
  private val healing: UploadHealingOperationServiceCoroutineStub,
  private val workItems: WorkItemsCoroutineStub,
) {
  private suspend fun <T> pages(fetch: suspend (String) -> Pair<List<T>, String>): List<T> {
    val result = mutableListOf<T>()
    val seen = mutableSetOf<String>()
    var token = ""
    do {
      check(seen.add(token)) { "Repeated page token; complete evidence unavailable" }
      val (values, next) = fetch(token)
      result.addAll(values)
      token = next
    } while (token.isNotEmpty())
    return result
  }

  private suspend fun allUploads(provider: String) = pages { token ->
    val response =
      uploads.listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = provider
          pageSize = 1000
          pageToken = token
        }
      )
    response.rawImpressionUploadsList to response.nextPageToken
  }

  /** Proves no outstanding VID work or unfinished upload/healing state in the supplied scope. */
  suspend fun requireDrained(providers: Set<String>, queues: Set<String>) {
    require(providers.isNotEmpty() && queues.isNotEmpty()) { "Deployment scope must be explicit" }
    val items = pages { token ->
      val response =
        workItems.listWorkItems(
          listWorkItemsRequest {
            pageSize = 100
            pageToken = token
          }
        )
      response.workItemsList to response.nextPageToken
    }
    for (item in items) {
      check(item.queue.isNotEmpty()) { "WorkItem queue missing" }
      if (item.queue !in queues) continue
      check(item.state == WorkItem.State.SUCCEEDED || item.state == WorkItem.State.FAILED) {
        "VID queue is not drained: \${item.name} (\${item.state})"
      }
    }
    for (provider in providers) {
      require(provider.startsWith("dataProviders/") && provider.substringAfter('/').isNotEmpty())
      for (upload in allUploads(provider)) {
        check(
          upload.state == RawImpressionUpload.State.COMPLETED ||
            upload.state == RawImpressionUpload.State.FAILED ||
            upload.state == RawImpressionUpload.State.REMOVED_WITHOUT_REPLACEMENT
        ) {
          "Nonterminal upload: \${upload.name} (\${upload.state})"
        }
        check(!upload.processingDeferred) { "Deferred processing: \${upload.name}" }
        check(upload.registrationComplete) { "Incomplete registration: ${upload.name}" }
        val rows = pages { token ->
          val response =
            modelLines.listRawImpressionUploadModelLines(
              listRawImpressionUploadModelLinesRequest {
                parent = upload.name
                pageSize = 1000
                pageToken = token
              }
            )
          response.rawImpressionUploadModelLinesList to response.nextPageToken
        }
        check(
          rows.all {
            it.state == RawImpressionUploadModelLine.State.COMPLETED ||
              it.state == RawImpressionUploadModelLine.State.FAILED
          }
        ) {
          "Nonterminal model line under terminal upload: ${upload.name}"
        }
      }
      val operations = pages { token ->
        val response =
          healing.listUploadHealingOperations(
            listUploadHealingOperationsRequest {
              parent = provider
              pageSize = 100
              pageToken = token
            }
          )
        response.uploadHealingOperationsList to response.nextPageToken
      }
      for (operation in operations) {
        check(operation.state == UploadHealingOperation.State.COMPLETE) {
          "Unfinished healing: \${operation.name} (\${operation.state})"
        }
      }
    }
  }

  /** Verifies one real, exact-generation smoke upload; does not create or modify test data. */
  suspend fun requireZeroMembership(
    provider: String,
    doneUri: String,
    generation: Long,
    modelLine: String,
    expectedShards: Int,
  ) {
    require(generation > 0 && expectedShards > 0 && doneUri.startsWith("gs://"))
    val matching =
      allUploads(provider).filter {
        it.doneBlobUri == doneUri && it.doneBlobGeneration == generation
      }
    check(matching.size == 1) { "Expected exactly one exact-generation smoke upload" }
    val upload = matching.single()
    check(upload.registrationComplete && !upload.processingDeferred)
    val rows =
      pages { token ->
          val response =
            modelLines.listRawImpressionUploadModelLines(
              listRawImpressionUploadModelLinesRequest {
                parent = upload.name
                pageSize = 1000
                pageToken = token
              }
            )
          response.rawImpressionUploadModelLinesList to response.nextPageToken
        }
        .filter { it.cmmsModelLine == modelLine }
    check(rows.size == 1) { "Smoke model line missing or ambiguous" }
    val row = rows.single()
    check(
      row.state == RawImpressionUploadModelLine.State.COMPLETED &&
        row.poolAssignmentComplete &&
        row.poolOffsetsCount == 0 &&
        !row.hasMaxEventDate() &&
        !row.hasEncryptedMergedDek()
    ) {
      "Smoke has not completed verified empty Phase 0 and labeling"
    }
    val shardJobs = pages { token ->
      val response =
        pools.listPoolAssignmentJobs(
          listPoolAssignmentJobsRequest {
            parent = upload.name
            pageSize = 1000
            pageToken = token
            filter = ListPoolAssignmentJobsRequestKt.filter { cmmsModelLine = modelLine }
          }
        )
      response.poolAssignmentJobsList to response.nextPageToken
    }
    check(
      shardJobs.size == expectedShards &&
        shardJobs.map { it.shardIndex }.toSet() == (0 until expectedShards).toSet() &&
        shardJobs.all { it.state == PoolAssignmentJob.State.SUCCEEDED }
    ) {
      "Smoke shard evidence incomplete"
    }
    val rankJobs = pages { token ->
      val response =
        rankers.listRankerJobs(
          listRankerJobsRequest {
            parent = upload.name
            pageSize = 1000
            pageToken = token
            filter = ListRankerJobsRequestKt.filter { cmmsModelLine = modelLine }
          }
        )
      response.rankerJobsList to response.nextPageToken
    }
    check(rankJobs.isEmpty()) { "Zero-membership smoke created ranking work" }
    val labelJobs = pages { token ->
      val response =
        labels.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            parent = upload.name
            pageSize = 1
            pageToken = token
            filter = ListVidLabelingJobsRequestKt.filter { cmmsModelLine = modelLine }
          }
        )
      response.vidLabelingJobsList to response.nextPageToken
    }
    check(labelJobs.isNotEmpty() && labelJobs.all { it.state == VidLabelingJob.State.SUCCEEDED })
    val inputFiles = pages { token ->
      val response =
        files.listRawImpressionUploadFiles(
          listRawImpressionUploadFilesRequest {
            parent = upload.name
            pageSize = 1000
            pageToken = token
          }
        )
      response.rawImpressionUploadFilesList to response.nextPageToken
    }
    check(inputFiles.isNotEmpty())
    check(
      labelJobs.flatMap { it.rawImpressionUploadFilesList }.sorted() ==
        inputFiles.map { it.name }.sorted()
    ) {
      "Smoke file coverage is incomplete or duplicated"
    }
  }
}

@CommandLine.Command(name = "vid-labeling-rollout-check", mixinStandardHelpOptions = true)
class VidLabelingRolloutCheck : Runnable {
  enum class Mode {
    DRAINED,
    ZERO_MEMBERSHIP,
  }

  @CommandLine.Mixin private lateinit var tls: TlsFlags
  @CommandLine.Option(names = ["--edpa-public-api-target"], required = true)
  private lateinit var edpaTarget: String
  @CommandLine.Option(names = ["--edpa-public-api-cert-host"])
  private var edpaCertHost: String? = null
  @CommandLine.Option(names = ["--control-plane-target"], required = true)
  private lateinit var controlTarget: String
  @CommandLine.Option(names = ["--control-plane-cert-host"])
  private var controlCertHost: String? = null
  @CommandLine.Option(names = ["--control-plane-tls-cert-file"], required = true)
  private lateinit var controlCert: File
  @CommandLine.Option(names = ["--control-plane-tls-key-file"], required = true)
  private lateinit var controlKey: File
  @CommandLine.Option(names = ["--control-plane-cert-collection-file"], required = true)
  private lateinit var controlRoots: File
  @CommandLine.Option(names = ["--mode"], required = true) private lateinit var mode: Mode
  @CommandLine.Option(names = ["--data-provider"], required = true)
  private var providers: List<String> = emptyList()
  @CommandLine.Option(names = ["--queue"]) private var queues: List<String> = emptyList()
  @CommandLine.Option(names = ["--done-blob-uri"]) private var doneUri: String = ""
  @CommandLine.Option(names = ["--done-blob-generation"]) private var generation: Long = 0
  @CommandLine.Option(names = ["--model-line"]) private var modelLine: String = ""
  @CommandLine.Option(names = ["--expected-shards"]) private var expectedShards: Int = 0
  @CommandLine.Option(names = ["--timeout-seconds"], defaultValue = "600")
  private var timeoutSeconds: Long = 600

  override fun run() = runBlocking {
    require(timeoutSeconds > 0)
    val certs = SigningCerts.fromPemFiles(tls.certFile, tls.privateKeyFile, tls.certCollectionFile)
    val controlCerts = SigningCerts.fromPemFiles(controlCert, controlKey, controlRoots)
    val edpa = buildMutualTlsChannel(edpaTarget, certs, edpaCertHost)
    val control = buildMutualTlsChannel(controlTarget, controlCerts, controlCertHost)
    try {
      val checker =
        VidLabelingRolloutChecker(
          RawImpressionUploadServiceCoroutineStub(edpa),
          RawImpressionUploadModelLineServiceCoroutineStub(edpa),
          PoolAssignmentJobServiceCoroutineStub(edpa),
          RankerJobServiceCoroutineStub(edpa),
          VidLabelingJobServiceCoroutineStub(edpa),
          RawImpressionUploadFileServiceCoroutineStub(edpa),
          UploadHealingOperationServiceCoroutineStub(edpa),
          WorkItemsCoroutineStub(control),
        )
      withTimeout(timeoutSeconds * 1000) {
        when (mode) {
          Mode.DRAINED -> checker.requireDrained(providers.toSet(), queues.toSet())
          Mode.ZERO_MEMBERSHIP -> {
            require(providers.size == 1 && modelLine.isNotEmpty())
            checker.requireZeroMembership(
              providers.single(),
              doneUri,
              generation,
              modelLine,
              expectedShards,
            )
          }
        }
      }
      println("PASS: \${mode.name}")
    } finally {
      edpa.shutdownNow()
      control.shutdownNow()
    }
  }
}

fun main(args: Array<String>) = commandLineMain(VidLabelingRolloutCheck(), args)
