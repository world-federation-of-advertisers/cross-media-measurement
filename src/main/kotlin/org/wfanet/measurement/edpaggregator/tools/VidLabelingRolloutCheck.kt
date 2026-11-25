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

import com.google.protobuf.ListValue
import com.google.protobuf.Struct
import com.google.protobuf.TextFormat
import com.google.protobuf.Value
import com.google.protobuf.util.JsonFormat
import java.io.File
import java.time.Duration
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequestKt
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
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsRequestKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.listWorkItemsRequest
import picocli.CommandLine

/** Known active work may finish naturally while the maintenance boundary remains held. */
class RolloutWorkInProgressException(message: String) : IllegalStateException(message)

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
  /** Maximum synchronization-lease lifetime plus margin. Required by maintenance checks. */
  private val leaseQuiescence: Duration = Duration.ZERO,
  private val sleep: suspend (Duration) -> Unit = { delay(it.toMillis()) },
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

  /**
   * Proves no outstanding VID work or unfinished upload/healing state in the supplied scope.
   *
   * Every collection is read with a server-side state filter, so the cost is bounded by the
   * outstanding work rather than by retained history. Rows outside the filter are still evaluated,
   * so a server that predates a filter yields a complete (unbounded) read rather than a false pass.
   */
  suspend fun requireDrained(providers: Set<String>, queues: Set<String>) {
    require(providers.isNotEmpty() && queues.isNotEmpty()) { "Deployment scope must be explicit" }
    // Deployment scope and the ListWorkItems filter name queues as `queues/{queue}`;
    // WorkItem.queue carries the bare queue resource ID.
    val queueIds: Set<String> =
      queues.mapTo(sortedSetOf()) { queue ->
        val queueId = queue.removePrefix(QUEUE_PREFIX)
        require(queue.startsWith(QUEUE_PREFIX) && queueId.isNotEmpty() && '/' !in queueId) {
          "Invalid queue: $queue"
        }
        queueId
      }
    val items = pages { token ->
      val response =
        workItems.listWorkItems(
          listWorkItemsRequest {
            pageSize = 100
            pageToken = token
            filter =
              ListWorkItemsRequestKt.filter {
                queueIn += queueIds.map { QUEUE_PREFIX + it }
                stateIn += UNFINISHED_WORK_ITEM_STATES
              }
          }
        )
      response.workItemsList to response.nextPageToken
    }
    for (item in items) {
      check(item.queue.isNotEmpty()) { "WorkItem queue missing" }
      if (item.queue !in queueIds) continue
      if (item.state == WorkItem.State.SUCCEEDED || item.state == WorkItem.State.FAILED) continue
      if (item.state == WorkItem.State.QUEUED || item.state == WorkItem.State.RUNNING) {
        throw RolloutWorkInProgressException("VID queue is not drained: ${item.name}")
      }
      error("Unknown WorkItem state: ${item.name} (${item.state})")
    }
    for (provider in providers) {
      require(
        provider.startsWith("dataProviders/") &&
          provider.count { it == '/' } == 1 &&
          provider.substringAfter('/').isNotEmpty()
      )
      val unfinishedUploads = pages { token ->
        val response =
          uploads.listRawImpressionUploads(
            listRawImpressionUploadsRequest {
              parent = provider
              pageSize = 1000
              pageToken = token
              filter =
                ListRawImpressionUploadsRequestKt.filter { stateIn += UNFINISHED_UPLOAD_STATES }
            }
          )
        response.rawImpressionUploadsList to response.nextPageToken
      }
      for (upload in unfinishedUploads) {
        if (upload.state in TERMINAL_UPLOAD_STATES) continue
        if (
          upload.state == RawImpressionUpload.State.CREATED ||
            upload.state == RawImpressionUpload.State.ACTIVE
        ) {
          throw RolloutWorkInProgressException("Nonterminal upload: ${upload.name}")
        }
        error("Upload requires intervention or has unknown state: ${upload.name} (${upload.state})")
      }
      // One cross-upload query (AIP-159) replaces listing the children of every upload.
      val unfinishedModelLines = pages { token ->
        val response =
          modelLines.listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              parent = "$provider/rawImpressionUploads/-"
              pageSize = 1000
              pageToken = token
              filter =
                ListRawImpressionUploadModelLinesRequestKt.filter {
                  stateIn += UNFINISHED_MODEL_LINE_STATES
                }
            }
          )
        response.rawImpressionUploadModelLinesList to response.nextPageToken
      }
      val activeModelLine =
        unfinishedModelLines.firstOrNull {
          it.state != RawImpressionUploadModelLine.State.COMPLETED &&
            it.state != RawImpressionUploadModelLine.State.FAILED
        }
      check(activeModelLine == null) {
        "Nonterminal model line under terminal upload: ${activeModelLine?.name}"
      }
      val unfinishedOperations = pages { token ->
        val response =
          healing.listUploadHealingOperations(
            listUploadHealingOperationsRequest {
              parent = provider
              pageSize = 100
              pageToken = token
              filter =
                ListUploadHealingOperationsRequestKt.filter { stateIn += UNFINISHED_HEALING_STATES }
            }
          )
        response.uploadHealingOperationsList to response.nextPageToken
      }
      val activeOperation =
        unfinishedOperations.firstOrNull { it.state != UploadHealingOperation.State.COMPLETE }
      check(activeOperation == null) {
        "Unfinished healing: ${activeOperation?.name} (${activeOperation?.state})"
      }
    }
  }

  /**
   * Complete maintenance preflight. Intake/backfill control must remain paused for the entire
   * caller-owned maintenance window, so no new work can start. After the first drain proof, this
   * waits one maximum synchronization-lease lifetime plus margin. No drained worker can renew a
   * lease, so every abandoned lease has expired before the same API checks are repeated.
   */
  suspend fun requireMaintenanceDrained(providers: Set<String>, queues: Set<String>) {
    check(!leaseQuiescence.isNegative && !leaseQuiescence.isZero) {
      "Synchronization-lease quiescence period is required"
    }
    requireDrained(providers, queues)
    sleep(leaseQuiescence)
    requireDrained(providers, queues)
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

  companion object {
    private const val QUEUE_PREFIX = "queues/"
    private val UNFINISHED_WORK_ITEM_STATES =
      WorkItem.State.values().filter {
        it != WorkItem.State.STATE_UNSPECIFIED &&
          it != WorkItem.State.UNRECOGNIZED &&
          it != WorkItem.State.SUCCEEDED &&
          it != WorkItem.State.FAILED
      }
    private val TERMINAL_UPLOAD_STATES =
      setOf(
        RawImpressionUpload.State.COMPLETED,
        RawImpressionUpload.State.FAILED,
        RawImpressionUpload.State.REMOVED_WITHOUT_REPLACEMENT,
      )
    private val UNFINISHED_UPLOAD_STATES =
      RawImpressionUpload.State.values().filter {
        it != RawImpressionUpload.State.STATE_UNSPECIFIED &&
          it != RawImpressionUpload.State.UNRECOGNIZED &&
          it !in TERMINAL_UPLOAD_STATES
      }
    private val UNFINISHED_MODEL_LINE_STATES =
      RawImpressionUploadModelLine.State.values().filter {
        it != RawImpressionUploadModelLine.State.STATE_UNSPECIFIED &&
          it != RawImpressionUploadModelLine.State.UNRECOGNIZED &&
          it != RawImpressionUploadModelLine.State.COMPLETED &&
          it != RawImpressionUploadModelLine.State.FAILED
      }
    private val UNFINISHED_HEALING_STATES =
      UploadHealingOperation.State.values().filter {
        it != UploadHealingOperation.State.STATE_UNSPECIFIED &&
          it != UploadHealingOperation.State.UNRECOGNIZED &&
          it != UploadHealingOperation.State.COMPLETE
      }
  }
}

@CommandLine.Command(name = "vid-labeling-rollout-check", mixinStandardHelpOptions = true)
class VidLabelingRolloutCheck : Runnable {
  enum class Mode {
    DRAINED,
    MAINTENANCE,
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
  @CommandLine.Option(
    names = ["--max-sync-lease-lifetime"],
    description = ["Maximum configured DataAvailabilitySync lease lifetime, for example 10m."],
    defaultValue = "10m",
  )
  private lateinit var maxSyncLeaseLifetime: Duration
  @CommandLine.Option(
    names = ["--sync-lease-margin"],
    description = ["Clock and renewal margin added to the lease lifetime, for example 1m."],
    defaultValue = "1m",
  )
  private lateinit var syncLeaseMargin: Duration
  @CommandLine.Option(names = ["--timeout-seconds"], defaultValue = "1800")
  private var timeoutSeconds: Long = 1800

  override fun run() = runBlocking {
    require(timeoutSeconds > 0)
    require(!maxSyncLeaseLifetime.isNegative && !maxSyncLeaseLifetime.isZero)
    require(!syncLeaseMargin.isNegative)
    val leaseQuiescence = maxSyncLeaseLifetime.plus(syncLeaseMargin)
    if (mode == Mode.MAINTENANCE) {
      require(timeoutSeconds > leaseQuiescence.seconds) {
        "Maintenance deadline must exceed the synchronization-lease quiescence period"
      }
    }
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
          leaseQuiescence = leaseQuiescence,
        )
      withTimeout(timeoutSeconds * 1000) {
        when (mode) {
          Mode.DRAINED -> checker.requireDrained(providers.toSet(), queues.toSet())
          Mode.MAINTENANCE -> {
            while (true) {
              try {
                checker.requireMaintenanceDrained(providers.toSet(), queues.toSet())
                break
              } catch (e: RolloutWorkInProgressException) {
                delay(1000L)
              }
            }
          }
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
      if (mode == Mode.MAINTENANCE) {
        val summary =
          Struct.newBuilder()
            .putFields("status", Value.newBuilder().setStringValue("PASS").build())
            .putFields("mode", Value.newBuilder().setStringValue("MAINTENANCE").build())
        for ((name, values) in listOf("providers" to providers, "queues" to queues)) {
          val list = ListValue.newBuilder()
          values.distinct().sorted().forEach {
            list.addValues(Value.newBuilder().setStringValue(it).build())
          }
          summary.putFields(name, Value.newBuilder().setListValue(list).build())
        }
        println(JsonFormat.printer().omittingInsignificantWhitespace().print(summary.build()))
      } else {
        println("PASS: ${mode.name}")
      }
    } finally {
      edpa.shutdownNow()
      control.shutdownNow()
    }
  }
}

fun main(args: Array<String>) = commandLineMain(VidLabelingRolloutCheck(), args)

/** Read the deployment's complete provider scope using its actual protobuf config parser. */
@CommandLine.Command(name = "vid-labeling-rollout-scope", mixinStandardHelpOptions = true)
class VidLabelingRolloutScope : Runnable {
  @CommandLine.Option(names = ["--vid-labeling-config"], required = true)
  private lateinit var configFile: File

  override fun run() {
    val builder = VidLabelingConfigs.newBuilder()
    configFile.reader().use { TextFormat.getParser().merge(it, builder) }
    val configs = builder.build().configsList
    val providers = configs.map { it.dataProvider }
    require(providers.isNotEmpty() && providers.distinct().size == providers.size) {
      "VID provider scope is missing or duplicated"
    }
    require(
      providers.all {
        it.startsWith("dataProviders/") &&
          it.count { c -> c == '/' } == 1 &&
          it.substringAfter('/').isNotEmpty()
      }
    ) {
      "Invalid configured provider"
    }
    require(configs.all { it.maxFileBatchSizeBytes > 0 }) { "Packing contract is missing" }
    val list = ListValue.newBuilder()
    providers.sorted().forEach { list.addValues(Value.newBuilder().setStringValue(it)) }
    val summary =
      Struct.newBuilder()
        .putFields("providers", Value.newBuilder().setListValue(list).build())
        .build()
    println(JsonFormat.printer().omittingInsignificantWhitespace().print(summary))
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(VidLabelingRolloutScope(), args)
  }
}
