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

import com.google.protobuf.ListValue
import com.google.protobuf.Struct
import com.google.protobuf.TextFormat
import com.google.protobuf.TypeRegistry
import com.google.protobuf.Value
import com.google.protobuf.util.JsonFormat
import io.grpc.Status
import io.grpc.StatusException
import java.io.File
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.edpaggregator.DataWatcherConfigTypes
import org.wfanet.measurement.common.edpaggregator.VidLabelingDispatchWorkItems
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.config.securecomputation.WatchedPath
import org.wfanet.measurement.edpaggregator.VidLabelingRpcDurationConverter
import org.wfanet.measurement.edpaggregator.v1alpha.ApproveUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatcherParams
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.retryWorkItemRequest
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcherConfigValidator
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcherWorkItemIds
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters

/** Connection options for operator commands using the control plane. */
abstract class DispatchWorkItemCommand : Runnable {
  @Mixin protected lateinit var controlPlaneTlsFlags: ControlPlaneTlsFlags

  @Option(names = ["--control-plane-api-target"], required = true)
  protected lateinit var controlPlaneApiTarget: String

  @Option(names = ["--control-plane-api-cert-host"])
  protected var controlPlaneApiCertHost: String? = null

  @Option(
    names = ["--rpc-timeout"],
    defaultValue = "30s",
    converter = [VidLabelingRpcDurationConverter::class],
  )
  protected lateinit var rpcTimeout: Duration

  protected fun runWithClient(block: suspend (WorkItemsCoroutineStub) -> Unit) {
    require(!rpcTimeout.isZero && !rpcTimeout.isNegative) { "RPC timeout must be positive" }
    val channel =
      buildMutualTlsChannel(
        controlPlaneApiTarget,
        controlPlaneTlsFlags.signingCerts,
        controlPlaneApiCertHost,
      )
    try {
      runBlocking {
        block(
          WorkItemsCoroutineStub(channel)
            .withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS)
        )
      }
    } finally {
      channel.shutdown()
      channel.awaitTermination(EdpaApiCommand.SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }
}

/** Inspects a dispatch execution and its original source. */
@Command(
  name = "get-dispatch-work-item",
  mixinStandardHelpOptions = true,
  description = ["Inspects a VID dispatch WorkItem, including its execution generation and source."],
)
class GetDispatchWorkItemCommand : DispatchWorkItemCommand() {
  @Parameters(index = "0", description = ["WorkItem resource name."])
  private lateinit var name: String

  override fun run() {
    runWithClient { items ->
      val item =
        items.getWorkItem(getWorkItemRequest { this.name = this@GetDispatchWorkItemCommand.name })
      check(item.name == name) { "WorkItem response does not match the requested name" }
      require(item.queue == VidLabelingDispatchWorkItems.QUEUE) { "Not a VID dispatch WorkItem" }
      println("name: ${item.name}\nstate: ${item.state}\nexecution_generation: ${item.generation}")
      val frame =
        runCatching { item.workItemParams.unpack(WorkItem.WorkItemParams::class.java) }.getOrNull()
      val app =
        frame?.let {
          runCatching { it.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java) }
            .getOrNull()
        }
      val source = if (app?.hasRecovery() == true) app.recovery.source else frame?.dataPathParams
      if (source != null) println("source:\n$source")
      if (app != null) println("application_parameters:\n$app")
      else println("work_item_params:\n${item.workItemParams}")
    }
  }
}

/** Retries only the failed execution the operator inspected. */
@Command(
  name = "retry-dispatch-work-item",
  mixinStandardHelpOptions = true,
  description =
    ["Retries a failed VID dispatch WorkItem at the explicitly inspected execution generation."],
)
class RetryDispatchWorkItemCommand : DispatchWorkItemCommand() {
  @Parameters(index = "0", description = ["WorkItem resource name."])
  private lateinit var name: String

  @Option(names = ["--expected-work-item-generation"], required = true)
  private var expectedGeneration: Long = 0L

  override fun run() {
    require(expectedGeneration > 0L) { "Expected execution generation must be positive" }
    runWithClient { items ->
      val item =
        items.getWorkItem(getWorkItemRequest { this.name = this@RetryDispatchWorkItemCommand.name })
      check(item.name == name) { "WorkItem response does not match the requested name" }
      check(item.queue == VidLabelingDispatchWorkItems.QUEUE) { "Not a VID dispatch WorkItem" }
      check(item.generation == expectedGeneration) {
        "Execution generation changed: inspected $expectedGeneration, current ${item.generation}"
      }
      check(item.state == WorkItem.State.FAILED) {
        "Retry requires FAILED; current state is ${item.state}. RUNNING work must be inspected separately."
      }
      val retried =
        items.retryWorkItem(
          retryWorkItemRequest {
            this.name = this@RetryDispatchWorkItemCommand.name
            expectedWorkItemGeneration = expectedGeneration
          }
        )
      println(
        "name: ${retried.name}\nstate: ${retried.state}\nexecution_generation: ${retried.generation}"
      )
    }
  }
}

/**
 * Reads only potentially unfinished registration and healing pages without advancing either
 * workflow.
 *
 * Registration can only be unfinished while an upload is CREATED; legacy non-CREATED rows keep
 * `registration_complete` false but are registered. Every healing state other than COMPLETE and
 * APPROVAL_REQUIRED is listed, so a newly added state fails closed.
 */
@Command(
  name = "preflight-dispatch-cutover",
  mixinStandardHelpOptions = true,
  description =
    ["Fails unless every selected provider has complete registrations and no active healing."],
)
class PreflightDispatchCutoverCommand : EdpaApiCommand() {
  @Option(names = ["--data-provider"], split = ",", required = true)
  private lateinit var dataProviders: List<String>

  override fun run() {
    require(
      dataProviders.isNotEmpty() && dataProviders.all { DataProviderKey.fromName(it) != null }
    ) {
      "Supply every configured VID-labeling DataProvider"
    }
    val channel = buildEdpaChannel()
    try {
      runBlocking {
        val uploads = RawImpressionUploadServiceCoroutineStub(channel)
        val operations = UploadHealingOperationServiceCoroutineStub(channel)
        for (provider in dataProviders.distinct()) {
          var pageToken = ""
          val uploadTokens = mutableSetOf<String>()
          do {
            check(uploadTokens.add(pageToken)) { "Repeated registration page token" }
            val response =
              uploads
                .withDeadlineAfter(30L, TimeUnit.SECONDS)
                .listRawImpressionUploads(
                  listRawImpressionUploadsRequest {
                    parent = provider
                    pageSize = 1000
                    this.pageToken = pageToken
                    filter =
                      ListRawImpressionUploadsRequestKt.filter {
                        stateIn += RawImpressionUpload.State.CREATED
                      }
                  }
                )
            check(response.rawImpressionUploadsList.all { it.registrationComplete }) {
              "Incomplete upload registration for $provider"
            }
            pageToken = response.nextPageToken
            if (pageToken.isNotEmpty()) delay(250L)
          } while (pageToken.isNotEmpty())
          pageToken = ""
          val operationTokens = mutableSetOf<String>()
          do {
            check(operationTokens.add(pageToken)) { "Repeated healing page token" }
            val response =
              operations
                .withDeadlineAfter(30L, TimeUnit.SECONDS)
                .listUploadHealingOperations(
                  listUploadHealingOperationsRequest {
                    parent = provider
                    pageSize = 1000
                    this.pageToken = pageToken
                    filter =
                      ListUploadHealingOperationsRequestKt.filter { stateIn += ACTIVE_HEALING }
                  }
                )
            check(response.uploadHealingOperationsList.all { it.state in INACTIVE_HEALING }) {
              "Active or unresolved healing operation for $provider"
            }
            pageToken = response.nextPageToken
            if (pageToken.isNotEmpty()) delay(250L)
          } while (pageToken.isNotEmpty())
        }
        println(
          "PASS: complete registration and inactive healing inventories for ${dataProviders.distinct().joinToString()}"
        )
      }
    } finally {
      channel.shutdown()
      channel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }

  companion object {
    private val INACTIVE_HEALING =
      setOf(UploadHealingOperation.State.COMPLETE, UploadHealingOperation.State.APPROVAL_REQUIRED)
    private val ACTIVE_HEALING =
      UploadHealingOperation.State.values().filter {
        it != UploadHealingOperation.State.STATE_UNSPECIFIED &&
          it != UploadHealingOperation.State.UNRECOGNIZED &&
          it !in INACTIVE_HEALING
      }
  }
}

/** Exercises authenticated queued ordinary intake and approved recovery on an isolated provider. */
@Command(
  name = "smoke-dispatch-cutover",
  mixinStandardHelpOptions = true,
  description = ["Runs one isolated ordinary/recovery queued-dispatch smoke test."],
)
class SmokeDispatchCutoverCommand : EdpaApiCommand() {
  @Mixin private lateinit var controlPlaneTlsFlags: ControlPlaneTlsFlags
  @Option(names = ["--control-plane-api-target"], required = true)
  private lateinit var controlPlaneApiTarget: String
  @Option(names = ["--control-plane-api-cert-host"])
  private var controlPlaneApiCertHost: String? = null
  @Option(names = ["--phase"], required = true, description = ["ordinary or recovery"])
  private lateinit var phase: String
  @Option(names = ["--watcher-identifier"], required = true)
  private lateinit var watcherIdentifier: String
  @Option(names = ["--isolated-data-provider"], required = true)
  private lateinit var isolatedDataProvider: String
  @Option(names = ["--live-data-provider"], required = true, split = ",")
  private lateinit var liveDataProviders: List<String>
  @Option(names = ["--ordinary-work-item-file"], required = true)
  private lateinit var ordinaryFile: File
  @Option(names = ["--recovery-work-item-file"], required = true)
  private lateinit var recoveryFile: File
  @Option(names = ["--approval-file"], required = true) private lateinit var approvalFile: File
  @Option(
    names = ["--timeout"],
    defaultValue = "20m",
    converter = [VidLabelingRpcDurationConverter::class],
  )
  private lateinit var timeout: Duration

  override fun run() {
    require(phase in setOf("ordinary", "recovery")) { "Choose ordinary or recovery phase" }
    require(watcherIdentifier.isNotBlank())
    require(DataProviderKey.fromName(isolatedDataProvider) != null)
    require(
      liveDataProviders.isNotEmpty() &&
        liveDataProviders.all { DataProviderKey.fromName(it) != null } &&
        isolatedDataProvider !in liveDataProviders
    ) {
      "Smoke provider must be isolated from every live provider"
    }
    require(!timeout.isNegative && !timeout.isZero) { "Smoke timeout must be positive" }
    val registry =
      TypeRegistry.newBuilder()
        .add(WorkItem.WorkItemParams.getDescriptor())
        .add(VidLabelingDispatchWorkItemParams.getDescriptor())
        .build()
    fun readItem(file: File): WorkItem {
      val item =
        WorkItem.newBuilder()
          .apply { JsonFormat.parser().usingTypeRegistry(registry).merge(file.readText(), this) }
          .build()
      require(
        item.name.isEmpty() &&
          item.generation == 0L &&
          item.state == WorkItem.State.STATE_UNSPECIFIED &&
          item.serializationKey.isEmpty()
      ) {
        "Smoke fixtures contain parameters only, without execution state"
      }
      require(item.queue == VidLabelingDispatchWorkItems.QUEUE) { "Unexpected smoke queue" }
      val frame = item.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
      val app =
        VidLabelingDispatchWorkItems.normalize(
          frame.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java)
        )
      require(app.dataProvider == isolatedDataProvider) {
        "Smoke WorkItem belongs to a live or different provider"
      }
      VidLabelingDispatchWorkItems.source(
        app,
        frame.dataPathParams.takeIf { frame.hasDataPathParams() },
      )
      return item
    }
    val ordinary = readItem(ordinaryFile)
    val recovery = readItem(recoveryFile)
    val ordinaryApp =
      ordinary.workItemParams
        .unpack(WorkItem.WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelingDispatchWorkItemParams::class.java)
    val recoveryApp =
      recovery.workItemParams
        .unpack(WorkItem.WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelingDispatchWorkItemParams::class.java)
    require(!ordinaryApp.hasRecovery() && recoveryApp.hasRecovery()) {
      "Provide one ordinary and one recovery fixture"
    }
    val approval =
      ApproveUploadHealingOperationRequest.newBuilder()
        .apply { JsonFormat.parser().merge(approvalFile.readText(), this) }
        .build()
    require(
      approval.name.startsWith("$isolatedDataProvider/uploadHealingOperations/") &&
        approval.etag.isNotEmpty() &&
        approval.name == recoveryApp.recovery.uploadHealingOperation
    ) {
      "Approval must authorize the isolated recovery fixture"
    }

    val edpaChannel = buildEdpaChannel()
    val controlPlaneChannel =
      buildMutualTlsChannel(
        controlPlaneApiTarget,
        controlPlaneTlsFlags.signingCerts,
        controlPlaneApiCertHost,
      )
    try {
      runBlocking {
        val items = WorkItemsCoroutineStub(controlPlaneChannel)
        val operations = UploadHealingOperationServiceCoroutineStub(edpaChannel)
        val deadline = System.nanoTime() + timeout.toNanos()
        suspend fun awaitIntake(fixture: WorkItem): WorkItem {
          val frame = fixture.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
          val app = frame.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java)
          val source =
            VidLabelingDispatchWorkItems.source(
              app,
              frame.dataPathParams.takeIf { frame.hasDataPathParams() },
            )
          val id =
            DataWatcherWorkItemIds.workItemId(
              DataWatcherWorkItemIds.idempotencyKey(watcherIdentifier, source, app)
            )
          val expectedName = "workItems/$id"
          println("SMOKE_WORK_ITEM=$expectedName")
          var executionGeneration: Long? = null
          while (System.nanoTime() < deadline) {
            val current =
              try {
                items
                  .withDeadlineAfter(30L, TimeUnit.SECONDS)
                  .getWorkItem(getWorkItemRequest { name = expectedName })
              } catch (e: StatusException) {
                if (e.status.code != Status.Code.NOT_FOUND) throw e
                delay(2000L)
                continue
              }
            check(current.name == expectedName && current.queue == fixture.queue) {
              "Unexpected intake identity or queue"
            }
            val observed = current.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
            val observedApp =
              observed.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java)
            check(
              observedApp == app &&
                VidLabelingDispatchWorkItems.source(
                  observedApp,
                  observed.dataPathParams.takeIf { observed.hasDataPathParams() },
                ) == source
            ) {
              "DataWatcher persisted different dispatch parameters"
            }
            check(
              current.generation > 0L &&
                (executionGeneration == null || current.generation == executionGeneration)
            ) {
              "Smoke execution generation changed"
            }
            executionGeneration = current.generation
            when (current.state) {
              WorkItem.State.SUCCEEDED -> return current
              WorkItem.State.QUEUED,
              WorkItem.State.RUNNING -> delay(2000L)
              else -> error("Smoke WorkItem ${current.name} ended in ${current.state}")
            }
          }
          error("Timed out waiting for DataWatcher intake and queued dispatch")
        }
        if (phase == "ordinary") {
          val originalSource = recoveryApp.recovery.source
          val upload =
            RawImpressionUploadServiceCoroutineStub(edpaChannel)
              .withDeadlineAfter(30L, TimeUnit.SECONDS)
              .getRawImpressionUpload(
                getRawImpressionUploadRequest { name = recoveryApp.recovery.sourceUpload }
              )
          check(
            upload.name == recoveryApp.recovery.sourceUpload &&
              upload.doneBlobUri == originalSource.dataPath &&
              upload.doneBlobGeneration == originalSource.generation
          ) {
            "Inspected recovery source does not match its stored upload"
          }
          awaitIntake(ordinary)
        }
        fun operatorSteps(operation: UploadHealingOperation): List<UploadHealingStep> {
          check(operation.name == approval.name) { "Healing response identity differs" }
          check(
            operation.planFormatVersion == 2 &&
              operation.rawImpressionUploadCorrectionCandidatesCount > 0
          ) {
            "Use a current candidate-backed operator-recovery plan"
          }
          val steps =
            operation.stepsList.filter {
              it.recoveryTarget && it.sourceRawImpressionUpload == recoveryApp.recovery.sourceUpload
            }
          check(
            steps.isNotEmpty() &&
              steps.all {
                it.recoveryAction ==
                  RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
              }
          ) {
            "Smoke requires an explicit operator-recovery target; candidate correction replay is not a fresh storage event"
          }
          return steps
        }
        val operation =
          operations
            .withDeadlineAfter(30L, TimeUnit.SECONDS)
            .getUploadHealingOperation(getUploadHealingOperationRequest { name = approval.name })
        operatorSteps(operation)
        if (phase == "ordinary") {
          check(
            operation.state == UploadHealingOperation.State.APPROVAL_REQUIRED &&
              operation.etag == approval.etag
          ) {
            "Inspect a fresh isolated approval-required operator-recovery plan"
          }
          operations
            .withDeadlineAfter(30L, TimeUnit.SECONDS)
            .approveUploadHealingOperation(approval)
        }
        var recordedGeneration: Long? = null
        while (System.nanoTime() < deadline) {
          val current =
            operations
              .withDeadlineAfter(30L, TimeUnit.SECONDS)
              .getUploadHealingOperation(getUploadHealingOperationRequest { name = approval.name })
          val steps = operatorSteps(current)
          val generations = steps.map { it.recoveryDoneBlobGeneration }.toSet()
          if (
            steps.all {
              it.state in
                setOf(UploadHealingStep.State.RECOVERY_STARTED, UploadHealingStep.State.COMPLETE)
            } && generations.size == 1 && generations.single() > 0L
          ) {
            recordedGeneration = generations.single()
            break
          }
          check(
            current.state in
              setOf(
                UploadHealingOperation.State.APPROVED,
                UploadHealingOperation.State.DRAINING,
                UploadHealingOperation.State.EVICTING,
                UploadHealingOperation.State.REPLAYING,
                UploadHealingOperation.State.RECOVERING,
              )
          ) {
            "Operator recovery stopped before recording its marker: ${current.state}"
          }
          delay(2000L)
        }
        val generation =
          checkNotNull(recordedGeneration) {
            "Timed out waiting for the controller-recorded recovery generation"
          }
        if (phase == "ordinary") {
          check(generation != recoveryApp.recovery.source.generation) {
            "Operator recovery reused an old generation; choose a fresh-marker fixture"
          }
          println("RECOVERY_READY=${approval.name}")
          println("RECOVERY_SOURCE_GENERATION=$generation")
          return@runBlocking
        }
        check(generation == recoveryApp.recovery.source.generation) {
          "Recovery fixture does not match the persisted checkpoint"
        }
        awaitIntake(recovery)
        println(
          "PASS: isolated queued dispatch and recovery completed; verify the emitted lifecycle outcomes"
        )
      }
    } finally {
      edpaChannel.shutdown()
      controlPlaneChannel.shutdown()
      edpaChannel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
      controlPlaneChannel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }
  }
}

/** Prepares an isolated queued route while retaining HTTP delivery for live providers. */
@Command(name = "prepare-dispatch-canary-config", mixinStandardHelpOptions = true)
class PrepareDispatchCanaryConfigCommand : Runnable {
  @Option(names = ["--data-watcher-config"], required = true) private lateinit var input: File
  @Option(names = ["--output"], required = true) private lateinit var output: File
  @Option(names = ["--isolated-data-provider"], required = true)
  private lateinit var isolated: String
  @Option(names = ["--dispatcher-endpoint"], required = true) private lateinit var endpoint: String

  override fun run() {
    require(DataProviderKey.fromName(isolated) != null)
    require(endpoint.startsWith("https://"))
    val config = readDataWatcherConfig(input)
    var isolatedRoutes = 0
    val prepared = config.toBuilder().clearWatchedPaths()
    for (route in config.watchedPathsList) {
      if (route.identifier != DISPATCH_ROUTE_IDENTIFIER) {
        prepared.addWatchedPaths(route)
        continue
      }
      require(
        route.hasControlPlaneQueueSink() &&
          route.controlPlaneQueueSink.queue == VidLabelingDispatchWorkItems.QUEUE
      )
      val provider =
        route.controlPlaneQueueSink.appParams
          .unpack(VidLabelingDispatcherParams::class.java)
          .dataProvider
      if (provider == isolated) {
        isolatedRoutes++
        prepared.addWatchedPaths(route)
      } else {
        val params =
          Struct.newBuilder()
            .putFields("dataProvider", Value.newBuilder().setStringValue(provider).build())
            .build()
        prepared.addWatchedPaths(
          route
            .toBuilder()
            .setHttpEndpointSink(
              WatchedPath.HttpEndpointSink.newBuilder()
                .setEndpointUri(endpoint)
                .setAppParams(params)
            )
        )
      }
    }
    require(isolatedRoutes > 0) { "Isolated provider has no queued route" }
    DataWatcherConfigValidator.validate(prepared.build())
    output.writeText(
      TextFormat.printer()
        .usingTypeRegistry(DataWatcherConfigTypes.registry)
        .printToString(prepared.build())
    )
  }
}

/**
 * Prints dispatch-cutover facts parsed with the repository's protobuf loaders, so workflow scripts
 * never interpret structured textproto themselves.
 */
@Command(
  name = "inspect-dispatch-cutover-config",
  mixinStandardHelpOptions = true,
  description = ["Prints configured VID labeling providers and DataWatcher dispatch routing."],
)
class InspectDispatchCutoverConfigCommand : Runnable {
  @Option(names = ["--vid-labeling-config"]) private var vidLabelingConfig: File? = null
  @Option(names = ["--data-watcher-config"]) private var dataWatcherConfig: File? = null

  override fun run() {
    require(vidLabelingConfig != null || dataWatcherConfig != null) {
      "Supply a configuration to inspect"
    }
    val facts = Struct.newBuilder()
    vidLabelingConfig?.let { file ->
      val providers =
        parseTextProto(file, VidLabelingConfigs.getDefaultInstance())
          .configsList
          .map { it.dataProvider }
          .toSortedSet()
      require(providers.isNotEmpty() && providers.all { DataProviderKey.fromName(it) != null }) {
        "VID labeling configuration must name valid DataProviders"
      }
      facts.putFields(
        "dataProviders",
        Value.newBuilder()
          .setListValue(
            ListValue.newBuilder()
              .addAllValues(providers.map { Value.newBuilder().setStringValue(it).build() })
          )
          .build(),
      )
    }
    dataWatcherConfig?.let { file ->
      val routes =
        readDataWatcherConfig(file).watchedPathsList.filter {
          it.identifier == DISPATCH_ROUTE_IDENTIFIER
        }
      val queued =
        routes.count {
          it.hasControlPlaneQueueSink() &&
            it.controlPlaneQueueSink.queue == VidLabelingDispatchWorkItems.QUEUE
        }
      facts.putFields("hasDispatchRoutes", boolValue(routes.isNotEmpty()))
      facts.putFields("anyDispatchRouteQueued", boolValue(queued > 0))
      facts.putFields(
        "allDispatchRoutesQueued",
        boolValue(routes.isNotEmpty() && queued == routes.size),
      )
    }
    println(INSPECTION_PREFIX + JsonFormat.printer().omittingInsignificantWhitespace().print(facts))
  }

  private fun boolValue(value: Boolean): Value = Value.newBuilder().setBoolValue(value).build()

  companion object {
    /** Prefix of the single JSON line read by the cutover workflow script. */
    const val INSPECTION_PREFIX = "DISPATCH_CUTOVER_CONFIG="
  }
}

private const val DISPATCH_ROUTE_IDENTIFIER = "vid-labeling-dispatch"

private fun readDataWatcherConfig(file: File): DataWatcherConfig {
  val builder = DataWatcherConfig.newBuilder()
  TextFormat.Parser.newBuilder()
    .setTypeRegistry(DataWatcherConfigTypes.registry)
    .build()
    .merge(file.readText(), builder)
  return builder.build().also { DataWatcherConfigValidator.validate(it) }
}
