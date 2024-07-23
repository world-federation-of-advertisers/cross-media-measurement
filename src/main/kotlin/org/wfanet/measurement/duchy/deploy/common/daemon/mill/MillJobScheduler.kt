/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill

import io.grpc.StatusException
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.models.V1Container
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1JobSpec
import io.kubernetes.client.openapi.models.V1LabelSelector
import io.kubernetes.client.openapi.models.V1ObjectMeta
import io.kubernetes.client.openapi.models.V1OwnerReference
import io.kubernetes.client.openapi.models.V1PodTemplate
import io.kubernetes.client.openapi.models.V1PodTemplateSpec
import io.kubernetes.client.util.ClientBuilder
import java.time.Duration
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.properties.Delegates
import kotlin.random.Random
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.delay
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.k8s.KubernetesClient
import org.wfanet.measurement.common.k8s.clone
import org.wfanet.measurement.common.k8s.complete
import org.wfanet.measurement.common.k8s.failed
import org.wfanet.measurement.common.k8s.matchLabelsSelector
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.deploy.common.ComputationsServiceFlags
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt
import org.wfanet.measurement.internal.duchy.claimWorkRequest
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import picocli.CommandLine

/** Scheduler for Mill Kubernetes Jobs. */
class MillJobScheduler(
  private val duchyId: String,
  private val computationsStub: ComputationsGrpcKt.ComputationsCoroutineStub,
  private val deploymentName: String,
  private val pollingDelay: Duration,
  private val successfulJobHistoryLimit: Int,
  private val failedJobHistoryLimit: Int,
  private val liquidLegionsV2PodTemplateName: String,
  private val liquidLegionsV2MaximumConcurrency: Int,
  private val liquidLegionsV2WorkLockDuration: Duration,
  coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
  private val random: Random = Random.Default,
) {
  init {
    val apiClient: ApiClient = ClientBuilder.cluster().build()
    Configuration.setDefaultApiClient(apiClient)
  }

  private val k8sClient = KubernetesClient(coroutineContext = coroutineContext)
  private lateinit var deployment: V1Deployment
  private lateinit var liquidLegionsV2PodTemplate: V1PodTemplate

  suspend fun run() {
    deployment =
      k8sClient.getDeployment(deploymentName) ?: error("Deployment $deploymentName not found")
    liquidLegionsV2PodTemplate =
      k8sClient.getPodTemplate(liquidLegionsV2PodTemplateName)
        ?: error("PodTemplate $liquidLegionsV2PodTemplateName not found")

    while (currentCoroutineContext().isActive) {
      for (computationTypeConfig in COMPUTATION_TYPE_CONFIGS) {
        val ownedJobs: List<V1Job> = getOwnedJobs(computationTypeConfig.millType)
        process(computationTypeConfig, ownedJobs)
        ownedJobs.cleanUp()
      }
      delay(pollingDelay)
    }
  }

  private suspend fun process(
    computationTypeConfig: ComputationTypeConfig,
    ownedJobs: List<V1Job>,
  ) {
    val millType: MillType = computationTypeConfig.millType
    val computationType: ComputationType = computationTypeConfig.computationType

    val activeJobCount: Int =
      ownedJobs.sumOf {
        val count = if ((it.status.active ?: 0) > 0) 1 else 0
        // Workaround for https://youtrack.jetbrains.com/issue/KT-46360
        count
      }
    if (activeJobCount >= millType.maximumConcurrency) {
      logger.fine { "Not scheduling: Maximum concurrency limit reached for $millType" }
      return
    }

    val jobName: String = generateJobName(millType)
    val claimedToken: ComputationToken? =
      claimWork(
        claimWorkRequest {
          this.computationType = computationType
          owner = jobName
          lockDuration = millType.workLockDuration.toProtoDuration()
          prioritizedStages += computationTypeConfig.prioritizedStages
        }
      )
    if (claimedToken == null) {
      logger.fine { "Not scheduling: No work available for computation type $computationType" }
      return
    }

    val claimedComputationId: String = claimedToken.globalComputationId
    logger.info { "Claimed work item for Computation $claimedComputationId" }
    val template =
      millType.podTemplate.template.clone().apply {
        val container: V1Container = spec.containers.first()
        container.addArgsItem("--mill-id=$jobName")
        container.addArgsItem("--computation-type=$computationType")
        container.addArgsItem("--claimed-computation-id=$claimedComputationId")
      }
    createJob(jobName, millType, template)
    logger.info { "Scheduled Job $jobName for Computation $claimedComputationId" }
    if (activeJobCount + 1 >= millType.maximumConcurrency) {
      logger.info { "Mill type $millType is now at maximum concurrency" }
    }
  }

  private suspend fun Collection<V1Job>.cleanUp() {
    deleteOverLimit(successfulJobHistoryLimit) { it.complete && !it.failed }
    deleteOverLimit(failedJobHistoryLimit) { it.failed }
  }

  private suspend fun Collection<V1Job>.deleteOverLimit(limit: Int, predicate: (V1Job) -> Boolean) {
    val affectedJobs: List<V1Job> = filter(predicate)
    val overLimitCount: Int = affectedJobs.size - limit
    if (overLimitCount <= 0) {
      return
    }

    affectedJobs
      .sortedBy { it.status.completionTime }
      .take(overLimitCount)
      .forEach {
        k8sClient.deleteJob(it.metadata.name, it.metadata.namespace)
        logger.info { "Deleted Job ${it.metadata.name}" }
      }
  }

  private fun generateJobName(millType: MillType): String {
    val suffix = KubernetesClient.generateNameSuffix(random)
    return "$duchyId-${millType.jobNamePrefix}-$suffix"
  }

  private suspend fun claimWork(request: ClaimWorkRequest): ComputationToken? {
    val response: ClaimWorkResponse =
      try {
        computationsStub.claimWork(request)
      } catch (e: StatusException) {
        throw Exception("Error claiming work", e)
      }
    return if (response.hasToken()) response.token else null
  }

  private suspend fun getOwnedJobs(millType: MillType): List<V1Job> {
    val labelSelector = V1LabelSelector().apply { matchLabels[MILL_TYPE_LABEL] = millType.name }
    return k8sClient.listJobs(labelSelector.matchLabelsSelector).items.filter {
      it.metadata.ownerReferences.any { ownerRef -> ownerRef.uid == deployment.metadata.uid }
    }
  }

  private suspend fun createJob(
    name: String,
    millType: MillType,
    template: V1PodTemplateSpec,
  ): V1Job {
    val job =
      V1Job().apply {
        apiVersion = "batch/v1"
        kind = "Job"
        metadata =
          V1ObjectMeta().apply {
            this.name = name
            putLabelsItem(MILL_TYPE_LABEL, millType.name)
            ownerReferences =
              listOf(
                V1OwnerReference().apply {
                  apiVersion = deployment.apiVersion
                  kind = deployment.kind
                  this.name = deployment.metadata.name
                  uid = deployment.metadata.uid
                  controller = false
                }
              )
          }
        spec = V1JobSpec().apply { this.template = template }
      }

    return k8sClient.createJob(job)
  }

  private enum class MillType(val jobNamePrefix: String) {
    LIQUID_LEGIONS_V2("llv2-mill-job"),
  }

  private data class ComputationTypeConfig(
    val computationType: ComputationType,
    val millType: MillType,
    val prioritizedStages: List<ComputationStage>,
  )

  private val MillType.workLockDuration: Duration
    get() =
      when (this) {
        MillType.LIQUID_LEGIONS_V2 -> liquidLegionsV2WorkLockDuration
      }

  private val MillType.maximumConcurrency: Int
    get() =
      when (this) {
        MillType.LIQUID_LEGIONS_V2 -> liquidLegionsV2MaximumConcurrency
      }

  private val MillType.podTemplate: V1PodTemplate
    get() =
      when (this) {
        MillType.LIQUID_LEGIONS_V2 -> liquidLegionsV2PodTemplate
      }

  private class Flags {
    @CommandLine.Mixin
    lateinit var tls: TlsFlags
      private set

    @CommandLine.Mixin
    lateinit var duchy: CommonDuchyFlags
      private set

    @CommandLine.Mixin
    lateinit var internalApi: ComputationsServiceFlags
      private set

    @CommandLine.Option(
      names = ["--channel-shutdown-timeout"],
      defaultValue = "3s",
      description = ["How long to allow for the gRPC channel to shutdown."],
    )
    lateinit var channelShutdownTimeout: Duration
      private set

    @CommandLine.Option(
      names = ["--deployment-name"],
      description = ["Name of this K8s Deployment"],
      required = true,
    )
    lateinit var deploymentName: String
      private set

    @CommandLine.Option(
      names = ["--polling-delay"],
      description = ["Delay before subsequent polling attempts"],
      defaultValue = "2s",
    )
    lateinit var pollingDelay: Duration
      private set

    @set:CommandLine.Option(
      names = ["--successful-jobs-history-limit"],
      description = ["Number of successful jobs to retain per mill type"],
      defaultValue = "3",
    )
    var successfulJobsHistoryLimit by Delegates.notNull<Int>()
      private set

    @set:CommandLine.Option(
      names = ["--failed-jobs-history-limit"],
      description = ["Number of failed jobs to retain per mill type"],
      defaultValue = "1",
    )
    var failedJobsHistoryLimit by Delegates.notNull<Int>()
      private set

    @CommandLine.Option(
      names = ["--llv2-pod-template-name"],
      description = ["Name of the K8s PodTemplate for Liquid Legions v2 Jobs"],
      required = true,
    )
    lateinit var liquidLegionsV2PodTemplateName: String
      private set

    @set:CommandLine.Option(
      names = ["--llv2-maximum-concurrency"],
      description = ["Maximum number of concurrent Liquid Legions v2 Jobs"],
      defaultValue = "1",
    )
    var liquidLegionsV2MaximumConcurrency by Delegates.notNull<Int>()
      private set

    @CommandLine.Option(
      names = ["--llv2-work-lock-duration"],
      defaultValue = "5m",
      description = ["How long to hold work locks for Liquid Legions v2"],
    )
    lateinit var liquidLegionsV2WorkLockDuration: Duration
      private set
  }

  companion object {
    private const val MILL_TYPE_LABEL = "mill-type"
    private val COMPUTATION_TYPE_CONFIGS =
      listOf(
        ComputationTypeConfig(
          ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
          MillType.LIQUID_LEGIONS_V2,
          listOf(LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage()),
        ),
        ComputationTypeConfig(
          ComputationType.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
          MillType.LIQUID_LEGIONS_V2,
          listOf(
            ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage()
          ),
        ),
      )

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    @CommandLine.Command(
      name = "MillJobScheduler",
      description = ["Mill Job scheduler"],
      mixinStandardHelpOptions = true,
      showDefaultValues = true,
    )
    private fun run(@CommandLine.Mixin flags: Flags) {
      val internalApiChannel =
        buildMutualTlsChannel(
            flags.internalApi.target,
            flags.tls.signingCerts,
            flags.internalApi.certHost,
          )
          .withShutdownTimeout(flags.channelShutdownTimeout)
          .withDefaultDeadline(flags.internalApi.defaultDeadlineDuration)
      val computationsStub =
        ComputationsGrpcKt.ComputationsCoroutineStub(internalApiChannel).withWaitForReady()
      val app =
        MillJobScheduler(
          flags.duchy.duchyName,
          computationsStub,
          flags.deploymentName,
          flags.pollingDelay,
          flags.successfulJobsHistoryLimit,
          flags.failedJobsHistoryLimit,
          flags.liquidLegionsV2PodTemplateName,
          flags.liquidLegionsV2MaximumConcurrency,
          flags.liquidLegionsV2WorkLockDuration,
        )
      runBlocking { app.run() }
    }

    @JvmStatic fun main(args: Array<String>) = commandLineMain(Companion::run, args)
  }
}
