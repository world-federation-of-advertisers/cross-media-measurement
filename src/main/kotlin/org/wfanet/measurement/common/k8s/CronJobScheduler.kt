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

package org.wfanet.measurement.common.k8s

import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.models.V1CronJob
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1LabelSelector
import io.kubernetes.client.util.ClientBuilder
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.delay
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.commandLineMain
import picocli.CommandLine

/**
 * Alternative `Job` scheduler for `CronJob` objects within a Kubernetes cluster.
 *
 * Behaviors that differ from the default scheduler:
 * * Missed job executions are always ignored. This should be equivalent to having
 *   `startingDeadlineSeconds` be `0`.
 * * Arbitrary schedules are not supported. Currently, the schedule must be `* * * * *`.
 *
 * In order to not conflict with the default scheduler, `CronJob` objects managed by this scheduler
 * must be suspended.
 */
class CronJobScheduler(
  private val cronJobNames: List<String>,
  private val interval: Duration,
  private val maximumConcurrencyPerCronJob: Map<String, Int>,
  private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
  private val clock: Clock = Clock.systemUTC(),
) {
  init {
    val apiClient: ApiClient = ClientBuilder.cluster().build()
    Configuration.setDefaultApiClient(apiClient)
  }

  private val k8sClient = KubernetesClient(coroutineContext = coroutineContext)
  private var lastAttempt: Instant = Instant.MIN

  suspend fun run() {
    while (currentCoroutineContext().isActive) {
      val now = clock.instant()
      val cronJobs =
        cronJobNames.map { cronJobName ->
          checkNotNull(k8sClient.getCronJob(cronJobName)) { "CronJob $cronJobName not found" }
            .also { cronJob ->
              val spec = cronJob.spec
              check(spec.suspend == true) { "CronJob $cronJobName is not suspended" }
              check(spec.concurrencyPolicy == "Allow") {
                "CronJob $cronJobName has unsupported concurrencyPolicy ${spec.concurrencyPolicy}"
              }
            }
        }

      for (cronJob in cronJobs) {
        maybeScheduleJob(now, cronJob)
      }

      lastAttempt = now
      delay(interval)
    }
  }

  private suspend fun maybeScheduleJob(now: Instant, cronJob: V1CronJob) {
    // TODO(@SanjayVas): Support arbitrary cron schedules.
    require(cronJob.spec.schedule == "* * * * *") { "Only every minute schedule supported" }
    val cronJobName: String = cronJob.metadata.name

    val scheduleTime = now.truncatedTo(ChronoUnit.MINUTES)
    if (!scheduleTime.isAfter(lastAttempt)) {
      logger.fine {
        val nextScheduleTime = scheduleTime.plus(1, ChronoUnit.MINUTES)
        "Not yet ready to schedule CronJob $cronJobName. Next schedule time: $nextScheduleTime"
      }
      return
    }

    val lastScheduleTime: Instant? = cronJob.lastScheduleTime
    if (lastScheduleTime != null && lastScheduleTime >= scheduleTime) {
      logger.fine { "CronJob ${cronJob.metadata.name} was already scheduled" }
      return
    }

    val activeJobCount = getOwnedActiveJobs(cronJob).size
    val maximumConcurrency = maximumConcurrencyPerCronJob.getOrDefault(cronJobName, 1)
    if (activeJobCount >= maximumConcurrency) {
      logger.info {
        "Maximum concurrency limit of $maximumConcurrencyPerCronJob reached. " +
          "Not scheduling CronJob ${cronJob.metadata.name}"
      }
      return
    }

    val job = k8sClient.createJobFromCronJob(cronJob)
    withContext(coroutineContext) {
      // Add our own annotation to track last schedule time.
      k8sClient.kubectlAnnotate<V1CronJob>(
        cronJob.metadata.name,
        mapOf(LAST_SCHEDULE_ANNOTATION to now.toString()),
      )
    }
    logger.info { "Scheduled Job ${job.metadata.name}" }
  }

  private suspend fun getOwnedActiveJobs(cronJob: V1CronJob): List<V1Job> {
    val labelSelector =
      V1LabelSelector().apply { matchLabels.putAll(cronJob.spec.jobTemplate.metadata.labels) }
    return k8sClient.listJobs(labelSelector.matchLabelsSelector).items.filter {
      (it.status.active ?: 0) > 0 &&
        it.metadata.ownerReferences.any { ownerRef -> ownerRef.uid == cronJob.metadata.uid }
    }
  }

  companion object {
    private const val LAST_SCHEDULE_ANNOTATION = "halo-cmm.org/last-schedule-time"

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val V1CronJob.lastScheduleTime: Instant?
      get() {
        val lastScheduleAnnotation: String? = metadata.annotations[LAST_SCHEDULE_ANNOTATION]
        val customLastScheduleTime: Instant? =
          if (lastScheduleAnnotation == null) {
            null
          } else {
            Instant.parse(lastScheduleAnnotation)
          }

        val lastScheduleTime = status.lastScheduleTime?.toInstant()
        return if (customLastScheduleTime == null) {
          lastScheduleTime
        } else if (lastScheduleTime != null) {
          maxOf(customLastScheduleTime, lastScheduleTime)
        } else {
          null
        }
      }

    @CommandLine.Command(
      name = "CronJobScheduler",
      description = ["Custom K8s CronJob scheduler"],
      mixinStandardHelpOptions = true,
      showDefaultValues = true,
    )
    private fun run(
      @CommandLine.Option(
        names = ["--interval"],
        description = ["How long to wait between schedule attempts"],
        defaultValue = "10s",
      )
      interval: Duration,
      @CommandLine.Option(
        names = ["--maximum-concurrency"],
        description =
          [
            "Key-value pair of CronJob name to maximum number of concurrent jobs",
            "Defaults to 1 per CronJob",
          ],
      )
      maximumConcurrencyPerCronJob: Map<String, Int>?,
      @CommandLine.Option(
        names = ["--cron-job-name"],
        description = ["Name of CronJob to schedule from", "May be specified multiple times"],
        required = true,
      )
      cronJobNames: List<String>,
    ) {
      val app = CronJobScheduler(cronJobNames, interval, maximumConcurrencyPerCronJob ?: emptyMap())
      runBlocking { app.run() }
    }

    @JvmStatic fun main(args: Array<String>) = commandLineMain(Companion::run, args)
  }
}
