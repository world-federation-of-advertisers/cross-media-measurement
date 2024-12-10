// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.panelmatch.client.deploy

import java.time.Clock
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.ExchangeStepExecutor
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidatorImpl
import org.wfanet.panelmatch.client.launcher.ExchangeTaskExecutor
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.client.storage.SharedStorageSelector
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.Timeout
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.secrets.SecretMap
import org.wfanet.panelmatch.common.storage.StorageFactory

/** Runs ExchangeWorkflows. */
abstract class ExchangeWorkflowDaemon : Runnable {
  /** Identity of the party executing this daemon. */
  abstract val identity: Identity

  /** Kingdom [ApiClient]. */
  abstract val apiClient: ApiClient

  /**
   * Maps partner resource names to serialized root certificates (from `X509Certificate::encoded').
   */
  abstract val rootCertificates: SecretMap

  /** [SecretMap] from RecurringExchange ID to serialized ExchangeWorkflow. */
  abstract val validExchangeWorkflows: SecretMap

  /** Limits how often to poll. */
  abstract val throttler: Throttler

  /** How long a task should be allowed to run for before being cancelled. */
  abstract val taskTimeout: Timeout

  /** [ExchangeTaskMapper] to create a task based on the step */
  abstract val exchangeTaskMapper: ExchangeTaskMapper

  /** Clock for ensuring future Exchanges don't execute yet. */
  abstract val clock: Clock

  /** How to build private storage. */
  protected abstract val privateStorageFactories:
    Map<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory>

  /** Serialized [StorageDetails] per RecurringExchange. */
  protected abstract val privateStorageInfo: StorageDetailsProvider

  /** How to build shared storage. */
  protected abstract val sharedStorageFactories:
    Map<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory>

  /** Serialized [StorageDetails] per RecurringExchange. */
  protected abstract val sharedStorageInfo: StorageDetailsProvider

  /** [CertificateManager] for [sharedStorageSelector]. */
  protected abstract val certificateManager: CertificateManager

  /** Maximum allowable number of tasks claimed concurrently. */
  protected abstract val maxParallelClaimedExchangeSteps: Int?

  /** [RunMode] that determines the structure of the job's main loop. */
  protected abstract val runMode: RunMode

  /** [PrivateStorageSelector] for writing to local (non-shared) storage. */
  protected val privateStorageSelector: PrivateStorageSelector by lazy {
    PrivateStorageSelector(privateStorageFactories, privateStorageInfo)
  }

  /** [SharedStorageSelector] for writing to shared storage. */
  protected val sharedStorageSelector: SharedStorageSelector by lazy {
    SharedStorageSelector(certificateManager, sharedStorageFactories, sharedStorageInfo)
  }

  protected open val stepExecutor: ExchangeStepExecutor by lazy {
    ExchangeTaskExecutor(
      apiClient = apiClient,
      timeout = taskTimeout,
      privateStorageSelector = privateStorageSelector,
      exchangeTaskMapper = exchangeTaskMapper,
      validator = ExchangeStepValidatorImpl(identity.party, validExchangeWorkflows, clock),
    )
  }

  override fun run() = runBlocking { runSuspending() }

  suspend fun runSuspending() {
    when (runMode) {
      RunMode.DAEMON -> runDaemon()
      RunMode.CRON_JOB -> runCronJob()
    }
  }

  /**
   * Claims and executes exchange steps in an infinite loop. Claimed steps are launched as child
   * coroutines to allow multiple steps to execute concurrently.
   */
  protected open suspend fun runDaemon() = coroutineScope {
    while (coroutineContext.isActive) {
      val step =
        throttler.onReady {
          try {
            apiClient.claimExchangeStep()
          } catch (e: Exception) {
            logger.severe("Failed to claim exchange step: $e")
            null
          }
        }

      if (step != null) {
        launch {
          try {
            stepExecutor.execute(step)
          } catch (e: Exception) {
            logger.severe("Failed to execute exchange step: $e")
          }
        }
      }
    }
  }

  /**
   * Claims and executes exchange steps until all claimed steps have completed and no further steps
   * are available. Claimed steps are launched as child coroutines to allow multiple steps to
   * execute concurrently.
   */
  protected open suspend fun runCronJob() = coroutineScope {
    while (coroutineContext.isActive) {
      val step =
        throttler.onReady {
          try {
            apiClient.claimExchangeStep()
          } catch (e: Exception) {
            logger.severe("Failed to claim exchange step: $e")
            null
          }
        }

      if (step != null) {
        launch {
          try {
            stepExecutor.execute(step)
          } catch (e: Exception) {
            logger.severe("Failed to execute exchange step: $e")
          }
        }
      } else if (coroutineContext.job.children.none { it.isActive }) {
        logger.info("All available steps executed; shutting down.")
        break
      }
    }
  }

  enum class RunMode {
    /** When running as a daemon, the job will poll for tasks in an infinite loop. */
    DAEMON,

    /**
     * When running as a cron, the job will poll for tasks until there are no outstanding tasks
     * available, as well as no currently active tasks, and then shut down.
     */
    CRON_JOB,
  }

  companion object {
    private val logger by loggerFor()
  }
}
