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
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.ExchangeStepLauncher
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
  protected open val stepExecutor by lazy {
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
    val exchangeStepLauncher =
      ExchangeStepLauncher(apiClient = apiClient, taskLauncher = stepExecutor)
    when (runMode) {
      RunMode.DAEMON -> runDaemon(exchangeStepLauncher)
      RunMode.CRON_JOB -> runCronJob(exchangeStepLauncher)
    }
  }

  /** Runs [exchangeStepLauncher] in an infinite loop. */
  protected open suspend fun runDaemon(exchangeStepLauncher: ExchangeStepLauncher) {
    throttler.loopOnReady {
      // All errors thrown inside the loop should be suppressed such that the daemon doesn't crash.
      logAndSuppressExceptionSuspend { exchangeStepLauncher.findAndRunExchangeStep() }
    }
  }

  /**
   * Runs [exchangeStepLauncher] in a loop until there are no remaining tasks and all launched tasks
   * have completed.
   */
  protected open suspend fun runCronJob(exchangeStepLauncher: ExchangeStepLauncher) {
    val activeJobs = mutableListOf<Job>()
    do {
      activeJobs.removeIf { !it.isActive }
      val job = logAndSuppressExceptionSuspend { exchangeStepLauncher.findAndRunExchangeStep() }
      if (job != null) {
        activeJobs += job
      }
    } while (currentCoroutineContext().isActive && activeJobs.isNotEmpty())
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
}
