// Copyright 2024 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.exchangetasks.testing.FakeExchangeTaskMapper
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.ExchangeStepExecutor
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.asTimeout
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.secrets.testing.TestMutableSecretMap
import org.wfanet.panelmatch.common.secrets.testing.TestSecretMap
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class ExchangeWorkflowDaemonTest {

  @Test
  fun runDaemonLaunchesMultipleExchangeTasksConcurrently() = runBlockingTest {
    val maxConcurrentSteps = Semaphore(permits = 5)
    val daemon =
      object : ExchangeWorkflowDaemon() {
        override val identity = Identity(id = "some-edp-id", party = Party.DATA_PROVIDER)
        override val apiClient =
          TestApiClient(
            recurringExchangeId = "some-recurring-exchange-id",
            exchangeDate = LocalDate.parse("2024-01-01"),
            workflow = ExchangeWorkflow.getDefaultInstance(),
            workflowFingerprint = ByteString.EMPTY,
          )
        override val stepExecutor = TestStepExecutor(maxConcurrentSteps)
        override val rootCertificates = TestSecretMap()
        override val validExchangeWorkflows = TestSecretMap()
        override val throttler =
          MinimumIntervalThrottler(clock = Clock.systemUTC(), interval = Duration.ofMillis(50L))
        override val taskTimeout = Duration.ofDays(1L).asTimeout()
        override val exchangeTaskMapper = FakeExchangeTaskMapper()
        override val clock = Clock.systemUTC()
        override val privateStorageFactories =
          emptyMap<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory>()
        override val privateStorageInfo = StorageDetailsProvider(TestMutableSecretMap())
        override val sharedStorageFactories =
          emptyMap<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory>()
        override val sharedStorageInfo = StorageDetailsProvider(TestMutableSecretMap())
        override val certificateManager = TestCertificateManager
        override val maxParallelClaimedExchangeSteps = null
        override val runMode = RunMode.DAEMON
      }

    val job = launch { daemon.runSuspending() }

    delay(1000)

    assertThat(maxConcurrentSteps.availablePermits).isEqualTo(0)

    job.cancel()
  }

  @Test
  fun runCronJobLaunchesMultipleExchangeTasksConcurrently() = runBlockingTest {
    val maxConcurrentSteps = Semaphore(permits = 5)
    val daemon =
      object : ExchangeWorkflowDaemon() {
        override val identity = Identity(id = "some-edp-id", party = Party.DATA_PROVIDER)
        override val apiClient =
          TestApiClient(
            recurringExchangeId = "some-recurring-exchange-id",
            exchangeDate = LocalDate.parse("2024-01-01"),
            workflow = ExchangeWorkflow.getDefaultInstance(),
            workflowFingerprint = ByteString.EMPTY,
          )
        override val stepExecutor = TestStepExecutor(maxConcurrentSteps)
        override val rootCertificates = TestSecretMap()
        override val validExchangeWorkflows = TestSecretMap()
        override val throttler =
          MinimumIntervalThrottler(clock = Clock.systemUTC(), interval = Duration.ofMillis(50L))
        override val taskTimeout = Duration.ofDays(1L).asTimeout()
        override val exchangeTaskMapper = FakeExchangeTaskMapper()
        override val clock = Clock.systemUTC()
        override val privateStorageFactories =
          emptyMap<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory>()
        override val privateStorageInfo = StorageDetailsProvider(TestMutableSecretMap())
        override val sharedStorageFactories =
          emptyMap<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory>()
        override val sharedStorageInfo = StorageDetailsProvider(TestMutableSecretMap())
        override val certificateManager = TestCertificateManager
        override val maxParallelClaimedExchangeSteps = null
        override val runMode = RunMode.CRON_JOB
      }

    val job = launch { daemon.runSuspending() }

    delay(1000)

    assertThat(maxConcurrentSteps.availablePermits).isEqualTo(0)

    job.cancel()
  }

  /**
   * Test [ApiClient] that returns exchange steps with the given parameters. The returned step's
   * index begins at 0 and increases by 1 each time [claimExchangeStep] is called.
   */
  private class TestApiClient(
    private val recurringExchangeId: String,
    private val exchangeDate: LocalDate,
    private val workflow: ExchangeWorkflow,
    private val workflowFingerprint: ByteString,
  ) : ApiClient {

    private val nextStepIndex = AtomicInteger(0)

    override suspend fun claimExchangeStep(): ApiClient.ClaimedExchangeStep {
      val stepIndex = nextStepIndex.getAndIncrement()
      return ApiClient.ClaimedExchangeStep(
        attemptKey =
          ExchangeStepAttemptKey(
            recurringExchangeId = recurringExchangeId,
            exchangeId = exchangeDate.toString(),
            stepId = stepIndex.toString(),
            attemptId = "1",
          ),
        exchangeDate = exchangeDate,
        stepIndex = stepIndex,
        workflow = workflow,
        workflowFingerprint = workflowFingerprint,
      )
    }

    override suspend fun appendLogEntry(key: ExchangeStepAttemptKey, messages: Iterable<String>) {
      throw UnsupportedOperationException("Unimplemented")
    }

    override suspend fun finishExchangeStepAttempt(
      key: ExchangeStepAttemptKey,
      finalState: ExchangeStepAttempt.State,
      logEntryMessages: Iterable<String>,
    ) {
      throw UnsupportedOperationException("Unimplemented")
    }
  }

  /**
   * Test [ExchangeStepExecutor] that runs steps by looping indefinitely. Acquires permits from
   * [semaphore] in order to execute steps. One can check the number of available permits in order
   * to ascertain the number of concurrently executing steps.
   */
  private class TestStepExecutor(private val semaphore: Semaphore) : ExchangeStepExecutor {
    override suspend fun execute(exchangeStep: ApiClient.ClaimedExchangeStep) {
      semaphore.withPermit {
        while (currentCoroutineContext().isActive) {
          delay(100)
        }
      }
    }
  }
}
