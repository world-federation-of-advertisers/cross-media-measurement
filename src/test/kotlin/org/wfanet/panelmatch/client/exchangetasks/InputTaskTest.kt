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

package org.wfanet.panelmatch.client.exchangetasks

import com.nhaarman.mockitokotlin2.mock
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.testing.MP_0_SECRET_KEY
import org.wfanet.panelmatch.client.launcher.testing.SINGLE_BLINDED_KEYS
import org.wfanet.panelmatch.client.launcher.testing.TestStep
import org.wfanet.panelmatch.client.storage.InMemoryStorage
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputs

private const val EXCHANGE_KEY = "some-exchange-key-00"
private const val ATTEMPT_KEY = "some-attempt-key-01"

@RunWith(JUnit4::class)
class InputTaskTest {
  private val apiClient: ApiClient = mock()
  private val privateStorage = InMemoryStorage(keyPrefix = "private")
  private val sharedStorage = InMemoryStorage(keyPrefix = "shared")

  @Test
  fun `wait on private input`() = runBlocking {
    val testStep =
      TestStep(
        apiClient = apiClient,
        exchangeKey = EXCHANGE_KEY,
        exchangeStepAttemptKey = ATTEMPT_KEY,
        privateOutputLabels = mapOf("input" to "$EXCHANGE_KEY-mp-crypto-key"),
        stepType = ExchangeWorkflow.Step.StepCase.INPUT_STEP,
        timeoutDuration = Duration.ofMillis(500),
        retryDuration = Duration.ofMillis(100),
        privateStorage = privateStorage,
        sharedStorage = sharedStorage
      )
    coroutineScope {
      var buildJob = async { testStep.buildAndExecuteTask() }
      delay(300)
      privateStorage.batchWrite(
        outputLabels = mapOf("output" to "$EXCHANGE_KEY-mp-crypto-key"),
        data = mapOf("output" to MP_0_SECRET_KEY)
      )
      buildJob.await()
    }
  }

  @Test
  fun `wait on shared input`() = runBlocking {
    val testStep =
      TestStep(
        apiClient = apiClient,
        exchangeKey = EXCHANGE_KEY,
        exchangeStepAttemptKey = ATTEMPT_KEY,
        sharedOutputLabels = mapOf("input" to "$EXCHANGE_KEY-mp-single-blinded-keys"),
        stepType = ExchangeWorkflow.Step.StepCase.INPUT_STEP,
        timeoutDuration = Duration.ofMillis(500),
        retryDuration = Duration.ofMillis(100),
        privateStorage = privateStorage,
        sharedStorage = sharedStorage
      )
    coroutineScope {
      val job = async { testStep.buildAndExecuteTask() }
      delay(300)
      sharedStorage.batchWrite(
        outputLabels = mapOf("output" to "$EXCHANGE_KEY-mp-single-blinded-keys"),
        data = mapOf("output" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS))
      )
      job.await()
    }
  }

  @Test
  fun `wait on private input fails after timeout`() =
    runBlocking<Unit> {
      val testStep =
        TestStep(
          apiClient = apiClient,
          exchangeKey = EXCHANGE_KEY,
          exchangeStepAttemptKey = ATTEMPT_KEY,
          privateOutputLabels = mapOf("input" to "$EXCHANGE_KEY-mp-crypto-key"),
          stepType = ExchangeWorkflow.Step.StepCase.INPUT_STEP,
          timeoutDuration = Duration.ofMillis(500),
          retryDuration = Duration.ofMillis(100),
          privateStorage = privateStorage,
          sharedStorage = sharedStorage
        )
      assertFailsWith(TimeoutCancellationException::class) {
        coroutineScope {
          val job = async { testStep.buildAndExecuteTask() }
          job.await()
        }
      }
    }

  @Test
  fun `wait on shared input fails if party takes too long to write`() =
    runBlocking<Unit> {
      val testStep =
        TestStep(
          apiClient = apiClient,
          exchangeKey = EXCHANGE_KEY,
          exchangeStepAttemptKey = ATTEMPT_KEY,
          sharedOutputLabels = mapOf("input" to "$EXCHANGE_KEY-mp-single-blinded-keys"),
          stepType = ExchangeWorkflow.Step.StepCase.INPUT_STEP,
          timeoutDuration = Duration.ofMillis(500),
          retryDuration = Duration.ofMillis(100),
          privateStorage = privateStorage,
          sharedStorage = sharedStorage
        )
      assertFailsWith(TimeoutCancellationException::class) {
        coroutineScope {
          val job = async { testStep.buildAndExecuteTask() }
          delay(600)
          sharedStorage.batchWrite(
            outputLabels = mapOf("output" to "$EXCHANGE_KEY-mp-single-blinded-keys"),
            data = mapOf("output" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS))
          )
          job.await()
        }
      }
    }
}
