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

package org.wfanet.panelmatch.client.launcher

import com.google.common.truth.Truth.assertThat
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.panelmatch.client.launcher.testing.DOUBLE_BLINDED_KEYS
import org.wfanet.panelmatch.client.launcher.testing.DP_0_SECRET_KEY
import org.wfanet.panelmatch.client.launcher.testing.JOIN_KEYS
import org.wfanet.panelmatch.client.launcher.testing.LOOKUP_KEYS
import org.wfanet.panelmatch.client.launcher.testing.MP_0_SECRET_KEY
import org.wfanet.panelmatch.client.launcher.testing.SINGLE_BLINDED_KEYS
import org.wfanet.panelmatch.client.launcher.testing.TestStep
import org.wfanet.panelmatch.client.storage.InMemoryStorage
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputs
import org.wfanet.panelmatch.protocol.common.parseSerializedSharedInputs

private const val EXCHANGE_KEY = "some-exchange-key-00"
private const val ATTEMPT_KEY = "some-attempt-key-01"

@RunWith(JUnit4::class)
class ExchangeTaskExecutorTest {
  private val apiClient: ApiClient = mock()
  private val privateStorage = InMemoryStorage(keyPrefix = "private")
  private val sharedStorage = InMemoryStorage(keyPrefix = "shared")

  @Test
  fun `test encrypt exchange step`() = runBlocking {
    val testStep =
      TestStep(
        apiClient = apiClient,
        exchangeKey = EXCHANGE_KEY,
        exchangeStepAttemptKey = ATTEMPT_KEY,
        privateInputLabels =
          mapOf(
            "encryption-key" to "$EXCHANGE_KEY-mp-crypto-key",
            "unencrypted-data" to "$EXCHANGE_KEY-mp-joinkeys"
          ),
        sharedOutputLabels = mapOf("encrypted-data" to "$EXCHANGE_KEY-mp-single-blinded-joinkeys"),
        stepType = ExchangeWorkflow.Step.StepCase.ENCRYPT_STEP,
        privateStorage = privateStorage,
        sharedStorage = sharedStorage
      )
    privateStorage.batchWrite(
      outputLabels = mapOf("output" to "$EXCHANGE_KEY-mp-crypto-key"),
      data = mapOf("output" to MP_0_SECRET_KEY)
    )
    privateStorage.batchWrite(
      outputLabels = mapOf("output" to "$EXCHANGE_KEY-mp-joinkeys"),
      data = mapOf("output" to makeSerializedSharedInputs(JOIN_KEYS))
    )

    testStep.buildAndExecuteTask()

    val singleBlindedKeys =
      requireNotNull(
        sharedStorage.batchRead(
          inputLabels = mapOf("input" to "$EXCHANGE_KEY-mp-single-blinded-joinkeys")
        )["input"]
      )
    assertThat(parseSerializedSharedInputs(singleBlindedKeys)).isEqualTo(SINGLE_BLINDED_KEYS)
    verify(apiClient, times(1))
      .finishExchangeStepAttempt(any(), eq(ExchangeStepAttempt.State.SUCCEEDED), any())
  }

  @Test
  fun `test intersect and validate exchange step passes`() = runBlocking {
    val testStep =
      TestStep(
        apiClient = apiClient,
        exchangeKey = EXCHANGE_KEY,
        exchangeStepAttemptKey = ATTEMPT_KEY,
        sharedInputLabels = mapOf("current-data" to "$EXCHANGE_KEY-mp-single-blinded-joinkeys"),
        privateInputLabels =
          mapOf("previous-data" to "$EXCHANGE_KEY-previous-mp-single-blinded-joinkeys"),
        privateOutputLabels =
          mapOf("current-data" to "$EXCHANGE_KEY-copy-mp-single-blinded-joinkeys"),
        stepType = ExchangeWorkflow.Step.StepCase.INTERSECT_AND_VALIDATE_STEP,
        intersectMaxSize = 100000,
        intersectMinimumOverlap = 0.99f,
        privateStorage = privateStorage,
        sharedStorage = sharedStorage
      )
    sharedStorage.batchWrite(
      outputLabels = mapOf("output" to "$EXCHANGE_KEY-mp-single-blinded-joinkeys"),
      data = mapOf("output" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS))
    )
    privateStorage.batchWrite(
      outputLabels = mapOf("output" to "$EXCHANGE_KEY-previous-mp-single-blinded-joinkeys"),
      data = mapOf("output" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS))
    )
    testStep.buildAndExecuteTask()
    val singleBlindedKeysCopy =
      privateStorage.batchRead(
        inputLabels = mapOf("input" to "$EXCHANGE_KEY-copy-mp-single-blinded-joinkeys")
      )["input"]
    assertThat(parseSerializedSharedInputs(requireNotNull(singleBlindedKeysCopy)))
      .isEqualTo(SINGLE_BLINDED_KEYS)
    verify(apiClient, times(1))
      .finishExchangeStepAttempt(any(), eq(ExchangeStepAttempt.State.SUCCEEDED), any())
  }

  @Test
  fun `test intersect and validate exchange step fails`() = runBlocking {
    val testStep =
      TestStep(
        apiClient = apiClient,
        exchangeKey = EXCHANGE_KEY,
        exchangeStepAttemptKey = ATTEMPT_KEY,
        sharedInputLabels = mapOf("current-data" to "$EXCHANGE_KEY-mp-single-blinded-joinkeys"),
        privateInputLabels =
          mapOf("previous-data" to "$EXCHANGE_KEY-previous-mp-single-blinded-joinkeys"),
        privateOutputLabels =
          mapOf("current-data" to "$EXCHANGE_KEY-copy-mp-single-blinded-joinkeys"),
        stepType = ExchangeWorkflow.Step.StepCase.INTERSECT_AND_VALIDATE_STEP,
        intersectMaxSize = 100000,
        intersectMinimumOverlap = 0.99f,
        privateStorage = privateStorage,
        sharedStorage = sharedStorage
      )
    sharedStorage.batchWrite(
      outputLabels = mapOf("output" to "$EXCHANGE_KEY-mp-single-blinded-joinkeys"),
      data = mapOf("output" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS))
    )
    privateStorage.batchWrite(
      outputLabels = mapOf("output" to "$EXCHANGE_KEY-previous-mp-single-blinded-joinkeys"),
      data = mapOf("output" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS.dropLast(2)))
    )
    assertFailsWith(IllegalArgumentException::class) { testStep.buildAndExecuteTask() }
    // Copy is not available if validate fails
    assertFailsWith(IllegalArgumentException::class) {
      privateStorage.batchRead(
        inputLabels = mapOf("input" to "$EXCHANGE_KEY-copy-mp-single-blinded-joinkeys")
      )["input"]
    }
    verify(apiClient, times(1))
      .finishExchangeStepAttempt(any(), eq(ExchangeStepAttempt.State.FAILED), any())
  }

  @Test
  fun `test reencrypt exchange task`() = runBlocking {
    val testStep =
      TestStep(
        apiClient = apiClient,
        exchangeKey = EXCHANGE_KEY,
        exchangeStepAttemptKey = ATTEMPT_KEY,
        privateInputLabels = mapOf("encryption-key" to "$EXCHANGE_KEY-dp-crypto-key"),
        sharedInputLabels = mapOf("encrypted-data" to "$EXCHANGE_KEY-mp-single-blinded-joinkeys"),
        sharedOutputLabels =
          mapOf("reencrypted-data" to "$EXCHANGE_KEY-dp-mp-double-blinded-joinkeys"),
        stepType = ExchangeWorkflow.Step.StepCase.REENCRYPT_STEP,
        privateStorage = privateStorage,
        sharedStorage = sharedStorage
      )
    privateStorage.batchWrite(
      outputLabels = mapOf("output" to "$EXCHANGE_KEY-dp-crypto-key"),
      data = mapOf("output" to DP_0_SECRET_KEY)
    )
    sharedStorage.batchWrite(
      outputLabels = mapOf("output" to "$EXCHANGE_KEY-mp-single-blinded-joinkeys"),
      data = mapOf("output" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS))
    )
    testStep.buildAndExecuteTask()
    val doubleBlindedKeys =
      requireNotNull(
        sharedStorage.batchRead(
          inputLabels = mapOf("input" to "$EXCHANGE_KEY-dp-mp-double-blinded-joinkeys")
        )["input"]
      )
    assertThat(parseSerializedSharedInputs(doubleBlindedKeys)).isEqualTo(DOUBLE_BLINDED_KEYS)
    verify(apiClient, times(1))
      .finishExchangeStepAttempt(any(), eq(ExchangeStepAttempt.State.SUCCEEDED), any())
  }

  @Test
  fun `test decrypt exchange step that only mp has access to lookup keys`() = runBlocking {
    val testStep =
      TestStep(
        apiClient = apiClient,
        exchangeKey = EXCHANGE_KEY,
        exchangeStepAttemptKey = ATTEMPT_KEY,
        privateInputLabels = mapOf("encryption-key" to "$EXCHANGE_KEY-mp-crypto-key"),
        sharedInputLabels =
          mapOf("encrypted-data" to "$EXCHANGE_KEY-dp-mp-double-blinded-joinkeys"),
        privateOutputLabels = mapOf("decrypted-data" to "$EXCHANGE_KEY-decrypted-data"),
        stepType = ExchangeWorkflow.Step.StepCase.DECRYPT_STEP,
        privateStorage = privateStorage,
        sharedStorage = sharedStorage
      )
    privateStorage.batchWrite(
      outputLabels = mapOf("output" to "$EXCHANGE_KEY-mp-crypto-key"),
      data = mapOf("output" to MP_0_SECRET_KEY)
    )
    sharedStorage.batchWrite(
      outputLabels = mapOf("output" to "$EXCHANGE_KEY-dp-mp-double-blinded-joinkeys"),
      data = mapOf("output" to makeSerializedSharedInputs(DOUBLE_BLINDED_KEYS))
    )
    testStep.buildAndExecuteTask()
    assertFailsWith(IllegalArgumentException::class) {
      sharedStorage.batchRead(inputLabels = mapOf("input" to "$EXCHANGE_KEY-decrypted-data"))
    }
    val lookupKeys =
      requireNotNull(
        privateStorage.batchRead(inputLabels = mapOf("input" to "$EXCHANGE_KEY-decrypted-data"))[
          "input"]
      )
    assertThat(parseSerializedSharedInputs(lookupKeys)).isEqualTo(LOOKUP_KEYS)
    verify(apiClient, times(1))
      .finishExchangeStepAttempt(any(), eq(ExchangeStepAttempt.State.SUCCEEDED), any())
  }
}
