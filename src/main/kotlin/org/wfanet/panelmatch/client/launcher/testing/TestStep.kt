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

package org.wfanet.panelmatch.client.launcher.testing

import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import java.time.Duration
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapperForJoinKeyExchange
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.CoroutineLauncher
import org.wfanet.panelmatch.client.launcher.ExchangeTaskExecutor
import org.wfanet.panelmatch.client.storage.Storage
import org.wfanet.panelmatch.protocol.common.Cryptor

val DP_0_SECRET_KEY = ByteString.copyFromUtf8("random-edp-string-0")
val MP_0_SECRET_KEY = ByteString.copyFromUtf8("random-mp-string-0")
val JOIN_KEYS =
  listOf<ByteString>(
    ByteString.copyFromUtf8("some joinkey0"),
    ByteString.copyFromUtf8("some joinkey1"),
    ByteString.copyFromUtf8("some joinkey2"),
    ByteString.copyFromUtf8("some joinkey3"),
    ByteString.copyFromUtf8("some joinkey4")
  )
val SINGLE_BLINDED_KEYS =
  listOf<ByteString>(
    ByteString.copyFromUtf8("some single-blinded key0"),
    ByteString.copyFromUtf8("some single-blinded key1"),
    ByteString.copyFromUtf8("some single-blinded key2"),
    ByteString.copyFromUtf8("some single-blinded key3"),
    ByteString.copyFromUtf8("some single-blinded key4")
  )
val DOUBLE_BLINDED_KEYS =
  listOf<ByteString>(
    ByteString.copyFromUtf8("some double-blinded key0"),
    ByteString.copyFromUtf8("some double-blinded key1"),
    ByteString.copyFromUtf8("some double-blinded key2"),
    ByteString.copyFromUtf8("some double-blinded key3"),
    ByteString.copyFromUtf8("some double-blinded key4")
  )
val LOOKUP_KEYS =
  listOf<ByteString>(
    ByteString.copyFromUtf8("some lookup0"),
    ByteString.copyFromUtf8("some lookup1"),
    ByteString.copyFromUtf8("some lookup2"),
    ByteString.copyFromUtf8("some lookup3"),
    ByteString.copyFromUtf8("some lookup4")
  )

fun buildMockCryptor(): Cryptor {
  val mockCryptor: Cryptor = mock<Cryptor>()
  whenever(mockCryptor.encrypt(any(), any())).thenReturn(SINGLE_BLINDED_KEYS)
  whenever(mockCryptor.reEncrypt(any(), any())).thenReturn(DOUBLE_BLINDED_KEYS)
  whenever(mockCryptor.decrypt(any(), any())).thenReturn(LOOKUP_KEYS)
  return mockCryptor
}

class TestStep(
  val apiClient: ApiClient,
  val exchangeKey: String,
  val exchangeStepAttemptKey: String,
  val privateInputLabels: Map<String, String> = emptyMap<String, String>(),
  val privateOutputLabels: Map<String, String> = emptyMap<String, String>(),
  val sharedInputLabels: Map<String, String> = emptyMap<String, String>(),
  val sharedOutputLabels: Map<String, String> = emptyMap<String, String>(),
  val stepType: ExchangeWorkflow.Step.StepCase,
  val deterministicCommutativeCryptor: Cryptor = buildMockCryptor(),
  // If you are running a high number of runs_per_test in bazel, you need a longer timeout
  val timeoutDuration: Duration = Duration.ofMillis(10000),
  // If the retry is too high the tests will be flaky because it will try to finish the test before
  // the job finishes
  val retryDuration: Duration = Duration.ofMillis(10),
  val sharedStorage: Storage,
  val privateStorage: Storage,
  val attemptKey: ExchangeStepAttemptKey =
    ExchangeStepAttemptKey(
      exchangeId = exchangeKey,
      exchangeStepAttemptId = exchangeStepAttemptKey,
      recurringExchangeId = "some-recurring-exchange-id-0",
      exchangeStepId = "some-exchange-step-id-0"
    )
) {
  private val exchangeTaskExecutor =
    ExchangeTaskExecutor(
      apiClient = apiClient,
      sharedStorage = sharedStorage,
      privateStorage = privateStorage,
      timeoutDuration = timeoutDuration,
      getExchangeTaskForStep =
        ExchangeTaskMapperForJoinKeyExchange(
          deterministicCommutativeCryptor = deterministicCommutativeCryptor,
          retryDuration = retryDuration,
          sharedStorage = sharedStorage,
          privateStorage = privateStorage
        )::getExchangeTaskForStep
    )
  suspend fun buildStep(): ExchangeWorkflow.Step {
    return ExchangeWorkflow.Step.newBuilder()
      .putAllPrivateInputLabels(privateInputLabels)
      .putAllPrivateOutputLabels(privateOutputLabels)
      .putAllSharedInputLabels(sharedInputLabels)
      .putAllSharedOutputLabels(sharedOutputLabels)
      .apply {
        when (stepType) {
          ExchangeWorkflow.Step.StepCase.INPUT_STEP ->
            inputStep = ExchangeWorkflow.Step.InputStep.getDefaultInstance()
          ExchangeWorkflow.Step.StepCase.ENCRYPT_STEP ->
            encryptStep = ExchangeWorkflow.Step.EncryptStep.getDefaultInstance()
          ExchangeWorkflow.Step.StepCase.REENCRYPT_STEP ->
            reencryptStep = ExchangeWorkflow.Step.ReEncryptStep.getDefaultInstance()
          ExchangeWorkflow.Step.StepCase.DECRYPT_STEP ->
            decryptStep = ExchangeWorkflow.Step.DecryptStep.getDefaultInstance()
          else -> error("Unsupported step config")
        }
      }
      .build()
  }

  suspend fun signStep(step: ExchangeWorkflow.Step): SignedData {
    return SignedData.newBuilder().setData(step.toByteString()).build()
  }

  suspend fun buildAndExecuteTask() = runBlocking {
    val step = buildStep()
    val job =
      async(CoroutineName(attemptKey.exchangeId) + Dispatchers.Default) {
        exchangeTaskExecutor.execute(attemptKey = attemptKey, step = step)
      }
    job.await()
  }

  suspend fun buildAndExecuteJob() {
    val builtStep: ExchangeWorkflow.Step = buildStep()
    val signedStep = signStep(builtStep)
    val exchangeStep =
      ExchangeStep.newBuilder()
        .apply {
          signedExchangeWorkflow = signedStep
          state = ExchangeStep.State.READY_FOR_RETRY
        }
        .build()
    CoroutineLauncher(exchangeTaskExecutor = exchangeTaskExecutor)
      .execute(exchangeStep = exchangeStep, attemptKey = attemptKey)
  }
}
