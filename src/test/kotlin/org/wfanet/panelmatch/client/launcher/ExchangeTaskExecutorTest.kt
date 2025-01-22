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
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskFailedException
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.exchangetasks.testing.FakeExchangeTaskMapper
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt.State
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.commutativeDeterministicEncryptStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.internal.exchangeWorkflow
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidator.ValidatedExchangeStep
import org.wfanet.panelmatch.client.launcher.testing.FakeTimeout
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetailsKt
import org.wfanet.panelmatch.client.storage.storageDetails
import org.wfanet.panelmatch.client.storage.testing.TestPrivateStorageSelector
import org.wfanet.panelmatch.common.Fingerprinters.sha256
import org.wfanet.panelmatch.common.storage.toStringUtf8
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private const val EXCHANGE_ID = "some-exchange-id"
private const val EXCHANGE_STEP_ID = "some-step-id"
private const val EXCHANGE_STEP_ATTEMPT_ID = "some-attempt-id"
private val ATTEMPT_KEY =
  ExchangeStepAttemptKey(
    RECURRING_EXCHANGE_ID,
    EXCHANGE_ID,
    EXCHANGE_STEP_ID,
    EXCHANGE_STEP_ATTEMPT_ID,
  )

private val DATE = LocalDate.of(2021, 11, 3)

private val WORKFLOW = exchangeWorkflow {
  steps += step {
    stepId = EXCHANGE_STEP_ID
    this.commutativeDeterministicEncryptStep = commutativeDeterministicEncryptStep {}
    inputLabels["a"] = "b"
    outputLabels["Out:a"] = "c"
  }
}

private val EXCHANGE_STEP =
  ApiClient.ClaimedExchangeStep(
    attemptKey = ATTEMPT_KEY,
    exchangeDate = DATE,
    stepIndex = 0,
    workflow = WORKFLOW,
    workflowFingerprint = sha256(WORKFLOW.toByteString()),
  )

private val VALIDATED_EXCHANGE_STEP = ValidatedExchangeStep(WORKFLOW, WORKFLOW.getSteps(0), DATE)

@RunWith(JUnit4::class)
class ExchangeTaskExecutorTest {
  private val validator: ExchangeStepValidator = mock()
  private val testPrivateStorageSelector = TestPrivateStorageSelector()
  private val apiClient: ApiClient = mock()
  private val timeout = FakeTimeout()

  private val storageDetails = storageDetails {
    gcs = StorageDetailsKt.gcsStorage {}
    visibility = StorageDetails.Visibility.PRIVATE
  }

  private val exchangeTaskExecutor = createExchangeTaskExecutor(FakeExchangeTaskMapper())

  @Before
  fun setUpStorage() {
    testPrivateStorageSelector.storageDetails.underlyingMap[RECURRING_EXCHANGE_ID] =
      storageDetails.toByteString()
  }

  @Test
  fun `reads inputs and writes outputs`() = runBlockingTest {
    prepareBlob("some-blob")
    whenever(validator.validate(any())).thenReturn(VALIDATED_EXCHANGE_STEP)

    exchangeTaskExecutor.execute(EXCHANGE_STEP)

    assertThat(testPrivateStorageSelector.storageClient.getBlob("c")?.toStringUtf8())
      .isEqualTo("Out:commutative-deterministic-encrypt-some-blob")
  }

  @Test
  fun timeout() = runBlockingTest {
    timeout.expired = true
    whenever(validator.validate(any())).thenReturn(VALIDATED_EXCHANGE_STEP)

    exchangeTaskExecutor.execute(EXCHANGE_STEP)

    assertThat(testPrivateStorageSelector.storageClient.getBlob("c")).isNull()
  }

  @Test
  fun `fails with transient attempt state from ExchangeTaskFailedException`() = runBlockingTest {
    prepareBlob("some-blob")
    whenever(validator.validate(any())).thenReturn(VALIDATED_EXCHANGE_STEP)

    val exchangeTaskExecutor =
      createExchangeTaskExecutor(FakeExchangeTaskMapper(::TransientThrowingExchangeTask))

    exchangeTaskExecutor.execute(EXCHANGE_STEP)

    verify(apiClient).finishExchangeStepAttempt(eq(ATTEMPT_KEY), eq(State.FAILED), any())
  }

  @Test
  fun `fails with permanent attempt state from ExchangeTaskFailedException`() = runBlockingTest {
    prepareBlob("some-blob")
    whenever(validator.validate(any())).thenReturn(VALIDATED_EXCHANGE_STEP)

    val exchangeTaskExecutor =
      createExchangeTaskExecutor(FakeExchangeTaskMapper(::PermanentThrowingExchangeTask))

    exchangeTaskExecutor.execute(EXCHANGE_STEP)

    verify(apiClient).finishExchangeStepAttempt(eq(ATTEMPT_KEY), eq(State.FAILED_STEP), any())
  }

  private suspend fun prepareBlob(contents: String) {
    val contentsAsFlow = contents.toByteStringUtf8().asBufferedFlow(1024)
    testPrivateStorageSelector.storageClient.writeBlob("b", contentsAsFlow)
  }

  private fun createExchangeTaskExecutor(
    exchangeTaskMapper: ExchangeTaskMapper
  ): ExchangeTaskExecutor {
    return ExchangeTaskExecutor(
      apiClient,
      timeout,
      testPrivateStorageSelector.selector,
      exchangeTaskMapper,
      validator,
    )
  }
}

private class TransientThrowingExchangeTask(taskName: String) : ExchangeTask {
  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> =
    throw ExchangeTaskFailedException.ofTransient(IllegalStateException())
}

private class PermanentThrowingExchangeTask(taskName: String) : ExchangeTask {
  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> =
    throw ExchangeTaskFailedException.ofPermanent(IllegalStateException())
}
