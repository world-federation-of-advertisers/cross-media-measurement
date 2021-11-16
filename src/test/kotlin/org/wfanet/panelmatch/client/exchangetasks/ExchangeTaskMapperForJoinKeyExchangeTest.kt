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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.encryptStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.launcher.testing.inputStep
import org.wfanet.panelmatch.client.privatemembership.testing.PlaintextPrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.testing.PlaintextQueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.testing.PlaintextQueryResultsDecryptor
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetailsKt.gcsStorage
import org.wfanet.panelmatch.client.storage.storageDetails
import org.wfanet.panelmatch.client.storage.testing.makeTestPrivateStorageSelector
import org.wfanet.panelmatch.client.storage.testing.makeTestSharedStorageSelector
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.crypto.testing.FakeDeterministicCommutativeCipher
import org.wfanet.panelmatch.common.secrets.testing.TestSecretMap
import org.wfanet.panelmatch.common.testing.AlwaysReadyThrottler
import org.wfanet.panelmatch.common.testing.runBlockingTest

// TODO: move elsewhere to enable reuse.
class TestPrivateStorageSelector {
  private val blobs = ConcurrentHashMap<String, StorageClient.Blob>()

  val storageClient = InMemoryStorageClient(blobs)
  val storageDetails = TestSecretMap()
  val selector = makeTestPrivateStorageSelector(storageDetails, storageClient)
}

// TODO: move elsewhere to enable reuse.
class TestSharedStorageSelector {
  private val blobs = ConcurrentHashMap<String, StorageClient.Blob>()

  val storageClient = InMemoryStorageClient(blobs)
  val storageDetails = TestSecretMap()
  val selector = makeTestSharedStorageSelector(storageDetails, storageClient)
}

private val WORKFLOW = exchangeWorkflow {
  steps += inputStep("a" to "b")
  steps += step { encryptStep = encryptStep {} }
}

private val DATE: LocalDate = LocalDate.of(2021, 11, 1)

private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private val ATTEMPT_KEY =
  ExchangeStepAttemptKey(RECURRING_EXCHANGE_ID, "some-exchange", "some-step", "some-attempt")

@RunWith(JUnit4::class)
class ExchangeTaskMapperForJoinKeyExchangeTest {
  private val testSharedStorageSelector = TestSharedStorageSelector()
  private val testPrivateStorageSelector = TestPrivateStorageSelector()
  private val exchangeTaskMapper =
    object : ExchangeTaskMapperForJoinKeyExchange() {
      override val deterministicCommutativeCryptor = FakeDeterministicCommutativeCipher
      override val getPrivateMembershipCryptor = { _: ByteString ->
        PlaintextPrivateMembershipCryptor()
      }
      override val queryResultsDecryptor = PlaintextQueryResultsDecryptor()
      override val privateStorageSelector = testPrivateStorageSelector.selector
      override val sharedStorageSelector = testSharedStorageSelector.selector
      override val certificateManager = TestCertificateManager()
      override val inputTaskThrottler = AlwaysReadyThrottler
      override val getQueryResultsEvaluator = { _: ByteString -> PlaintextQueryEvaluator }
    }

  private val testStorageDetails = storageDetails {
    gcs = gcsStorage {}
    visibility = StorageDetails.Visibility.PRIVATE
  }

  @Before
  fun setUpStorageDetails() {
    testPrivateStorageSelector.storageDetails.underlyingMap[RECURRING_EXCHANGE_ID] =
      testStorageDetails.toByteString()
  }

  @Test
  fun `map input task`() = runBlockingTest {
    val context = ExchangeContext(ATTEMPT_KEY, DATE, WORKFLOW, WORKFLOW.getSteps(0))
    val exchangeTask = exchangeTaskMapper.getExchangeTaskForStep(context)
    assertThat(exchangeTask).isInstanceOf(InputTask::class.java)
  }

  @Test
  fun `map crypto task`() = runBlockingTest {
    val context = ExchangeContext(ATTEMPT_KEY, DATE, WORKFLOW, WORKFLOW.getSteps(1))
    val exchangeTask = exchangeTaskMapper.getExchangeTaskForStep(context)
    assertThat(exchangeTask).isInstanceOf(CryptorExchangeTask::class.java)
  }
}
