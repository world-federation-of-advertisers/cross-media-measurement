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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.sign
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_FILE
import org.wfanet.measurement.common.crypto.testing.KEY_ALGORITHM
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.launcher.testing.MP_0_SECRET_KEY
import org.wfanet.panelmatch.client.launcher.testing.inputStep
import org.wfanet.panelmatch.client.storage.testing.makeTestVerifiedStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class InputTaskTest {
  private val underlyingStorage = mock<StorageClient>()
  private val storage = makeTestVerifiedStorageClient(underlyingStorage)

  private val secretKeySourceBlob =
    mock<StorageClient.Blob> {
      on { read(any()) } doReturn MP_0_SECRET_KEY.asBufferedFlow(1024)
    } // MP_0_SECRET_KEY
  private val secretKeySourceBlobSignature =
    mock<StorageClient.Blob> {
      on { read(any()) } doReturn
        readPrivateKey(FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)
          .sign(readCertificate(FIXED_SERVER_CERT_PEM_FILE), MP_0_SECRET_KEY)
          .asBufferedFlow(1024)
    } // MP_0_SECRET_KEY

  private val throttler =
    object : Throttler {
      override suspend fun <T> onReady(block: suspend () -> T): T {
        return block()
      }
    }

  @Test
  fun `wait on input`() = runBlockingTest {
    val step = inputStep("input" to "mp-crypto-key")
    val task = InputTask(step, throttler, storage)

    whenever(underlyingStorage.getBlob("mp-crypto-key"))
      .thenReturn(null)
      .thenReturn(null)
      .thenReturn(null)
      .thenReturn(null)
      .thenReturn(secretKeySourceBlob)

    whenever(underlyingStorage.getBlob("mp-crypto-key_signature"))
      .thenReturn(secretKeySourceBlobSignature)

    val result: Map<String, Flow<ByteString>> = task.execute(emptyMap())

    assertThat(result).isEmpty()

    verify(underlyingStorage, times(5)).getBlob("mp-crypto-key")
    verify(underlyingStorage, times(1)).getBlob("mp-crypto-key_signature")
    verify(underlyingStorage, times(1)).defaultBufferSizeBytes
  }

  @Test
  fun `invalid inputs`() = runBlockingTest {
    fun runTest(step: ExchangeWorkflow.Step) {
      if (step.inputLabelsCount == 0 && step.outputLabelsCount == 1) {
        // Expect no failure.
        InputTask(step, throttler, storage)
      } else {
        assertFailsWith<IllegalArgumentException>(step.toString()) {
          InputTask(step, throttler, storage)
        }
      }
    }

    val maps: List<Map<String, String>> =
      listOf(emptyMap(), mapOf("a" to "b"), mapOf("a" to "b", "c" to "d"))

    for (inputLabels in maps) {
      for (outputLabels in maps) {
        runTest(
          step {
            inputStep = ExchangeWorkflow.Step.InputStep.getDefaultInstance()
            this.inputLabels.putAll(inputLabels)
            this.outputLabels.putAll(outputLabels)
          }
        )
      }
    }
  }
}
