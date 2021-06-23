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

import com.google.protobuf.ByteString
import java.time.Duration
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.panelmatch.client.logger.loggerFor
import org.wfanet.panelmatch.client.storage.Storage

/**
 * Input task waits for output labels to be present. Clients should not pass in the actual required
 * inputs for the next task. Instead, these outputs should be small files with contents of `done`
 * that are written after the actual outputs are done being written.
 */
class InputTask(
  val step: ExchangeWorkflow.Step,
  val retryDuration: Duration,
  val sharedStorage: Storage,
  val privateStorage: Storage
) : ExchangeTask {

  /**
   * Waits for all private and shared task input from shared and private storage based on for an
   * [ExchangeStep] and returns when ready
   */
  suspend fun waitForOutputsToBeReady(
    sharedStorage: Storage,
    privateStorage: Storage,
    step: ExchangeWorkflow.Step
  ): List<Map<String, ByteString>> = coroutineScope {
    val privateOutputLabels = step.getPrivateOutputLabelsMap()
    val sharedOutputLabels = step.getSharedOutputLabelsMap()
    awaitAll(
      async(start = CoroutineStart.DEFAULT) {
        privateStorage.batchRead(inputLabels = privateOutputLabels)
      },
      async(start = CoroutineStart.DEFAULT) {
        sharedStorage.batchRead(inputLabels = sharedOutputLabels)
      }
    )
  }

  override suspend fun execute(input: Map<String, ByteString>): Map<String, ByteString> {
    flow<Boolean> {
        waitForOutputsToBeReady(
          sharedStorage = sharedStorage,
          privateStorage = privateStorage,
          step = step
        )
        emit(true)
      }
      .retryWhen { cause, _ ->
        logger.info(cause.toString())
        delay(retryDuration.toMillis())
        true
      }
      .toList()

    // This function only returns that input is ready. It does not return actual values.
    return emptyMap<String, ByteString>()
  }

  companion object {
    val logger by loggerFor()
  }
}
