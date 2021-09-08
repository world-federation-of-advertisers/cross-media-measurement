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

import com.google.protobuf.ByteString
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.client.logger.getAndClearTaskLog
import org.wfanet.panelmatch.client.logger.loggerFor
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient.VerifiedBlob
import org.wfanet.panelmatch.common.Timeout

/**
 * Maps ExchangeWorkflow.Step to respective tasks. Retrieves necessary inputs. Executes step. Stores
 * outputs.
 */
class ExchangeTaskExecutor(
  private val apiClient: ApiClient,
  private val timeout: Timeout,
  private val sharedStorage: VerifiedStorageClient,
  private val privateStorage: VerifiedStorageClient,
  private val getExchangeTaskForStep: suspend (ExchangeWorkflow.Step) -> ExchangeTask
) : ExchangeStepExecutor {
  /**
   * Reads inputs for [step], executes [step], and writes the outputs to appropriate [StorageClient]
   * .
   */
  override suspend fun execute(attemptKey: ExchangeStepAttemptKey, step: ExchangeWorkflow.Step) {
    withContext(CoroutineName(attemptKey.exchangeStepAttemptId)) {
      try {
        tryExecute(attemptKey, step)
      } catch (e: Exception) {
        logger.addToTaskLog(e.toString())
        markAsFinished(attemptKey, ExchangeStepAttempt.State.FAILED)
        throw e
      }
    }
  }

  private suspend fun readInputs(step: ExchangeWorkflow.Step): Map<String, VerifiedBlob> =
      coroutineScope {
    val privateInputLabels = step.privateInputLabelsMap
    val sharedInputLabels = step.sharedInputLabelsMap
    awaitAll(
      async(start = CoroutineStart.DEFAULT) {
        privateStorage.verifiedBatchRead(inputLabels = privateInputLabels)
      },
      async(start = CoroutineStart.DEFAULT) {
        sharedStorage.verifiedBatchRead(inputLabels = sharedInputLabels)
      }
    )
      .reduce { a, b -> a.toMutableMap().apply { putAll(b) } }
  }

  private suspend fun writeOutputs(
    step: ExchangeWorkflow.Step,
    taskOutput: Map<String, Flow<ByteString>>
  ) {
    coroutineScope {
      val privateOutputLabels = step.privateOutputLabelsMap
      val sharedOutputLabels = step.sharedOutputLabelsMap
      awaitAll(
        async {
          privateStorage.verifiedBatchWrite(outputLabels = privateOutputLabels, data = taskOutput)
        },
        async {
          sharedStorage.verifiedBatchWrite(outputLabels = sharedOutputLabels, data = taskOutput)
        }
      )
    }
  }

  private suspend fun markAsFinished(
    attemptKey: ExchangeStepAttemptKey,
    state: ExchangeStepAttempt.State
  ) {
    apiClient.finishExchangeStepAttempt(attemptKey, state, getAndClearTaskLog())
  }

  private suspend fun tryExecute(attemptKey: ExchangeStepAttemptKey, step: ExchangeWorkflow.Step) {
    logger.addToTaskLog("Executing $step with attempt $attemptKey")
    val exchangeTask: ExchangeTask = getExchangeTaskForStep(step)
    timeout.runWithTimeout {
      val taskInput: Map<String, VerifiedBlob> = readInputs(step)
      val taskOutput: Map<String, Flow<ByteString>> = exchangeTask.execute(taskInput)
      writeOutputs(step, taskOutput)
    }
    markAsFinished(attemptKey, ExchangeStepAttempt.State.SUCCEEDED)
  }

  companion object {
    private val logger by loggerFor()
  }
}
