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
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.StorageNotFoundException
import org.wfanet.panelmatch.client.storage.verifiedBatchRead

/**
 * Input task waits for output labels to be present. Clients should not pass in the actual required
 * inputs for the next task. Instead, these outputs should be small files with contents of `done`
 * that are written after the actual outputs are done being written.
 */
class InputTask(
  private val step: ExchangeWorkflow.Step,
  private val throttler: Throttler,
  private val sharedStorage: StorageClient,
  private val privateStorage: StorageClient
) : ExchangeTask {

  init {
    with(step) {
      require(privateOutputLabelsCount + sharedOutputLabelsCount == 1)
      require(privateInputLabelsCount + sharedInputLabelsCount == 0)
    }
  }

  /** Reads a single blob from either [sharedStorage] or [privateStorage] as specified in [step]. */
  private suspend fun readValue() {
    val privateOutputLabels = step.privateOutputLabelsMap
    val sharedOutputLabels = step.sharedOutputLabelsMap
    if (privateOutputLabels.isNotEmpty()) {
      privateStorage.verifiedBatchRead(inputLabels = privateOutputLabels)
    } else {
      sharedStorage.verifiedBatchRead(inputLabels = sharedOutputLabels)
    }
  }

  private suspend fun isReady(): Boolean {
    return try {
      readValue()
      true
    } catch (e: StorageNotFoundException) {
      false
    }
  }

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    while (true) {
      if (throttler.onReady { isReady() }) {
        // This function only returns that input is ready. It does not return actual values.
        return emptyMap()
      }
    }
  }
}
