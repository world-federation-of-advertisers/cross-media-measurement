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

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.common.loggerFor

/**
 * Input task waits for output labels to be present. Clients should not pass in the actual required
 * inputs for the next task. Instead, these outputs should be small files with contents of `done`
 * that are written after the actual outputs are done being written.
 */
class InputTask(
  private val blobKey: String,
  private val throttler: Throttler,
  private val storage: StorageClient,
) : CustomIOExchangeTask() {

  private suspend fun isReady(): Boolean {
    logger.fine("Checking for blobKey '$blobKey'")
    return storage.getBlob(blobKey) != null
  }

  override suspend fun execute() {
    while (currentCoroutineContext().isActive) {
      if (throttler.onReady { isReady() }) {
        // This function only returns that input is ready. It does not return actual values.
        logger.fine("Found blobKey '$blobKey'")
        return
      }
      logger.fine("Did not find '$blobKey'")
    }
  }

  companion object {
    private val logger by loggerFor()
  }
}
