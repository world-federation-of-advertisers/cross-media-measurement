// Copyright 2024 The Cross-Media Measurement Authors
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
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.exchangetasks.remote.RemoteTaskOrchestrator

class RemoteExchangeTask(
  private val remoteTaskOrchestrator: RemoteTaskOrchestrator,
  private val orchestrationName: String,
  private val exchangeId: String,
  private val exchangeStepIndex: Int,
  private val exchangeStepAttempt: CanonicalExchangeStepAttemptKey,
  private val exchangeDate: LocalDate,
): ExchangeTask {
  override suspend fun execute(input: Map<String, StorageClient.Blob>): Map<String, Flow<ByteString>> {
    try {
      remoteTaskOrchestrator.orchestrateTask(
        orchestrationName,
        exchangeId,
        exchangeStepIndex,
        exchangeStepAttempt,
        exchangeDate,
      )

      return emptyMap()
    } catch (e: Exception) {
      throw ExchangeTaskFailedException.ofPermanent(e)
    }
  }

  override fun skipReadInput(): Boolean = true
}
