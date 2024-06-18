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
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.common.ExchangeDateKey

/**
 * Copies [previousBlobKey] from the previous Exchange to the current one.
 *
 * [privateStorageSelector] must be valid for both the previous and current Exchanges. If this is
 * the first Exchange for a RecurringExchange, this should NOT be used.
 */
class CopyFromPreviousExchangeTask(
  private val privateStorageSelector: PrivateStorageSelector,
  private val schedule: ExchangeWorkflow.Schedule,
  private val currentExchangeDateKey: ExchangeDateKey,
  private val previousBlobKey: String,
) : ExchangeTask {
  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    val previousExchangeDate = calculatePreviousExchangeDate(currentExchangeDateKey.date, schedule)
    val previousExchangeDateKey =
      ExchangeDateKey(currentExchangeDateKey.recurringExchangeId, previousExchangeDate)
    val storageClient = privateStorageSelector.getStorageClient(previousExchangeDateKey)
    val previousBlob =
      requireNotNull(storageClient.getBlob(previousBlobKey)) {
        "Previous blob (with key '$previousBlobKey') is missing"
      }
    return mapOf("output" to previousBlob.read())
  }
}

private fun calculatePreviousExchangeDate(
  date: LocalDate,
  schedule: ExchangeWorkflow.Schedule,
): LocalDate {
  return when (schedule.cronExpression) {
    "@daily" -> date.minusDays(1)
    "@weekly" -> date.minusWeeks(1)
    "@monthly" -> date.minusMonths(1)
    "@yearly" -> date.minusYears(1)
    else ->
      throw IllegalArgumentException("Unsupported cron expression: ${schedule.cronExpression}")
  }
}
