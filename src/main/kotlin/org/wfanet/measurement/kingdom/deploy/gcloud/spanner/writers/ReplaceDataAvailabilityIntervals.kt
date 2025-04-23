/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.type.Interval
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.ModelLineKey
import org.wfanet.measurement.internal.kingdom.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineInternalKey
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

/**
 * Writer for replacing data availability intervals in a [DataProvider].
 *
 * Throws one of the following on [execute]:
 * * [DataProviderNotFoundException]
 * * [ModelLineNotFoundException]
 */
class ReplaceDataAvailabilityIntervals(
  private val request: ReplaceDataAvailabilityIntervalsRequest
) : SpannerWriter<DataProvider, DataProvider>() {
  override suspend fun TransactionScope.runTransaction(): DataProvider {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val readResult: DataProviderReader.Result =
      DataProviderReader().readByExternalDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)
    transactionContext.syncDataAvailabilityIntervals(
      InternalId(readResult.dataProviderId),
      readResult.dataProvider.dataAvailabilityIntervalsList,
    )

    // TODO(world-federation-of-advertisers/cross-media-measurement#2178): Update UpdateTime.

    return readResult.dataProvider
  }

  override fun ResultScope<DataProvider>.buildResult(): DataProvider {
    return transactionResult!!.copy {
      dataAvailabilityIntervals.clear()
      dataAvailabilityIntervals += request.dataAvailabilityIntervalsList
    }
  }

  private suspend fun AsyncDatabaseClient.TransactionContext.syncDataAvailabilityIntervals(
    dataProviderId: InternalId,
    existingEntries: Iterable<DataProvider.DataAvailabilityMapEntry>,
  ) {
    val externalModelLineKeys =
      request.dataAvailabilityIntervalsList.concat(existingEntries).map { it.key }.toSet()
    val keyMapping: Map<ModelLineKey, ModelLineInternalKey> =
      ModelLineReader.readInternalIds(this, externalModelLineKeys)
    val replacementIntervals: Map<ModelLineInternalKey, Interval> =
      request.dataAvailabilityIntervalsList.toMap(keyMapping)
    val existingIntervals: Map<ModelLineInternalKey, Interval> = existingEntries.toMap(keyMapping)

    if (replacementIntervals == existingIntervals) {
      return // Optimization.
    }

    for ((modelLineKey: ModelLineInternalKey, interval: Interval) in replacementIntervals) {
      if (existingIntervals.containsKey(modelLineKey)) {
        bufferUpdateMutation(TABLE) {
          setAvailabilityInterval(dataProviderId, modelLineKey, interval)
        }
      } else {
        bufferInsertMutation(TABLE) {
          setAvailabilityInterval(dataProviderId, modelLineKey, interval)
        }
      }
    }
    for (modelLineKey: ModelLineInternalKey in existingIntervals.keys) {
      if (!replacementIntervals.containsKey(modelLineKey)) {
        buffer(
          Mutation.delete(
            TABLE,
            Key.of(
              dataProviderId.value,
              modelLineKey.modelProviderId.value,
              modelLineKey.modelSuiteId.value,
              modelLineKey.modelLineId.value,
            ),
          )
        )
      }
    }
  }

  companion object {
    private const val TABLE = "DataProviderAvailabilityIntervals"

    private fun Mutation.WriteBuilder.setAvailabilityInterval(
      dataProviderId: InternalId,
      modelLineKey: ModelLineInternalKey,
      interval: Interval,
    ) {
      set("DataProviderId").to(dataProviderId)
      set("ModelProviderId").to(modelLineKey.modelProviderId)
      set("ModelSuiteId").to(modelLineKey.modelSuiteId)
      set("ModelLineId").to(modelLineKey.modelLineId)
      set("StartTime").to(interval.startTime.toGcloudTimestamp())
      set("EndTime").to(interval.endTime.toGcloudTimestamp())
    }

    /**
     * Converts this collection of map entries to a map keyed by [ModelLineInternalKey].
     *
     * @throws ModelLineNotFoundException
     */
    private fun Iterable<DataProvider.DataAvailabilityMapEntry>.toMap(
      keyMapping: Map<ModelLineKey, ModelLineInternalKey>
    ): Map<ModelLineInternalKey, Interval> {
      val source = this
      return buildMap {
        for (entry in source) {
          val externalKey: ModelLineKey = entry.key
          val key: ModelLineInternalKey =
            keyMapping[entry.key]
              ?: throw ModelLineNotFoundException(
                ExternalId(externalKey.externalModelProviderId),
                ExternalId(externalKey.externalModelSuiteId),
                ExternalId(externalKey.externalModelLineId),
              )
          put(key, entry.value)
        }
      }
    }

    private fun <T> Iterable<T>.concat(other: Iterable<T>): Sequence<T> {
      return asSequence() + other.asSequence()
    }
  }
}
