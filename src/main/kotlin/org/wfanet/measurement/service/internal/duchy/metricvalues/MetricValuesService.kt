// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.service.internal.duchy.metricvalues

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.consumeFirstOr
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.db.duchy.metricvalue.MetricValueDatabase
import org.wfanet.measurement.internal.duchy.GetMetricValueRequest
import org.wfanet.measurement.internal.duchy.MetricValue
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineImplBase as MetricValuesCoroutineService
import org.wfanet.measurement.internal.duchy.StoreMetricValueRequest
import org.wfanet.measurement.internal.duchy.StreamMetricValueRequest
import org.wfanet.measurement.internal.duchy.StreamMetricValueResponse
import org.wfanet.measurement.storage.MetricValueStore
import org.wfanet.measurement.storage.StorageClient

/**
 * Buffer size in bytes for each chunk's data.
 *
 * The total size for each message in gRPC streaming should be 16-64 KiB
 * according to https://github.com/grpc/grpc.github.io/issues/371.
 */
private const val STREAM_BYTE_BUFFER_SIZE = 1024 * 32 // 32 KiB

/** Implementation of `wfa.measurement.internal.duchy.MetricValues` gRPC service. */
class MetricValuesService private constructor(
  private val metricValueDb: MetricValueDatabase,
  private val metricValueStore: MetricValueStore
) : MetricValuesCoroutineService() {

  constructor(
    metricValueDb: MetricValueDatabase,
    storageClient: StorageClient
  ) : this(metricValueDb, MetricValueStore(storageClient))

  override suspend fun getMetricValue(request: GetMetricValueRequest): MetricValue {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (request.keyCase) {
      GetMetricValueRequest.KeyCase.EXTERNAL_ID ->
        metricValueDb.getMetricValue(ExternalId(request.externalId))
      GetMetricValueRequest.KeyCase.RESOURCE_KEY ->
        metricValueDb.getMetricValue(request.resourceKey)
      GetMetricValueRequest.KeyCase.KEY_NOT_SET ->
        throw Status.INVALID_ARGUMENT.withDescription("key not set").asRuntimeException()
    } ?: throw Status.NOT_FOUND.asRuntimeException()
  }

  override suspend fun storeMetricValue(requests: Flow<StoreMetricValueRequest>): MetricValue {
    val metricValue = requests.consumeFirstOr { StoreMetricValueRequest.getDefaultInstance() }
      .use { consumed ->
        val resourceKey = consumed.item.header.resourceKey
        if (!resourceKey.valid) {
          throw Status.INVALID_ARGUMENT.withDescription("resource_key missing or incomplete")
            .asRuntimeException()
        }

        val blob = metricValueStore.write(consumed.remaining.map { it.chunk.data })

        MetricValue.newBuilder().apply {
          this.resourceKey = resourceKey
          blobStorageKey = blob.blobKey
        }.build()
      }

    return metricValueDb.insertMetricValue(metricValue)
  }

  override fun streamMetricValue(request: StreamMetricValueRequest) =
    flow<StreamMetricValueResponse> {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      val metricValue = when (request.keyCase) {
        StreamMetricValueRequest.KeyCase.EXTERNAL_ID ->
          metricValueDb.getMetricValue(ExternalId(request.externalId))
        StreamMetricValueRequest.KeyCase.RESOURCE_KEY ->
          metricValueDb.getMetricValue(request.resourceKey)
        StreamMetricValueRequest.KeyCase.KEY_NOT_SET ->
          throw Status.INVALID_ARGUMENT.withDescription("key not set").asRuntimeException()
      } ?: throw Status.NOT_FOUND.asRuntimeException()

      val content = metricValueStore.get(metricValue.blobStorageKey)
        ?: throw Status.DATA_LOSS.withDescription("Missing metric value data").asRuntimeException()

      // Emit header.
      emit(
        StreamMetricValueResponse.newBuilder().apply {
          headerBuilder.metricValue = metricValue
          headerBuilder.dataSizeBytes = content.size
        }.build()
      )

      // Emit chunks.
      content.read(STREAM_BYTE_BUFFER_SIZE).collect { bytes ->
        emit(
          StreamMetricValueResponse.newBuilder().apply {
            chunkBuilder.data = bytes
          }.build()
        )
      }
    }

  companion object {
    fun forTesting(metricValueDb: MetricValueDatabase, metricValueStore: MetricValueStore) =
      MetricValuesService(metricValueDb, metricValueStore)
  }
}

val MetricValue.ResourceKey.valid: Boolean
  get() {
    return dataProviderResourceId.isNotEmpty() &&
      campaignResourceId.isNotEmpty() &&
      metricRequisitionResourceId.isNotEmpty()
  }
