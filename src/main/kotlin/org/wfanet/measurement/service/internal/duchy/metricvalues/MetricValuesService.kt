package org.wfanet.measurement.service.internal.duchy.metricvalues

import com.google.protobuf.ByteString
import io.grpc.Status
import java.nio.ByteBuffer
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.duchy.MetricValueDatabase
import org.wfanet.measurement.internal.duchy.GetMetricValueRequest
import org.wfanet.measurement.internal.duchy.MetricValue
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineImplBase as MetricValuesCoroutineService
import org.wfanet.measurement.internal.duchy.StoreMetricValueRequest
import org.wfanet.measurement.internal.duchy.StreamMetricValueRequest
import org.wfanet.measurement.internal.duchy.StreamMetricValueResponse
import org.wfanet.measurement.storage.MetricValueStore

/**
 * Buffer size in bytes for each chunk's data.
 *
 * The total size for each message in gRPC streaming should be 16-64 KiB
 * according to https://github.com/grpc/grpc.github.io/issues/371.
 */
private const val STREAM_BYTE_BUFFER_SIZE = 1024 * 32 // 32 KiB

/** Implementation of `wfa.measurement.internal.duchy.MetricValues` gRPC service. */
class MetricValuesService(
  private val metricValueDb: MetricValueDatabase,
  private val metricValueStore: MetricValueStore
) : MetricValuesCoroutineService() {

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
    lateinit var resourceKey: MetricValue.ResourceKey
    val bytes = flow<ByteBuffer> {
      requests.collect { requestMessage ->
        if (requestMessage.hasHeader()) {
          resourceKey = requestMessage.header.resourceKey
        } else {
          emit(requestMessage.chunk.data.asReadOnlyByteBuffer())
        }
      }
    }

    val blobKey = metricValueStore.write(bytes)

    return metricValueDb.insertMetricValue(
      MetricValue.newBuilder().apply {
        this.resourceKey = resourceKey
        blobStorageKey = blobKey
      }.build()
    )
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
        }.build()
      )

      // Emit chunks.
      content.read(STREAM_BYTE_BUFFER_SIZE).collect { buffer ->
        emit(
          StreamMetricValueResponse.newBuilder().apply {
            chunkBuilder.data = ByteString.copyFrom(buffer)
          }.build()
        )
      }
    }
}
