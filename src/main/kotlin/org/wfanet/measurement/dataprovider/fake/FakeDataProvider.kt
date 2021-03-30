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

package org.wfanet.measurement.dataprovider.fake

import com.google.protobuf.ByteString
import java.util.logging.Logger
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.UploadMetricValueRequest
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.throttler.Throttler

/** Size of chunks in each streamed message for uploading sketches. */
private const val DEFAULT_STREAM_BYTE_BUFFER_SIZE: Int = 1024 * 32 // 32 KiB

/**
 * Implements a basic Data Provider that listens for [MetricRequisition]s and fulfills them.
 *
 * TODO: when upgraded to v2alpha API, this should also accept a SketchConfigsCoroutineStub.
 * This is used to retrieve the SketchConfig, since it's unfortunately not embedded in the
 * Requisition. [generateSketch] will also need to accept the SketchConfig.
 */
class FakeDataProvider(
  private val publisherDataStub: PublisherDataCoroutineStub,
  private val externalDataProviderId: ExternalId,
  private val throttler: Throttler,
  private val generateSketch: (MetricRequisition, ElGamalPublicKey) -> ByteString,
  private val streamByteBufferSize: Int = DEFAULT_STREAM_BYTE_BUFFER_SIZE
) {
  private val publicKeyCache = mutableMapOf<String, CombinedPublicKey>()
  private var lastPageToken = ""

  suspend fun start() {
    while (true) {
      logAndSuppressExceptionSuspend {
        throttler.onReady {
          val requisition: MetricRequisition? = pollForRequisition()

          if (requisition != null) {
            val combinedPublicKey = getCombinedPublicKey(requisition.combinedPublicKey)
            fulfillRequisition(requisition, combinedPublicKey.encryptionKey)
          }
        }
      }
    }
  }

  private suspend fun getCombinedPublicKey(resourceKey: CombinedPublicKey.Key): CombinedPublicKey {
    return publicKeyCache.getOrPut(resourceKey.combinedPublicKeyId) {
      val request = GetCombinedPublicKeyRequest.newBuilder().apply {
        key = resourceKey
      }.build()

      logger.info("Fetching CombinedPublicKey for ${resourceKey.combinedPublicKeyId}")
      publisherDataStub.getCombinedPublicKey(request)
    }
  }

  private fun getListMetricRequisitionsRequest(): ListMetricRequisitionsRequest {
    return ListMetricRequisitionsRequest.newBuilder().apply {
      parentBuilder.dataProviderId = externalDataProviderId.apiId.value
      filterBuilder.addStates(MetricRequisition.State.UNFULFILLED)
      pageToken = lastPageToken
      pageSize = 1
    }.build()
  }

  private suspend fun pollForRequisition(): MetricRequisition? {
    logger.info("Polling for a MetricRequisition")
    val response = publisherDataStub.listMetricRequisitions(getListMetricRequisitionsRequest())
    logger.info("Found ${response.metricRequisitionsCount} MetricRequisitions")
    lastPageToken = response.nextPageToken
    return response.metricRequisitionsList.firstOrNull()
  }

  private suspend fun fulfillRequisition(
    metricRequisition: MetricRequisition,
    key: ElGamalPublicKey
  ) {
    logger.info("Fulfilling MetricRequisition ${metricRequisition.key.metricRequisitionId}")
    val header = makeUploadMetricValueHeader(metricRequisition)

    logger.info("Generating sketch for ${metricRequisition.key.metricRequisitionId}")
    val sketchChunks = generateSketch(metricRequisition, key).asBufferedFlow(streamByteBufferSize)

    publisherDataStub.uploadMetricValue(
      flow {
        logger.info("Uploading header for ${metricRequisition.key.metricRequisitionId}")
        emit(header)

        sketchChunks.collect {
          logger.info("Uploading chunk for ${metricRequisition.key.metricRequisitionId}")
          emit(makeUploadMetricValueBody(it))
        }
      }
    )
  }

  private fun makeUploadMetricValueHeader(
    metricRequisition: MetricRequisition
  ): UploadMetricValueRequest {
    return UploadMetricValueRequest.newBuilder().apply {
      headerBuilder.key = metricRequisition.key
    }.build()
  }

  private fun makeUploadMetricValueBody(chunk: ByteString): UploadMetricValueRequest {
    return UploadMetricValueRequest.newBuilder().apply {
      chunkBuilder.data = chunk
    }.build()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
