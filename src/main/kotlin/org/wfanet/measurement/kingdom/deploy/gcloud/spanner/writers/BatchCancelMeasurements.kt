/*
 * Copyright 2023 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.BatchCancelMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.BatchCancelMeasurementsResponse
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.batchCancelMeasurementsResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurementLogEntryDetails
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ETags
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementEtagMismatchException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundByMeasurementConsumerException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader.Companion.readEtag

/**
 * Cancels [Measurement]s, transitioning their states to [Measurement.State.CANCELLED]. Operation
 * will fail for all [Measurement]s when any is not found.
 *
 * Throws the following [KingdomInternalException] type on [execute]:
 * * [MeasurementNotFoundByMeasurementConsumerException] when a Measurement is not found
 * * [MeasurementStateIllegalException] when a Measurement state is not in pending
 * * [MeasurementEtagMismatchException] when requested etag does not match actual etag
 */
class BatchCancelMeasurements(private val requests: BatchCancelMeasurementsRequest) :
  SpannerWriter<BatchCancelMeasurementsResponse, BatchCancelMeasurementsResponse>() {

  override suspend fun TransactionScope.runTransaction(): BatchCancelMeasurementsResponse {
    val resultsList = mutableListOf<MeasurementReader.Result>()

    for (request in requests.requestsList) {
      val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
      val externalMeasurementId = ExternalId(request.externalMeasurementId)
      val result =
        MeasurementReader(Measurement.View.DEFAULT)
          .readByExternalIds(
            transactionContext,
            externalMeasurementConsumerId,
            externalMeasurementId,
          )
          ?: throw MeasurementNotFoundByMeasurementConsumerException(
            externalMeasurementConsumerId,
            externalMeasurementId,
          ) {
            "Measurement with external MeasurementConsumer ID $externalMeasurementConsumerId and " +
              "external Measurement ID $externalMeasurementId not found"
          }
      when (val state = result.measurement.state) {
        Measurement.State.PENDING_REQUISITION_PARAMS,
        Measurement.State.PENDING_REQUISITION_FULFILLMENT,
        Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
        Measurement.State.PENDING_COMPUTATION -> {}
        Measurement.State.SUCCEEDED,
        Measurement.State.FAILED,
        Measurement.State.CANCELLED,
        Measurement.State.STATE_UNSPECIFIED,
        Measurement.State.UNRECOGNIZED -> {
          throw MeasurementStateIllegalException(
            externalMeasurementConsumerId,
            externalMeasurementId,
            state,
          ) {
            "Unexpected Measurement state $state (${state.number})"
          }
        }
      }
      if (request.etag.isNotEmpty()) {
        val actualEtag =
          readEtag(
            transactionContext,
            Key.of(result.measurementConsumerId.value, result.measurementId.value),
          )
        if (actualEtag != request.etag) {
          throw MeasurementEtagMismatchException(actualEtag, request.etag) {
            "Requested Measurement etag ${request.etag} does not match actual measurement etag " +
              actualEtag
          }
        }
      }

      resultsList.add(result)
    }

    val measurementLogEntryDetails = measurementLogEntryDetails {
      logMessage = "Measurement was cancelled"
    }

    for (result in resultsList) {
      updateMeasurementState(
        measurementConsumerId = result.measurementConsumerId,
        measurementId = result.measurementId,
        nextState = Measurement.State.CANCELLED,
        previousState = result.measurement.state,
        measurementLogEntryDetails = measurementLogEntryDetails,
      )
      withdrawRequisitions(result.measurementConsumerId, result.measurementId)
    }

    return batchCancelMeasurementsResponse {
      measurements +=
        resultsList.map { it.measurement.copy { this.state = Measurement.State.CANCELLED } }
    }
  }

  override fun ResultScope<BatchCancelMeasurementsResponse>.buildResult():
    BatchCancelMeasurementsResponse {
    checkNotNull(this.transactionResult)

    return batchCancelMeasurementsResponse {
      measurements +=
        this@buildResult.transactionResult.measurementsList.map {
          it.copy {
            updateTime = commitTimestamp.toProto()
            etag = ETags.computeETag(commitTimestamp)
          }
        }
    }
  }
}
