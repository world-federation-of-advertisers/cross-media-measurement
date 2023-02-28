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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryKt
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundByMeasurementConsumerException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

/**
 * Cancels a [Measurement], transitioning its state to [Measurement.State.CANCELLED].
 *
 * Throws a subclass of [KingdomInternalException] on [execute]
 *
 * @throws [MeasurementNotFoundByMeasurementConsumerException] Measurement not found
 * @throws [MeasurementStateIllegalException] Measurement state is not in pending
 */
class CancelMeasurement(
  private val externalMeasurementConsumerId: ExternalId,
  private val externalMeasurementId: ExternalId
) : SpannerWriter<Measurement, Measurement>() {
  override suspend fun TransactionScope.runTransaction(): Measurement {
    val (measurementConsumerId, measurementId, measurement) =
      MeasurementReader(Measurement.View.DEFAULT)
        .readByExternalIds(transactionContext, externalMeasurementConsumerId, externalMeasurementId)
        ?: throw MeasurementNotFoundByMeasurementConsumerException(
          externalMeasurementConsumerId,
          externalMeasurementId
        ) {
          "Measurement with external MeasurementConsumer ID $externalMeasurementConsumerId and " +
            "external Measurement ID $externalMeasurementId not found"
        }

    when (val state = measurement.state) {
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
          state
        ) {
          "Unexpected Measurement state $state (${state.number})"
        }
      }
    }

    val measurementLogEntryDetails =
      MeasurementLogEntryKt.details { logMessage = "Measurement was cancelled" }

    updateMeasurementState(
      measurementConsumerId = measurementConsumerId,
      measurementId = measurementId,
      nextState = Measurement.State.CANCELLED,
      previousState = measurement.state,
      logDetails = measurementLogEntryDetails
    )

    return measurement.copy { this.state = Measurement.State.CANCELLED }
  }

  override fun ResultScope<Measurement>.buildResult(): Measurement {
    return checkNotNull(this.transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
