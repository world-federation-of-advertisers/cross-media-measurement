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

import java.time.Clock
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementFailure
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryError
import org.wfanet.measurement.internal.kingdom.RefuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurementFailure
import org.wfanet.measurement.internal.kingdom.measurementLogEntryDetails
import org.wfanet.measurement.internal.kingdom.measurementLogEntryError
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ETags
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionEtagMismatchException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionNotFoundByDataProviderException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

/**
 * Refuses a [Requisition].
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementStateIllegalException] Measurement state is not
 *   PENDING_REQUISITION_FULFILLMENT
 * @throws [RequisitionStateIllegalException] Requisition state is not UNFULFILLED
 * @throws [RequisitionNotFoundByDataProviderException] Requisition not found.
 */
class RefuseRequisition(private val request: RefuseRequisitionRequest) :
  SpannerWriter<Requisition, Requisition>() {
  override suspend fun TransactionScope.runTransaction(): Requisition {
    val readResult: RequisitionReader.Result = readRequisition()
    val (measurementConsumerId, measurementId, requisitionId, requisition, measurementDetails) =
      readResult

    if (request.etag.isNotEmpty()) {
      val currentEtag = ETags.computeETag(requisition.updateTime.toGcloudTimestamp())
      if (request.etag != currentEtag) {
        throw RequisitionEtagMismatchException(request.etag, currentEtag)
      }
    }

    val state = requisition.state
    if (state != Requisition.State.UNFULFILLED) {
      throw RequisitionStateIllegalException(ExternalId(requisition.externalRequisitionId), state) {
        "Expected ${Requisition.State.UNFULFILLED}, got $state"
      }
    }
    val measurementState = requisition.parentMeasurement.state
    if (measurementState != Measurement.State.PENDING_REQUISITION_FULFILLMENT) {
      throw MeasurementStateIllegalException(
        ExternalId(requisition.externalMeasurementConsumerId),
        ExternalId(requisition.externalMeasurementId),
        measurementState,
      ) {
        "Expected ${Measurement.State.PENDING_REQUISITION_FULFILLMENT}, got $measurementState"
      }
    }

    val updatedDetails = requisition.details.copy { refusal = request.refusal }
    updateRequisition(readResult, Requisition.State.REFUSED, updatedDetails)
    val measurementLogEntryDetails = measurementLogEntryDetails {
      logMessage = "Measurement failed due to a requisition refusal"
      this.error = measurementLogEntryError {
        this.type = MeasurementLogEntryError.Type.PERMANENT
        // TODO(@marcopremier): plumb in a clock instance dependency not to hardcode the system
        // one
        this.errorTime = Clock.systemUTC().protoTimestamp()
      }
    }

    val updatedMeasurementDetails =
      measurementDetails.copy {
        failure = measurementFailure {
          reason = MeasurementFailure.Reason.REQUISITION_REFUSED
          message = "ID of refused Requisition: " + externalIdToApiId(request.externalRequisitionId)
        }
      }

    updateMeasurementState(
      measurementConsumerId = measurementConsumerId,
      measurementId = measurementId,
      nextState = Measurement.State.FAILED,
      previousState = measurementState,
      measurementLogEntryDetails = measurementLogEntryDetails,
      details = updatedMeasurementDetails,
    )

    withdrawRequisitions(measurementConsumerId, measurementId, requisitionId)

    return requisition.copy {
      this.state = Requisition.State.REFUSED
      details = updatedDetails
      parentMeasurement = parentMeasurement.copy { this.state = Measurement.State.FAILED }
    }
  }

  override fun ResultScope<Requisition>.buildResult(): Requisition {
    return checkNotNull(transactionResult).copy {
      updateTime = commitTimestamp.toProto()
      etag = ETags.computeETag(commitTimestamp)
    }
  }

  private suspend fun TransactionScope.readRequisition(): RequisitionReader.Result {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val externalRequisitionId = ExternalId(request.externalRequisitionId)

    return RequisitionReader.readByExternalDataProviderId(
      transactionContext,
      externalDataProviderId = externalDataProviderId,
      externalRequisitionId = externalRequisitionId,
    )
      ?: throw RequisitionNotFoundByDataProviderException(
        externalDataProviderId,
        externalRequisitionId,
      )
  }
}
