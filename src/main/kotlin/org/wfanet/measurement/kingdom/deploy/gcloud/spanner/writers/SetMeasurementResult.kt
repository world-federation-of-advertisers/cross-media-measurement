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

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.ComputationParticipantKt.details
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.SetMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

private val NEXT_MEASUREMENT_STATE = Measurement.State.SUCCEEDED
/**
 * Sets participant details for a computationPartcipant in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.MEASUREMENT_NOT_FOUND]
 */
class SetMeasurementResult(private val request: SetMeasurementResultRequest) :
  SpannerWriter<Measurement, Measurement>() {

  override suspend fun TransactionScope.runTransaction(): Measurement {

    val measurementResult =
      MeasurementReader(Measurement.View.DEFAULT)
        .readExternalIdOrNull(transactionContext, ExternalId(request.externalComputationId))
        ?: throw KingdomInternalException(
          KingdomInternalException.Code.MEASUREMENT_NOT_FOUND
        ) { "Measurement for external computation ID ${request.externalComputationId} not found" }

    val measurement = measurementResult.measurement
    val measurementId = measurementResult.measurementId
    val measurementConsumerId = measurementResult.measurementConsumerId

    val measurementDetails =
      measurement.details.copy {
        aggregatorCertificate = request.aggregatorCertificate
        resultPublicKey = request.resultPublicKey
        encryptedResult = request.encryptedResult
      }

    transactionContext.bufferUpdateMutation("Measurements") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("State" to NEXT_MEASUREMENT_STATE)
      set("MeasurementDetails" to measurementDetails)
      setJson("MeasurementDetailsJson" to measurementDetails)
    }

    return measurement.copy {
      state = NEXT_MEASUREMENT_STATE
      details = measurementDetails
    }
  }
  override fun ResultScope<Measurement>.buildResult(): Measurement {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
