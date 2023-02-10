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
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementKt.resultInfo
import org.wfanet.measurement.internal.kingdom.SetMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

private val NEXT_MEASUREMENT_STATE = Measurement.State.SUCCEEDED

/**
 * Sets participant details for a computationParticipant in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementNotFoundByComputationException] Measurement not found
 * @throws [DuchyNotFoundException] Duchy not found
 * @throws [DuchyCertificateNotFoundException] Duchy's Certificate not found
 */
class SetMeasurementResult(private val request: SetMeasurementResultRequest) :
  SpannerWriter<Measurement, Measurement>() {

  override suspend fun TransactionScope.runTransaction(): Measurement {
    val (measurementConsumerId, measurementId, measurement) =
      MeasurementReader(Measurement.View.DEFAULT)
        .readByExternalComputationId(transactionContext, ExternalId(request.externalComputationId))
        ?: throw MeasurementNotFoundByComputationException(
          ExternalId(request.externalComputationId)
        ) {
          "Measurement for external computation ID ${request.externalComputationId} not found"
        }
    val aggregatorDuchyId =
      DuchyIds.getInternalId(request.externalAggregatorDuchyId)
        ?: throw DuchyNotFoundException(request.externalAggregatorDuchyId) {
          "Duchy with external ID ${request.externalAggregatorDuchyId} not found"
        }
    val aggregatorCertificateId =
      CertificateReader.getDuchyCertificateId(
        transactionContext,
        InternalId(aggregatorDuchyId),
        ExternalId(request.externalAggregatorCertificateId)
      )
        ?: throw DuchyCertificateNotFoundException(
          request.externalAggregatorDuchyId,
          ExternalId(request.externalAggregatorCertificateId)
        ) {
          "Aggregator certificate ${request.externalAggregatorCertificateId} not found"
        }

    transactionContext.bufferInsertMutation("DuchyMeasurementResults") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("DuchyId" to aggregatorDuchyId)
      set("CertificateId" to aggregatorCertificateId)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("EncryptedResult" to request.encryptedResult.toGcloudByteArray())
    }

    transactionContext.bufferUpdateMutation("Measurements") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("State" to NEXT_MEASUREMENT_STATE)
    }

    return measurement.copy {
      state = NEXT_MEASUREMENT_STATE
      results += resultInfo {
        externalAggregatorDuchyId = request.externalAggregatorDuchyId
        externalCertificateId = request.externalAggregatorCertificateId
        encryptedResult = request.encryptedResult
      }
    }
  }
  override fun ResultScope<Measurement>.buildResult(): Measurement {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
