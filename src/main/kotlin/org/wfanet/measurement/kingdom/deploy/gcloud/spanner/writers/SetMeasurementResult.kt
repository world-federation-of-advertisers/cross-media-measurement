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
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementDetails
import org.wfanet.measurement.internal.kingdom.MeasurementDetailsKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.resultInfo
import org.wfanet.measurement.internal.kingdom.SetMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurementLogEntryDetails
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ETags
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
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
 * @throws [InvalidFieldValueException] result payload does not match the Measurement's protocol
 * @throws [RequiredFieldNotSetException] result payload not set
 */
class SetMeasurementResult(private val request: SetMeasurementResultRequest) :
  SpannerWriter<Measurement, Measurement>() {

  override suspend fun TransactionScope.runTransaction(): Measurement {
    val (measurementConsumerId, measurementId, _, measurement) =
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
    val trusTeeV2: Boolean = measurement.details.protocolConfig.hasTrusTeeV2()
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf oneof case fields cannot be null.
    when (request.resultPayloadCase) {
      SetMeasurementResultRequest.ResultPayloadCase.ENCRYPTED_RESULT ->
        if (trusTeeV2) {
          throw InvalidFieldValueException("encrypted_result") {
            "encrypted_result must not be set for a TrusTEE v2 Measurement"
          }
        }
      SetMeasurementResultRequest.ResultPayloadCase.RESULT ->
        if (!trusTeeV2) {
          throw InvalidFieldValueException("result") {
            "result must only be set for a TrusTEE v2 Measurement"
          }
        }
      SetMeasurementResultRequest.ResultPayloadCase.RESULTPAYLOAD_NOT_SET ->
        throw RequiredFieldNotSetException("result_payload")
    }

    return if (trusTeeV2) {
      setPlaintextResult(measurementConsumerId, measurementId, measurement)
    } else {
      setEncryptedResult(
        measurementConsumerId,
        measurementId,
        InternalId(aggregatorDuchyId),
        measurement,
      )
    }
  }

  /** Sets the encrypted and signed result of [measurement], verified by an aggregator cert. */
  private suspend fun TransactionScope.setEncryptedResult(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    aggregatorDuchyId: InternalId,
    measurement: Measurement,
  ): Measurement {
    val aggregatorCertificateId =
      CertificateReader.getDuchyCertificateId(
        transactionContext,
        aggregatorDuchyId,
        ExternalId(request.externalAggregatorCertificateId),
      )
        ?: throw DuchyCertificateNotFoundException(
          request.externalAggregatorDuchyId,
          ExternalId(request.externalAggregatorCertificateId),
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
      set("PublicApiVersion" to request.publicApiVersion)
    }

    transactionContext.bufferUpdateMutation("Measurements") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("State").toInt64(NEXT_MEASUREMENT_STATE)
    }

    updateMeasurementState(
      measurementConsumerId = measurementConsumerId,
      measurementId = measurementId,
      nextState = NEXT_MEASUREMENT_STATE,
      previousState = measurement.state,
      measurementLogEntryDetails =
        measurementLogEntryDetails { logMessage = "Measurement succeeded" },
    )

    return measurement.copy {
      state = NEXT_MEASUREMENT_STATE
      results += resultInfo {
        externalAggregatorDuchyId = request.externalAggregatorDuchyId
        externalCertificateId = request.externalAggregatorCertificateId
        encryptedResult = request.encryptedResult
        apiVersion = request.publicApiVersion
      }
    }
  }

  /**
   * Sets the plaintext result of a TrusTEE v2 [measurement], which is neither signed nor encrypted
   * and so has no aggregator certificate.
   *
   * The result is stored in the `MeasurementDetails` rather than in `DuchyMeasurementResults`,
   * whose primary key requires a certificate.
   */
  private fun TransactionScope.setPlaintextResult(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    measurement: Measurement,
  ): Measurement {
    val details: MeasurementDetails =
      measurement.details.copy {
        plaintextResult =
          MeasurementDetailsKt.plaintextResult {
            externalAggregatorDuchyId = request.externalAggregatorDuchyId
            result = request.result
            apiVersion = request.publicApiVersion
          }
      }

    updateMeasurementState(
      measurementConsumerId = measurementConsumerId,
      measurementId = measurementId,
      nextState = NEXT_MEASUREMENT_STATE,
      previousState = measurement.state,
      measurementLogEntryDetails =
        measurementLogEntryDetails { logMessage = "Measurement succeeded" },
      details = details,
    )

    return measurement.copy {
      state = NEXT_MEASUREMENT_STATE
      this.details = details
      results += resultInfo {
        externalAggregatorDuchyId = request.externalAggregatorDuchyId
        result = request.result
        apiVersion = request.publicApiVersion
      }
    }
  }

  override fun ResultScope<Measurement>.buildResult(): Measurement {
    return checkNotNull(transactionResult).copy {
      updateTime = commitTimestamp.toProto()
      etag = ETags.computeETag(commitTimestamp)
    }
  }
}
