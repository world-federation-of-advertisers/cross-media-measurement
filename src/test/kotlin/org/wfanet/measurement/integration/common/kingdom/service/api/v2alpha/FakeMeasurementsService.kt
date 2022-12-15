// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common.kingdom.service.api.v2alpha

import com.google.protobuf.duration
import io.grpc.Status
import java.util.concurrent.ConcurrentHashMap
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetMeasurementRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signResult

class FakeMeasurementsService(
  private val idGenerator: IdGenerator,
  private val edpSigningKeyHandle: SigningKeyHandle,
  private val dataProviderCertificateName: String,
) : MeasurementsCoroutineImplBase() {
  private val measurementsApiIdMap = ConcurrentHashMap<String, Measurement>()
  private val measurementsReferenceIdMap = ConcurrentHashMap<String, Measurement>()

  override suspend fun createMeasurement(request: CreateMeasurementRequest): Measurement {
    val referenceId = request.measurement.measurementReferenceId
    val existingMeasurement = measurementsReferenceIdMap[referenceId]
    if (existingMeasurement != null) {
      return existingMeasurement
    }

    val externalId = idGenerator.generateExternalId()
    val apiId = externalId.apiId.value

    val measurementConsumerCertificateKey =
      grpcRequireNotNull(
        MeasurementConsumerCertificateKey.fromName(
          request.measurement.measurementConsumerCertificate
        )
      ) {
        "Measurement Consumer Certificate resource name is either unspecified or invalid"
      }

    val measurement =
      request.measurement.copy {
        name =
          MeasurementKey(
              measurementConsumerId = measurementConsumerCertificateKey.measurementConsumerId,
              measurementId = apiId
            )
            .toName()
        state = Measurement.State.AWAITING_REQUISITION_FULFILLMENT
      }

    measurementsApiIdMap[apiId] = measurement
    if (referenceId.isNotBlank()) {
      measurementsReferenceIdMap[referenceId] = measurement
    }

    return measurement
  }

  override suspend fun getMeasurement(request: GetMeasurementRequest): Measurement {
    val key =
      grpcRequireNotNull(MeasurementKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val measurement =
      measurementsApiIdMap[key.measurementId]
        ?: failGrpc(Status.NOT_FOUND) { "Measurement not found" }

    val measurementSpec = MeasurementSpec.parseFrom(measurement.measurementSpec.data)
    val result =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (measurementSpec.measurementTypeCase) {
        MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY ->
          throw Status.UNIMPLEMENTED.asRuntimeException()
        MeasurementSpec.MeasurementTypeCase.IMPRESSION ->
          MeasurementKt.result { impression = MeasurementKt.ResultKt.impression { value = 100 } }
        MeasurementSpec.MeasurementTypeCase.DURATION ->
          MeasurementKt.result {
            watchDuration =
              MeasurementKt.ResultKt.watchDuration { value = duration { seconds = 100 } }
          }
        MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
          failGrpc(Status.INVALID_ARGUMENT) { "MeasurementSpec MeasurementType not set" }
      }

    val signedResult: SignedData = signResult(result, edpSigningKeyHandle)
    val encryptedResult =
      encryptResult(
        signedResult,
        EncryptionPublicKey.parseFrom(measurementSpec.measurementPublicKey)
      )

    return measurement.copy {
      results +=
        MeasurementKt.resultPair {
          certificate = dataProviderCertificateName
          this.encryptedResult = encryptedResult
        }
      state = Measurement.State.SUCCEEDED
    }
  }
}
