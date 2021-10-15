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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CreateMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.GetMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as MeasurementConsumersCoroutineService
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer as InternalMeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerKt.details
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.getMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.measurementConsumer as internalMeasurementConsumer

private val API_VERSION = Version.V2_ALPHA

class MeasurementConsumersService(
  private val internalClient: InternalMeasurementConsumersCoroutineStub
) : MeasurementConsumersCoroutineService() {

  override suspend fun createMeasurementConsumer(
    request: CreateMeasurementConsumerRequest
  ): MeasurementConsumer {
    val measurementConsumer = request.measurementConsumer

    grpcRequire(!measurementConsumer.publicKey.data.isEmpty) { "public_key.data is missing" }
    grpcRequire(!measurementConsumer.publicKey.signature.isEmpty) {
      "public_key.signature is missing"
    }

    val internalResponse: InternalMeasurementConsumer =
      internalClient.createMeasurementConsumer(
        internalMeasurementConsumer {
          certificate = parseCertificateDer(measurementConsumer.certificateDer)
          details =
            details {
              apiVersion = API_VERSION.string
              publicKey = measurementConsumer.publicKey.data
              publicKeySignature = measurementConsumer.publicKey.signature
            }
        }
        // TODO(world-federation-of-advertisers/cross-media-measurement#119): Add authenticated user
        // as owner.
        )
    return internalResponse.toMeasurementConsumer()
  }

  override suspend fun getMeasurementConsumer(
    request: GetMeasurementConsumerRequest
  ): MeasurementConsumer {
    val key: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }
    // TODO(world-federation-of-advertisers/cross-media-measurement#119): Pass credentials for
    // ownership check.
    val internalResponse: InternalMeasurementConsumer =
      internalClient.getMeasurementConsumer(
        getMeasurementConsumerRequest {
          externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
        }
      )
    return internalResponse.toMeasurementConsumer()
  }
}

private fun InternalMeasurementConsumer.toMeasurementConsumer(): MeasurementConsumer {
  check(Version.fromString(details.apiVersion) == API_VERSION) {
    "Incompatible API version ${details.apiVersion}"
  }
  val internalMeasurementConsumer = this
  val measurementConsumerId: String = externalIdToApiId(externalMeasurementConsumerId)
  val certificateId: String = externalIdToApiId(certificate.externalCertificateId)

  return measurementConsumer {
    name = MeasurementConsumerKey(measurementConsumerId).toName()
    certificate = MeasurementConsumerCertificateKey(measurementConsumerId, certificateId).toName()
    certificateDer = internalMeasurementConsumer.certificate.details.x509Der
    publicKey =
      signedData {
        data = internalMeasurementConsumer.details.publicKey
        signature = internalMeasurementConsumer.details.publicKeySignature
      }
  }
}
