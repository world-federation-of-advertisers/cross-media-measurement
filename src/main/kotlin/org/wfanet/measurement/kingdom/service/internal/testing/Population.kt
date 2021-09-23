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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerKt
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementConsumer
import org.wfanet.measurement.internal.kingdom.protocolConfig

private const val API_VERSION = "v2alpha"

class Population(val clock: Clock, val idGenerator: IdGenerator) {
  fun buildRequestCertificate(derUtf8: String, skidUtf8: String, notValidBefore: Instant) =
      certificate {
    fillRequestCertificate(derUtf8, skidUtf8, notValidBefore)
  }

  private fun CertificateKt.Dsl.fillRequestCertificate(
    derUtf8: String,
    skidUtf8: String,
    notValidBefore: Instant
  ) {
    this.notValidBefore = notValidBefore.toProtoTime()
    notValidAfter = notValidBefore.plus(365L, ChronoUnit.DAYS).toProtoTime()
    subjectKeyIdentifier = ByteString.copyFromUtf8(skidUtf8)
    details = CertificateKt.details { x509Der = ByteString.copyFromUtf8(derUtf8) }
  }

  suspend fun createMeasurementConsumer(
    measurementConsumersService: MeasurementConsumersCoroutineImplBase
  ): MeasurementConsumer {
    return measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        certificate =
          buildRequestCertificate(
            "MC cert",
            "MC SKID " + idGenerator.generateExternalId().value,
            clock.instant()
          )
        details =
          MeasurementConsumerKt.details {
            apiVersion = API_VERSION
            publicKey = ByteString.copyFromUtf8("MC public key")
            publicKeySignature = ByteString.copyFromUtf8("MC public key signature")
          }
      }
    )
  }

  suspend fun createDataProvider(
    dataProvidersService: DataProvidersCoroutineImplBase
  ): DataProvider {
    return dataProvidersService.createDataProvider(
      dataProvider {
        certificate =
          buildRequestCertificate(
            "EDP cert",
            "EDP SKID " + idGenerator.generateExternalId().value,
            clock.instant()
          )
        details =
          DataProviderKt.details {
            apiVersion = API_VERSION
            publicKey = ByteString.copyFromUtf8("EDP public key")
            publicKeySignature = ByteString.copyFromUtf8("EDP public key signature")
          }
      }
    )
  }

  suspend fun createMeasurement(
    measurementsService: MeasurementsCoroutineImplBase,
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    vararg dataProviders: DataProvider
  ): Measurement {
    return measurementsService.createMeasurement(
      measurement {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        this.providedMeasurementId = providedMeasurementId
        externalMeasurementConsumerCertificateId =
          measurementConsumer.certificate.externalCertificateId
        details =
          MeasurementKt.details {
            apiVersion = API_VERSION
            measurementSpec = ByteString.copyFromUtf8("MeasurementSpec")
            measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
            dataProviderList = ByteString.copyFromUtf8("EDP list")
            dataProviderListSalt = ByteString.copyFromUtf8("EDP list salt")
            duchyProtocolConfig =
              duchyProtocolConfig {
                liquidLegionsV2 = DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance()
              }
            protocolConfig =
              protocolConfig {
                liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
              }
          }
        for (dataProvider in dataProviders) {
          this.dataProviders.put(
            dataProvider.externalDataProviderId,
            dataProviderValue {
              externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
              dataProviderPublicKey = dataProvider.details.publicKey
              dataProviderPublicKeySignature = dataProvider.details.publicKeySignature
              encryptedRequisitionSpec = ByteString.copyFromUtf8("Encrypted RequisitionSpec")
            }
          )
        }
      }
    )
  }

  suspend fun createDuchyCertificate(
    certificatesService: CertificatesCoroutineImplBase,
    externalDuchyId: String
  ): Certificate {
    return certificatesService.createCertificate(
      certificate {
        this.externalDuchyId = externalDuchyId
        fillRequestCertificate("Duchy cert", "Duchy $externalDuchyId SKID", clock.instant())
      }
    )
  }
}
