/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.requisitionfetcher.testing

import com.google.protobuf.kotlin.toByteString
import com.google.type.interval
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import kotlin.random.Random
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.dataprovider.DataProviderData

/** Test requisition data used for testing different scenarios. */
object TestRequisitionData {
  private const val MC_ID = "mc"
  private const val MC_NAME = "measurementConsumers/$MC_ID"
  private const val EDP_DISPLAY_NAME = "edp1"
  private val SECRET_FILES_PATH: Path =
    checkNotNull(
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )
    )
  private const val EDP_ID = "someDataProvider"
  const val EDP_NAME = "dataProviders/$EDP_ID"

  private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
  private const val MEASUREMENT_NAME = "$MC_NAME/measurements/BBBBBBBBBHs"
  private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
    "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"

  private val LAST_EVENT_DATE = LocalDate.now()
  private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)

  val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

  val MC_SIGNING_KEY = loadSigningKey("${MC_ID}_cs_cert.der", "${MC_ID}_cs_private.der")

  private val EDP_SIGNING_KEY =
    loadSigningKey("${EDP_DISPLAY_NAME}_cs_cert.der", "${EDP_DISPLAY_NAME}_cs_private.der")
  private val EDP_RESULT_SIGNING_KEY =
    loadSigningKey(
      "${EDP_DISPLAY_NAME}_result_cs_cert.der",
      "${EDP_DISPLAY_NAME}_result_cs_private.der",
    )
  private val DATA_PROVIDER_CERTIFICATE_KEY =
    DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))
  private val DATA_PROVIDER_RESULT_CERTIFICATE_KEY =
    DataProviderCertificateKey(EDP_ID, externalIdToApiId(9L))

  private val DATA_PROVIDER_CERTIFICATE = certificate {
    name = DATA_PROVIDER_CERTIFICATE_KEY.toName()
    x509Der = EDP_SIGNING_KEY.certificate.encoded.toByteString()
    subjectKeyIdentifier = EDP_SIGNING_KEY.certificate.subjectKeyIdentifier!!
  }

  val EDP_DATA =
    DataProviderData(
      EDP_NAME,
      loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink"),
      EDP_RESULT_SIGNING_KEY,
      DATA_PROVIDER_RESULT_CERTIFICATE_KEY,
    )

  private val MC_PUBLIC_KEY =
    loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile()).toEncryptionPublicKey()
  val DATA_PROVIDER_PUBLIC_KEY =
    loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
      .toEncryptionPublicKey()

  const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
  val REQUISITION_SPEC = requisitionSpec {
    events =
      RequisitionSpecKt.events {
        eventGroups +=
          RequisitionSpecKt.eventGroupEntry {
            key = EVENT_GROUP_NAME
            value =
              RequisitionSpecKt.EventGroupEntryKt.value {
                collectionInterval = interval {
                  startTime = TIME_RANGE.start.toProtoTime()
                  endTime = TIME_RANGE.endExclusive.toProtoTime()
                }
                filter =
                  RequisitionSpecKt.eventFilter {
                    expression =
                      "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE} && " +
                        "person.gender == ${Person.Gender.FEMALE_VALUE}"
                  }
              }
          }
      }
    measurementPublicKey = MC_PUBLIC_KEY.pack()
    nonce = Random.nextLong()
  }

  private val ENCRYPTED_REQUISITION_SPEC =
    encryptRequisitionSpec(
      signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
      DATA_PROVIDER_PUBLIC_KEY,
    )

  private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
    epsilon = 1.0
    delta = 1E-12
  }
  val MEASUREMENT_SPEC = measurementSpec {
    measurementPublicKey = MC_PUBLIC_KEY.pack()
    reachAndFrequency =
      MeasurementSpecKt.reachAndFrequency {
        reachPrivacyParams = OUTPUT_DP_PARAMS
        frequencyPrivacyParams = OUTPUT_DP_PARAMS
        maximumFrequency = 10
      }
    vidSamplingInterval =
      MeasurementSpecKt.vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
    nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
    reportingMetadata = MeasurementSpecKt.reportingMetadata { report = "some-report" }
    modelLine = "some-model-line"
  }

  val REQUISITION = requisition {
    name = "${EDP_NAME}/requisitions/foo"
    measurement = MEASUREMENT_NAME
    state = Requisition.State.UNFULFILLED
    measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
    measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
    encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
    protocolConfig = protocolConfig {
      protocols += ProtocolConfigKt.protocol { direct = ProtocolConfigKt.direct {} }
    }
    dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
    dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
  }

  private fun loadSigningKey(
    certDerFileName: String,
    privateKeyDerFileName: String,
  ): SigningKeyHandle {
    return org.wfanet.measurement.common.crypto.testing.loadSigningKey(
      SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
      SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
    )
  }

  private fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
    return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
  }
}
