// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.testing

import com.google.protobuf.kotlin.toByteString
import com.google.type.interval
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import kotlin.random.Random
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.honestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
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
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec

object Requisitions {

  val SECRET_FILES_PATH: Path =
    checkNotNull(
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )
    )

  fun loadSigningKey(certDerFileName: String, privateKeyDerFileName: String): SigningKeyHandle {
    return loadSigningKey(
      SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
      SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
    )
  }

  const val EDP_DISPLAY_NAME = "edp1"
  const val EDP_ID = "someDataProvider"
  const val EDP_NAME = "dataProviders/$EDP_ID"

  const val MC_ID = "mc"
  const val MC_NAME = "measurementConsumers/$MC_ID"

  const val DUCHY_ONE_ID = "worker1"
  const val DUCHY_TWO_ID = "worker2"

  val DUCHY_ONE_NAME = DuchyKey(DUCHY_ONE_ID).toName()
  val DUCHY_TWO_NAME = DuchyKey(DUCHY_TWO_ID).toName()

  val DUCHY_ONE_SIGNING_KEY =
    loadSigningKey("${DUCHY_ONE_ID}_cs_cert.der", "${DUCHY_ONE_ID}_cs_private.der")
  val DUCHY_TWO_SIGNING_KEY =
    loadSigningKey("${DUCHY_TWO_ID}_cs_cert.der", "${DUCHY_TWO_ID}_cs_private.der")

  val DUCHY_ONE_CERTIFICATE = certificate {
    name = DuchyCertificateKey(DUCHY_ONE_ID, externalIdToApiId(6L)).toName()
    x509Der = DUCHY_ONE_SIGNING_KEY.certificate.encoded.toByteString()
  }
  val DUCHY_TWO_CERTIFICATE = certificate {
    name = DuchyCertificateKey(DUCHY_TWO_ID, externalIdToApiId(6L)).toName()
    x509Der = DUCHY_TWO_SIGNING_KEY.certificate.encoded.toByteString()
  }

  val DUCHY1_ENCRYPTION_PUBLIC_KEY =
    loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile()).toEncryptionPublicKey()

  val EDP_SIGNING_KEY =
    loadSigningKey("${EDP_DISPLAY_NAME}_cs_cert.der", "${EDP_DISPLAY_NAME}_cs_private.der")
  val EDP_RESULT_SIGNING_KEY =
    loadSigningKey(
      "${EDP_DISPLAY_NAME}_result_cs_cert.der",
      "${EDP_DISPLAY_NAME}_result_cs_private.der",
    )
  val DATA_PROVIDER_CERTIFICATE_KEY = DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))
  val DATA_PROVIDER_RESULT_CERTIFICATE_KEY =
    DataProviderCertificateKey(EDP_ID, externalIdToApiId(9L))

  val DATA_PROVIDER_CERTIFICATE = certificate {
    name = DATA_PROVIDER_CERTIFICATE_KEY.toName()
    x509Der = EDP_SIGNING_KEY.certificate.encoded.toByteString()
    subjectKeyIdentifier = EDP_SIGNING_KEY.certificate.subjectKeyIdentifier!!
  }
  val DATA_PROVIDER_RESULT_CERTIFICATE = certificate {
    name = DATA_PROVIDER_RESULT_CERTIFICATE_KEY.toName()
    x509Der = EDP_RESULT_SIGNING_KEY.certificate.encoded.toByteString()
    subjectKeyIdentifier = EDP_RESULT_SIGNING_KEY.certificate.subjectKeyIdentifier!!
  }

  val DUCHY_ENTRY_ONE = duchyEntry {
    key = DUCHY_ONE_NAME
    value = value {
      duchyCertificate = DUCHY_ONE_CERTIFICATE.name
      honestMajorityShareShuffle = honestMajorityShareShuffle {
        publicKey = signEncryptionPublicKey(DUCHY1_ENCRYPTION_PUBLIC_KEY, DUCHY_ONE_SIGNING_KEY)
      }
    }
  }

  val DUCHY_ENTRY_TWO = duchyEntry {
    key = DUCHY_TWO_NAME
    value = value { duchyCertificate = DUCHY_TWO_CERTIFICATE.name }
  }

  const val MEASUREMENT_NAME = "$MC_NAME/measurements/BBBBBBBBBHs"

  const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
  const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
    "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
  val MEASUREMENT_CONSUMER_CERTIFICATE_DER =
    SECRET_FILES_PATH.resolve("mc_cs_cert.der").toFile().readByteString()
  val MEASUREMENT_CONSUMER_CERTIFICATE = certificate {
    name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
    x509Der = MEASUREMENT_CONSUMER_CERTIFICATE_DER
  }

  val MC_PUBLIC_KEY =
    loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile()).toEncryptionPublicKey()
  val MC_PRIVATE_KEY = loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
  val MC_SIGNING_KEY = loadSigningKey("${MC_ID}_cs_cert.der", "${MC_ID}_cs_private.der")

  val DATA_PROVIDER_PUBLIC_KEY =
    loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
      .toEncryptionPublicKey()

  const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
  val LAST_EVENT_DATE = LocalDate.now()!!
  val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)!!
  val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)
  val REQUISITION_SPEC = requisitionSpec {
    events =
      RequisitionSpecKt.events {
        eventGroups += eventGroupEntry {
          key = EVENT_GROUP_NAME
          value =
            RequisitionSpecKt.EventGroupEntryKt.value {
              collectionInterval = interval {
                startTime = TIME_RANGE.start.toProtoTime()
                endTime = TIME_RANGE.endExclusive.toProtoTime()
              }
              filter = eventFilter {
                expression =
                  "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE} && " +
                    "person.gender == ${Person.Gender.FEMALE_VALUE}"
              }
            }
        }
      }
    measurementPublicKey = MC_PUBLIC_KEY.pack()
    nonce = Random.Default.nextLong()
  }

  val OUTPUT_DP_PARAMS = differentialPrivacyParams {
    epsilon = 1.0
    delta = 1E-12
  }
  val RF_MEASUREMENT_SPEC = measurementSpec {
    measurementPublicKey = MC_PUBLIC_KEY.pack()
    reachAndFrequency = reachAndFrequency {
      reachPrivacyParams = OUTPUT_DP_PARAMS
      frequencyPrivacyParams = OUTPUT_DP_PARAMS
      maximumFrequency = 10
    }
    vidSamplingInterval = vidSamplingInterval {
      start = 0.0f
      width = 1.0f
    }
    nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
  }

  val ENCRYPTED_REQUISITION_SPEC =
    encryptRequisitionSpec(
      signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
      DATA_PROVIDER_PUBLIC_KEY,
    )

  val NOISE_MECHANISM = ProtocolConfig.NoiseMechanism.DISCRETE_GAUSSIAN

  val HMSS_REQUISITION = requisition {
    name = "${EDP_NAME}/requisitions/foo"
    measurement = MEASUREMENT_NAME
    state = Requisition.State.UNFULFILLED
    measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
    measurementSpec = signMeasurementSpec(RF_MEASUREMENT_SPEC, MC_SIGNING_KEY)
    encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
    protocolConfig = protocolConfig {
      protocols +=
        ProtocolConfigKt.protocol {
          honestMajorityShareShuffle =
            ProtocolConfigKt.honestMajorityShareShuffle {
              noiseMechanism = NOISE_MECHANISM
              ringModulus = 127
            }
        }
    }
    dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
    dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
    duchies += DUCHY_ENTRY_ONE
    duchies += DUCHY_ENTRY_TWO
  }
}
