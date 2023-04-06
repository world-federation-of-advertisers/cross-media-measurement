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

package org.wfanet.measurement.integration.common

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import java.nio.file.Path
import java.nio.file.Paths
import java.security.cert.X509Certificate
import java.time.Instant
import org.jetbrains.annotations.Blocking
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.internal.kingdom.DuchyIdConfig
import org.wfanet.measurement.internal.kingdom.Llv2ProtocolConfigConfig
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent

private val SECRET_FILES_PATH: Path =
  checkNotNull(
    getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )
  )

private const val TEMPLATE_PREFIX = "wfa.measurement.api.v2alpha.event_templates.testing"

val DUCHY_ID_CONFIG: DuchyIdConfig =
  loadTextProto("duchy_id_config.textproto", DuchyIdConfig.getDefaultInstance())
val AGGREGATOR_PROTOCOLS_SETUP_CONFIG: ProtocolsSetupConfig =
  loadTextProto(
    "aggregator_protocols_setup_config.textproto",
    ProtocolsSetupConfig.getDefaultInstance()
  )
val NON_AGGREGATOR_PROTOCOLS_SETUP_CONFIG: ProtocolsSetupConfig =
  loadTextProto(
    "non_aggregator_protocols_setup_config.textproto",
    ProtocolsSetupConfig.getDefaultInstance()
  )
val LLV2_PROTOCOL_CONFIG_CONFIG: Llv2ProtocolConfigConfig =
  loadTextProto(
    "llv2_protocol_config_config.textproto",
    Llv2ProtocolConfigConfig.getDefaultInstance()
  )
val LLV2_AGGREGATOR_NAME =
  AGGREGATOR_PROTOCOLS_SETUP_CONFIG.liquidLegionsV2.externalAggregatorDuchyId!!

val ALL_DUCHY_NAMES = DUCHY_ID_CONFIG.duchiesList.map { it.externalDuchyId }
val ALL_DUCHIES =
  DUCHY_ID_CONFIG.duchiesList.map { duchy ->
    val activeEndTime =
      if (duchy.hasActiveEndTime()) {
        duchy.activeEndTime.toInstant()
      } else {
        Instant.MAX
      }
    DuchyIds.Entry(
      duchy.internalDuchyId,
      duchy.externalDuchyId,
      duchy.activeStartTime.toInstant()..activeEndTime
    )
  }
val ALL_EDP_DISPLAY_NAMES = listOf("edp1", "edp2", "edp3")

/**
 * Values of this map are anded to create the event filter to be sent to the EDPs.
 *
 * For purposes of this simulation, all of the EDPs register the same templates and receive the same
 * filter from the MC.
 */
val EVENT_TEMPLATES_TO_FILTERS_MAP =
  mapOf(
    "$TEMPLATE_PREFIX.Video" to "video_ad.viewed_fraction.value > 0.25",
    "$TEMPLATE_PREFIX.Person" to "person.gender.value == $TEMPLATE_PREFIX.Person.Gender.MALE"
  )
const val MC_DISPLAY_NAME = "mc"

@Blocking
fun <T : Message> loadTextProto(fileName: String, default: T): T {
  return parseTextProto(SECRET_FILES_PATH.resolve(fileName).toFile(), default)
}

@Blocking
fun loadTestCertDerFile(fileName: String): ByteString {
  return SECRET_FILES_PATH.resolve(fileName).toFile().readByteString()
}

@Blocking
fun loadTestCertCollection(fileName: String): Collection<X509Certificate> =
  readCertificateCollection(SECRET_FILES_PATH.resolve(fileName).toFile())

@Blocking
fun loadSigningKey(certDerFileName: String, privateKeyDerFileName: String): SigningKeyHandle {
  return loadSigningKey(
    SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
    SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile()
  )
}

@Blocking
fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
  return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
}

@Blocking
fun loadEncryptionPublicKey(fileName: String): TinkPublicKeyHandle {
  return loadPublicKey(SECRET_FILES_PATH.resolve(fileName).toFile())
}

/** Builds a [EntityContent] for the entity with a certain [displayName]. */
@Blocking
fun createEntityContent(displayName: String) =
  EntityContent(
    displayName = displayName,
    encryptionPublicKey =
      loadEncryptionPublicKey("${displayName}_enc_public.tink").toEncryptionPublicKey(),
    signingKey = loadSigningKey("${displayName}_cs_cert.der", "${displayName}_cs_private.der")
  )
