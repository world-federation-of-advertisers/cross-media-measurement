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
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.config.access.PermissionsConfig
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig
import org.wfanet.measurement.config.securecomputation.QueuesConfig
import org.wfanet.measurement.config.securecomputation.WatchedPath
import org.wfanet.measurement.config.securecomputation.WatchedPathKt
import org.wfanet.measurement.config.securecomputation.watchedPath
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.resultsFulfillerParams
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.internal.kingdom.DuchyIdConfig
import org.wfanet.measurement.internal.kingdom.HmssProtocolConfigConfig
import org.wfanet.measurement.internal.kingdom.Llv2ProtocolConfigConfig
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping

private const val REPO_NAME = "wfa_measurement_system"

val SECRET_FILES_PATH: Path =
  checkNotNull(getRuntimePath(Paths.get(REPO_NAME, "src", "main", "k8s", "testing", "secretfiles")))

val AGGREGATOR_PROTOCOLS_SETUP_CONFIG: ProtocolsSetupConfig =
  loadTextProto(
    "aggregator_protocols_setup_config.textproto",
    ProtocolsSetupConfig.getDefaultInstance(),
  )
val WORKER1_PROTOCOLS_SETUP_CONFIG: ProtocolsSetupConfig =
  loadTextProto(
    "worker1_protocols_setup_config.textproto",
    ProtocolsSetupConfig.getDefaultInstance(),
  )
val WORKER2_PROTOCOLS_SETUP_CONFIG: ProtocolsSetupConfig =
  loadTextProto(
    "worker2_protocols_setup_config.textproto",
    ProtocolsSetupConfig.getDefaultInstance(),
  )

val LLV2_PROTOCOL_CONFIG_CONFIG: Llv2ProtocolConfigConfig =
  loadTextProto(
    "llv2_protocol_config_config.textproto",
    Llv2ProtocolConfigConfig.getDefaultInstance(),
  )
val RO_LLV2_PROTOCOL_CONFIG_CONFIG: Llv2ProtocolConfigConfig =
  loadTextProto(
    "ro_llv2_protocol_config_config.textproto",
    Llv2ProtocolConfigConfig.getDefaultInstance(),
  )
val HMSS_PROTOCOL_CONFIG_CONFIG: HmssProtocolConfigConfig =
  loadTextProto(
    "hmss_protocol_config_config.textproto",
    HmssProtocolConfigConfig.getDefaultInstance(),
  )
val IMPRESSION_QUALIFICATION_FILTER_CONFIG: ImpressionQualificationFilterConfig =
  loadTextProto(
    "impression_qualification_filter_config.textproto",
    ImpressionQualificationFilterConfig.getDefaultInstance(),
  )
val IMPRESSION_QUALIFICATION_FILTER_MAPPING: ImpressionQualificationFilterMapping =
  ImpressionQualificationFilterMapping(
    IMPRESSION_QUALIFICATION_FILTER_CONFIG,
    TestEvent.getDescriptor(),
  )

val AGGREGATOR_NAME =
  AGGREGATOR_PROTOCOLS_SETUP_CONFIG.honestMajorityShareShuffle.aggregatorDuchyId!!
val WORKER1_NAME =
  AGGREGATOR_PROTOCOLS_SETUP_CONFIG.honestMajorityShareShuffle.firstNonAggregatorDuchyId!!
val WORKER2_NAME =
  AGGREGATOR_PROTOCOLS_SETUP_CONFIG.honestMajorityShareShuffle.secondNonAggregatorDuchyId!!

val DUCHY_ID_CONFIG: DuchyIdConfig =
  loadTextProto("duchy_id_config.textproto", DuchyIdConfig.getDefaultInstance())
val ALL_DUCHY_NAMES =
  DUCHY_ID_CONFIG.duchiesList
    .map { it.externalDuchyId }
    .also { check(it.containsAll(listOf(AGGREGATOR_NAME, WORKER1_NAME, WORKER2_NAME))) }
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
      duchy.activeStartTime.toInstant()..activeEndTime,
    )
  }

val PERMISSIONS_CONFIG: PermissionsConfig =
  parseTextProto(
    checkNotNull(
        getRuntimePath(
          Paths.get(
            REPO_NAME,
            "src",
            "main",
            "proto",
            "wfa",
            "measurement",
            "reporting",
            "v2alpha",
            "permissions_config.textproto",
          )
        )
      )
      .toFile(),
    PermissionsConfig.getDefaultInstance(),
  )

val ALL_EDP_WITH_HMSS_CAPABILITIES_DISPLAY_NAMES = listOf("edp1", "edp3")
val ALL_EDP_WITHOUT_HMSS_CAPABILITIES_DISPLAY_NAMES = listOf("edp2")
val ALL_EDP_DISPLAY_NAMES =
  ALL_EDP_WITH_HMSS_CAPABILITIES_DISPLAY_NAMES + ALL_EDP_WITHOUT_HMSS_CAPABILITIES_DISPLAY_NAMES

const val DUCHY_MILL_PARALLELISM = 3

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
    SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
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
    signingKey = loadSigningKey("${displayName}_cs_cert.der", "${displayName}_cs_private.der"),
  )

/** Used to configure Secure Computation Control Plane */
const val PROJECT_ID = "some-project-id"
const val SUBSCRIPTION_ID = "some-subscription-id"
const val FULFILLER_TOPIC_ID = "results-fulfiller-queue"
val QUEUES_CONFIG: QueuesConfig
  get() {
    val configPath =
      Paths.get(
        REPO_NAME,
        "src",
        "main",
        "proto",
        "wfa",
        "measurement",
        "securecomputation",
        "controlplane",
        "v1alpha",
      )
    val configFile = getRuntimePath(configPath.resolve("queues_config.textproto"))!!.toFile()
    return parseTextProto(configFile, QueuesConfig.getDefaultInstance())
  }

/* Returns [ResultFulfillerParams] for a given test EDP. */
fun getResultsFulfillerParams(
  edpDisplayName: String,
  edpResourceName: String,
  edpCertificateKey: DataProviderCertificateKey,
  labeledImpressionBlobUriPrefix: String,
  noiseType: ResultsFulfillerParams.NoiseParams.NoiseType,
): ResultsFulfillerParams {
  return resultsFulfillerParams {
    this.dataProvider = edpResourceName
    this.storageParams =
      ResultsFulfillerParamsKt.storageParams {
        this.labeledImpressionsBlobDetailsUriPrefix = labeledImpressionBlobUriPrefix
      }
    this.cmmsConnection =
      ResultsFulfillerParamsKt.transportLayerSecurityParams {
        clientCertResourcePath = SECRET_FILES_PATH.resolve("${edpDisplayName}_tls.pem").toString()
        clientPrivateKeyResourcePath =
          SECRET_FILES_PATH.resolve("${edpDisplayName}_tls.key").toString()
      }
    this.consentParams =
      ResultsFulfillerParamsKt.consentParams {
        resultCsCertDerResourcePath =
          SECRET_FILES_PATH.resolve("${edpDisplayName}_cs_cert.der").toString()
        resultCsPrivateKeyDerResourcePath =
          SECRET_FILES_PATH.resolve("${edpDisplayName}_cs_private.der").toString()
        privateEncryptionKeyResourcePath =
          SECRET_FILES_PATH.resolve("${edpDisplayName}_enc_private.tink").toString()
        edpCertificateName = edpCertificateKey.toName()
      }
    this.noiseParams = ResultsFulfillerParamsKt.noiseParams { this.noiseType = noiseType }
  }
}

/* Used to construct the Watched Path to initiate Result Fulfiller Work to the Control Plane. */
fun getDataWatcherResultFulfillerParamsConfig(
  blobPrefix: String,
  edpResultFulfillerConfigs: Map<String, ResultsFulfillerParams>,
): List<WatchedPath> {
  return edpResultFulfillerConfigs
    .map { (edpName, params) ->
      listOf(
        watchedPath {
          sourcePathRegex = "$blobPrefix-$edpName/(.*)"
          this.controlPlaneQueueSink =
            WatchedPathKt.controlPlaneQueueSink {
              queue = FULFILLER_TOPIC_ID
              appParams = params.pack()
            }
        }
      )
    }
    .flatten()
}

/* Returns the [PrivateKeyHandle] for a test EDP. */
fun getDataProviderPrivateEncryptionKey(edpShortName: String): PrivateKeyHandle {
  val privateKeyHandleFile =
    checkNotNull(
        getRuntimePath(
          Paths.get(SECRET_FILES_PATH.resolve("${edpShortName}_enc_private.tink").toString())
        )
      )
      .toFile()
  return loadPrivateKey(privateKeyHandleFile)
}
