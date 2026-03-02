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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.kotlin.unpack
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct.DirectMeasurementResultFactory
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.DirectMeasurementFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.HMShuffleMeasurementFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.MeasurementFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.TrusTeeMeasurementFulfiller
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee.FulfillRequisitionRequestBuilder as TrusteeFulfillRequisitionRequestBuilder

/**
 * Configuration for TrusTee protocol envelope encryption.
 *
 * @property kmsClient The KMS client for encryption operations.
 * @property workloadIdentityProvider The workload identity provider URL for GCP WIF.
 * @property impersonatedServiceAccount The service account to impersonate for KMS operations.
 */
data class TrusTeeConfig(
  val kmsClient: KmsClient,
  val workloadIdentityProvider: String,
  val impersonatedServiceAccount: String,
) {
  /**
   * Builds EncryptionParams for the TrusTee protocol using the provided KEK URI.
   *
   * If the kekUri is found in the kekUriToKeyNameMap, the output KEK URI will be constructed by
   * replacing the key name in the original URI with the mapped key name (on the same keyring). If
   * not found in the map, the original kekUri is used unchanged.
   *
   * @param kekUri The KEK URI from BlobDetails.encryptedDek.
   * @param kekUriToKeyNameMap Map from input KEK URI to output key name for re-encryption.
   * @return EncryptionParams for the TrusTee fulfillment request builder.
   */
  fun buildEncryptionParams(
    kekUri: String,
    kekUriToKeyNameMap: Map<String, String>,
  ): TrusteeFulfillRequisitionRequestBuilder.EncryptionParams {
    val remappedKekUri = remapKekUri(kekUri, kekUriToKeyNameMap)
    return TrusteeFulfillRequisitionRequestBuilder.EncryptionParams(
      kmsClient = kmsClient,
      kmsKekUri = remappedKekUri,
      workloadIdentityProvider = workloadIdentityProvider,
      impersonatedServiceAccount = impersonatedServiceAccount,
    )
  }

  /**
   * Remaps the input KEK URI using the provided map.
   *
   * If the kekUri is found in the map, constructs a new KEK URI by replacing the key name/ID
   * component with the mapped value (keeping the same keyring/account). Supports both GCP and AWS
   * KMS URI formats. If not found, returns the original kekUri unchanged.
   */
  private fun remapKekUri(kekUri: String, kekUriToKeyNameMap: Map<String, String>): String {
    val mappedKeyName = kekUriToKeyNameMap[kekUri] ?: return kekUri

    val gcpMatch = KmsConstants.GCP_KMS_KEY_URI_REGEX.matchEntire(kekUri)
    if (gcpMatch != null) {
      val (project, location, keyRing) = gcpMatch.destructured
      return "gcp-kms://projects/$project/locations/$location/keyRings/$keyRing/cryptoKeys/$mappedKeyName"
    }

    val awsMatch = KmsConstants.AWS_KMS_KEY_URI_REGEX.matchEntire(kekUri)
    if (awsMatch != null) {
      val (region, account) = awsMatch.destructured
      return "aws-kms://arn:aws:kms:$region:$account:key/$mappedKeyName"
    }

    return kekUri
  }
}

/**
 * Default implementation that routes requisitions to protocol-specific fulfillers.
 *
 * @param requisitionsStub gRPC stub for Direct protocol requisitions
 * @param requisitionFulfillmentStubMap duchy name → gRPC stub mapping for HM Shuffle and TrusTee
 * @param dataProviderCertificateKey EDP certificate identifier for result signing
 * @param dataProviderSigningKeyHandle cryptographic key for result authentication
 * @param noiserSelector strategy for selecting differential privacy mechanisms
 * @param kAnonymityParams optional k-anonymity thresholds; null disables k-anonymity
 * @param overrideImpressionMaxFrequencyPerUser optional frequency cap override; null or -1 means no
 *   capping and uses totalUncappedImpressions instead
 * @param trusTeeConfig configuration for TrusTee protocol; null disables TrusTee
 */
class DefaultFulfillerSelector(
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  private val requisitionFulfillmentStubMap:
    Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub>,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val dataProviderSigningKeyHandle: SigningKeyHandle,
  private val noiserSelector: NoiserSelector,
  private val kAnonymityParams: KAnonymityParams?,
  private val overrideImpressionMaxFrequencyPerUser: Int?,
  private val trusTeeConfig: TrusTeeConfig? = null,
  private val kekUriToKeyNameMap: Map<String, String> = emptyMap(),
) : FulfillerSelector {

  init {
    val keyNamePattern = Regex("[a-zA-Z0-9_-]{1,63}")
    for ((kekUri, keyName) in kekUriToKeyNameMap) {
      require(keyNamePattern.matches(keyName)) {
        "Invalid key name format in kekUriToKeyNameMap: '$keyName' for URI '$kekUri'. " +
          "Key name must match pattern [a-zA-Z0-9_-]{1,63}"
      }
    }
  }

  /**
   * Selects the appropriate fulfiller based on requisition protocol configuration.
   *
   * @param requisition requisition containing protocol configuration
   * @param measurementSpec measurement specification including DP parameters
   * @param requisitionSpec decrypted requisition details including nonce
   * @param frequencyVector frequency vector containing per-VID frequency counts
   * @param populationSpec population definition for VID range validation
   * @param kekUri the KEK URI from BlobDetails.encryptedDek for TrusTee encryption. Required if the
   *   frequencyVector is non-empty and the protocol is TrusTee.
   * @return protocol-specific fulfiller ready for execution
   * @throws IllegalArgumentException if no supported protocol is found
   */
  override suspend fun selectFulfiller(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec,
    frequencyVector: StripedByteFrequencyVector,
    populationSpec: PopulationSpec,
    kekUri: String?,
  ): MeasurementFulfiller {

    val frequencyDataBytes = frequencyVector.getByteArray()

    val vec =
      FrequencyVectorBuilder(
        populationSpec = populationSpec,
        measurementSpec = measurementSpec,
        frequencyDataBytes = frequencyDataBytes,
        strict = false,
        kAnonymityParams = kAnonymityParams,
        overrideImpressionMaxFrequencyPerUser = overrideImpressionMaxFrequencyPerUser,
      )

    return if (requisition.protocolConfig.protocolsList.any { it.hasDirect() }) {
      val totalUncappedImpressions = frequencyVector.getTotalUncappedImpressions()
      buildDirectMeasurementFulfiller(
        requisition = requisition,
        measurementSpec = measurementSpec,
        requisitionSpec = requisitionSpec,
        maxPopulation = null,
        frequencyData = vec.frequencyDataArray,
        kAnonymityParams = kAnonymityParams,
        totalUncappedImpressions = totalUncappedImpressions,
      )
    } else if (requisition.protocolConfig.protocolsList.any { it.hasTrusTee() }) {
      // Build TrusTee encryption params dynamically using the kekUri from BlobDetails.
      // If kekUri is not null, trusTeeConfig must be provided.
      // If kekUri is null, it implies there were no input blobs; verify no impressions exist.
      val trusTeeEncryptionParams =
        if (kekUri != null) {
          requireNotNull(trusTeeConfig) {
            "TrusTee protocol selected but trusTeeConfig is null. " +
              "TrusTeeConfig must be provided when impression data sources are available."
          }
          trusTeeConfig.buildEncryptionParams(kekUri, kekUriToKeyNameMap)
        } else {
          val totalUncappedImpressions = frequencyVector.getTotalUncappedImpressions()
          require(
            totalUncappedImpressions == 0L
          ) { // if no kekUri, then we don't know a valid project id to encrypt with so can only
            // fulfill an empty vector
            "TrusTee protocol selected with null kekUri but totalUncappedImpressions is $totalUncappedImpressions. " +
              "Expected 0 impressions when no data sources are available."
          }
          null
        }

      if (kAnonymityParams == null) {
        TrusTeeMeasurementFulfiller(
          requisition,
          requisitionSpec.nonce,
          vec.build(),
          requisitionFulfillmentStubMap,
          requisitionsStub,
          trusTeeEncryptionParams,
        )
      } else {
        TrusTeeMeasurementFulfiller.buildKAnonymized(
          requisition,
          requisitionSpec.nonce,
          measurementSpec,
          populationSpec,
          vec,
          requisitionFulfillmentStubMap,
          requisitionsStub,
          kAnonymityParams,
          maxPopulation = null,
          trusTeeEncryptionParams,
        )
      }
    } else if (
      requisition.protocolConfig.protocolsList.any { it.hasHonestMajorityShareShuffle() }
    ) {
      if (kAnonymityParams == null) {
        HMShuffleMeasurementFulfiller(
          requisition,
          requisitionSpec.nonce,
          vec.build(),
          dataProviderSigningKeyHandle,
          dataProviderCertificateKey,
          requisitionFulfillmentStubMap,
          requisitionsStub,
        )
      } else {
        HMShuffleMeasurementFulfiller.buildKAnonymized(
          requisition,
          requisitionSpec.nonce,
          measurementSpec,
          populationSpec,
          vec,
          dataProviderSigningKeyHandle,
          dataProviderCertificateKey,
          requisitionFulfillmentStubMap,
          requisitionsStub,
          kAnonymityParams,
          maxPopulation = null,
        )
      }
    } else {
      throw IllegalArgumentException("Protocol not supported for ${requisition.name}")
    }
  }

  /** Builds a Direct protocol fulfiller. */
  private suspend fun buildDirectMeasurementFulfiller(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec,
    maxPopulation: Int?,
    frequencyData: IntArray,
    kAnonymityParams: KAnonymityParams?,
    totalUncappedImpressions: Long,
  ): DirectMeasurementFulfiller {
    val measurementEncryptionPublicKey: EncryptionPublicKey =
      measurementSpec.measurementPublicKey.unpack()
    val directProtocolConfig =
      requisition.protocolConfig.protocolsList.first { it.hasDirect() }.direct
    val noiseMechanism =
      noiserSelector.selectNoiseMechanism(directProtocolConfig.noiseMechanismsList)

    val result =
      DirectMeasurementResultFactory.buildMeasurementResult(
        directProtocolConfig,
        noiseMechanism,
        measurementSpec,
        frequencyData,
        maxPopulation,
        kAnonymityParams = kAnonymityParams,
        impressionMaxFrequencyPerUser = overrideImpressionMaxFrequencyPerUser,
        totalUncappedImpressions = totalUncappedImpressions,
      )
    return DirectMeasurementFulfiller(
      requisition.name,
      requisition.dataProviderCertificate,
      result,
      requisitionSpec.nonce,
      measurementEncryptionPublicKey,
      directProtocolConfig,
      noiseMechanism,
      dataProviderSigningKeyHandle,
      dataProviderCertificateKey,
      requisitionsStub,
    )
  }
}
