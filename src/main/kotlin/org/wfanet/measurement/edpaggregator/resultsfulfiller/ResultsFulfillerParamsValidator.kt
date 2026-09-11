/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.ImpressionCapMode
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.NoiseParams.NoiseType

/** Validates configuration carried in a ResultsFulfiller WorkItem. */
object ResultsFulfillerParamsValidator {
  fun validate(params: ResultsFulfillerParams, expectedDataProvider: String? = null) {
    val dataProviderKey =
      requireNotNull(DataProviderKey.fromName(params.dataProvider)) {
        "Invalid 'data_provider' in results_fulfiller_params: ${params.dataProvider}"
      }
    if (expectedDataProvider != null) {
      require(params.dataProvider == expectedDataProvider) {
        "results_fulfiller_params.data_provider must match the enclosing data provider: " +
          "expected $expectedDataProvider, got ${params.dataProvider}"
      }
    }

    require(params.hasStorageParams()) {
      "Missing 'storage_params' in results_fulfiller_params for ${params.dataProvider}."
    }
    require(params.storageParams.labeledImpressionsBlobDetailsUriPrefix.isNotBlank()) {
      "Missing 'labeled_impressions_blob_details_uri_prefix' in " +
        "results_fulfiller_params.storage_params for ${params.dataProvider}."
    }

    require(params.hasConsentParams()) {
      "Missing 'consent_params' in results_fulfiller_params for ${params.dataProvider}."
    }
    val consentParams = params.consentParams
    require(consentParams.resultCsCertDerResourcePath.isNotBlank()) {
      "Missing 'result_cs_cert_der_resource_path' in results_fulfiller_params.consent_params " +
        "for ${params.dataProvider}."
    }
    require(consentParams.resultCsPrivateKeyDerResourcePath.isNotBlank()) {
      "Missing 'result_cs_private_key_der_resource_path' in " +
        "results_fulfiller_params.consent_params for ${params.dataProvider}."
    }
    require(consentParams.privateEncryptionKeyResourcePath.isNotBlank()) {
      "Missing 'private_encryption_key_resource_path' in " +
        "results_fulfiller_params.consent_params for ${params.dataProvider}."
    }
    val certificateKey =
      requireNotNull(DataProviderCertificateKey.fromName(consentParams.edpCertificateName)) {
        "Invalid 'edp_certificate_name' in results_fulfiller_params.consent_params: " +
          consentParams.edpCertificateName
      }
    require(certificateKey.dataProviderId == dataProviderKey.dataProviderId) {
      "results_fulfiller_params.consent_params.edp_certificate_name must belong to " +
        "${params.dataProvider}."
    }

    require(params.hasCmmsConnection()) {
      "Missing 'cmms_connection' in results_fulfiller_params for ${params.dataProvider}."
    }
    require(params.cmmsConnection.clientCertResourcePath.isNotBlank()) {
      "Missing 'client_cert_resource_path' in results_fulfiller_params.cmms_connection for " +
        "${params.dataProvider}."
    }
    require(params.cmmsConnection.clientPrivateKeyResourcePath.isNotBlank()) {
      "Missing 'client_private_key_resource_path' in results_fulfiller_params.cmms_connection " +
        "for ${params.dataProvider}."
    }

    require(params.hasNoiseParams()) {
      "Missing 'noise_params' in results_fulfiller_params for ${params.dataProvider}."
    }
    require(params.noiseParams.noiseType.isSupported()) {
      "Unsupported noise type in results_fulfiller_params: ${params.noiseParams.noiseType}"
    }

    if (params.hasKAnonymityParams()) {
      require(params.kAnonymityParams.minUsers > 0) {
        "Result minimum thresholds minUsers must be greater than 0, got " +
          params.kAnonymityParams.minUsers
      }
      require(params.kAnonymityParams.minImpressions > 0) {
        "Result minimum thresholds minImpressions must be greater than 0, got " +
          params.kAnonymityParams.minImpressions
      }
      require(params.kAnonymityParams.reachMaxFrequencyPerUser > 0) {
        "Result minimum thresholds reachMaxFrequencyPerUser must be greater than 0, got " +
          params.kAnonymityParams.reachMaxFrequencyPerUser
      }
    }

    require(
      params.impressionMaxFrequencyPerUser >= -1 &&
        params.impressionMaxFrequencyPerUser <= Byte.MAX_VALUE
    ) {
      "impressionMaxFrequencyPerUser must be between -1 and ${Byte.MAX_VALUE}, got " +
        params.impressionMaxFrequencyPerUser
    }
    requireCapMatchesMode(params.impressionCapMode, params.impressionMaxFrequencyPerUser)

    require(params.multiPartyConfig.supportedNoiseTypesList.all { it.isSupported() }) {
      "Unsupported multi-party noise type in results_fulfiller_params"
    }
  }

  private fun NoiseType.isSupported(): Boolean {
    return this != NoiseType.UNSPECIFIED && this != NoiseType.UNRECOGNIZED
  }
}

/**
 * Throws if [configuredCap] and [impressionCapMode] disagree.
 *
 * Only [ImpressionCapMode.CUSTOM_CAP] reads `impression_max_frequency_per_user`, and it requires a
 * positive value. Every other explicit mode carries the choice itself, so a configured cap there is
 * an operator setting a value that would be silently ignored. [ImpressionCapMode.UNSPECIFIED] is
 * exempt: reading the field is what it is for.
 */
fun requireCapMatchesMode(impressionCapMode: ImpressionCapMode, configuredCap: Int) {
  when (impressionCapMode) {
    ImpressionCapMode.CUSTOM_CAP ->
      require(configuredCap > 0) {
        "impression_max_frequency_per_user must be greater than zero under CUSTOM_CAP, got " +
          "$configuredCap"
      }
    ImpressionCapMode.UNCAPPED,
    ImpressionCapMode.USE_MEASUREMENT_SPEC_CAP,
    ImpressionCapMode.DYNAMIC ->
      require(configuredCap == 0) {
        "impression_max_frequency_per_user is ignored under $impressionCapMode and must be unset, " +
          "got $configuredCap"
      }
    ImpressionCapMode.UNSPECIFIED,
    ImpressionCapMode.UNRECOGNIZED -> {}
  }
}
