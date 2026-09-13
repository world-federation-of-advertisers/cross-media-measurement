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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import org.wfanet.measurement.config.edpaggregator.ResultsFulfillerConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.TransportLayerSecurityParams

/** Converts static deployment configuration to versioned WorkItem application parameters. */
fun ResultsFulfillerConfig.toV1Alpha(): ResultsFulfillerParams {
  val source = this
  return ResultsFulfillerParams.newBuilder()
    .setDataProvider(dataProvider)
    .apply {
      if (source.hasStorageParams()) {
        storageParams =
          ResultsFulfillerParams.StorageParams.newBuilder()
            .setLabeledImpressionsBlobDetailsUriPrefix(
              source.storageParams.labeledImpressionsBlobDetailsUriPrefix
            )
            .setGcsProjectId(source.storageParams.gcsProjectId)
            .build()
      }
      if (source.hasConsentParams()) {
        consentParams =
          ResultsFulfillerParams.ConsentParams.newBuilder()
            .setResultCsCertDerResourcePath(source.consentParams.resultCsCertDerResourcePath)
            .setResultCsPrivateKeyDerResourcePath(
              source.consentParams.resultCsPrivateKeyDerResourcePath
            )
            .setPrivateEncryptionKeyResourcePath(
              source.consentParams.privateEncryptionKeyResourcePath
            )
            .setEdpCertificateName(source.consentParams.edpCertificateName)
            .build()
      }
      if (source.hasCmmsConnection()) {
        cmmsConnection =
          TransportLayerSecurityParams.newBuilder()
            .setClientCertResourcePath(source.cmmsConnection.clientCertResourcePath)
            .setClientPrivateKeyResourcePath(source.cmmsConnection.clientPrivateKeyResourcePath)
            .build()
      }
      if (source.hasNoiseParams()) {
        noiseParams =
          ResultsFulfillerParams.NoiseParams.newBuilder()
            .setNoiseTypeValue(source.noiseParams.noiseTypeValue)
            .build()
      }
      if (source.hasKAnonymityParams()) {
        kAnonymityParams =
          ResultsFulfillerParams.KAnonymityParams.newBuilder()
            .setMinImpressions(source.kAnonymityParams.minImpressions)
            .setMinUsers(source.kAnonymityParams.minUsers)
            .setReachMaxFrequencyPerUser(source.kAnonymityParams.reachMaxFrequencyPerUser)
            .build()
      }
      impressionMaxFrequencyPerUser = source.impressionMaxFrequencyPerUser
      putAllModelLineMap(source.modelLineMapMap)
      if (source.hasTrusteeParams()) {
        trusteeParams =
          ResultsFulfillerParams.TrusTeeParams.newBuilder()
            .putAllKekUriToKeyName(source.trusteeParams.kekUriToKeyNameMap)
            .build()
      }
      if (source.hasMultiPartyConfig()) {
        val multiPartyConfigBuilder = ResultsFulfillerParams.MultiPartyConfig.newBuilder()
        for (noiseType in source.multiPartyConfig.supportedNoiseTypesValueList) {
          multiPartyConfigBuilder.addSupportedNoiseTypesValue(noiseType)
        }
        multiPartyConfig = multiPartyConfigBuilder.build()
      }
      impressionCapModeValue = source.impressionCapModeValue
    }
    .build()
}
