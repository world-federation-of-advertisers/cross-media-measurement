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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.edpaggregator.ResultsFulfillerConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.TransportLayerSecurityParams

@RunWith(JUnit4::class)
class ResultsFulfillerConfigTest {

  @Test
  fun `toV1Alpha converts all fields`() {
    val config =
      ResultsFulfillerConfig.newBuilder()
        .setDataProvider("dataProviders/edp1")
        .setStorageParams(
          ResultsFulfillerConfig.StorageParams.newBuilder()
            .setLabeledImpressionsBlobDetailsUriPrefix("gs://bucket/impressions")
            .setGcsProjectId("project")
        )
        .setConsentParams(
          ResultsFulfillerConfig.ConsentParams.newBuilder()
            .setResultCsCertDerResourcePath("result-cert")
            .setResultCsPrivateKeyDerResourcePath("result-key")
            .setPrivateEncryptionKeyResourcePath("encryption-key")
            .setEdpCertificateName("dataProviders/edp1/certificates/cert1")
        )
        .setCmmsConnection(
          ResultsFulfillerConfig.CmmsConnectionParams.newBuilder()
            .setClientCertResourcePath("client-cert")
            .setClientPrivateKeyResourcePath("client-key")
        )
        .setNoiseParams(
          ResultsFulfillerConfig.NoiseParams.newBuilder()
            .setNoiseType(ResultsFulfillerConfig.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN)
        )
        .setKAnonymityParams(
          ResultsFulfillerConfig.KAnonymityParams.newBuilder()
            .setMinImpressions(10)
            .setMinUsers(5)
            .setReachMaxFrequencyPerUser(3)
        )
        .setImpressionMaxFrequencyPerUser(7)
        .putModelLineMap("external", "internal")
        .setTrusteeParams(
          ResultsFulfillerConfig.TrusTeeParams.newBuilder()
            .putKekUriToKeyName("gcp-kms://key", "trustee-key")
        )
        .setMultiPartyConfig(
          ResultsFulfillerConfig.MultiPartyConfig.newBuilder()
            .addSupportedNoiseTypes(ResultsFulfillerConfig.NoiseParams.NoiseType.NONE)
            .addSupportedNoiseTypes(
              ResultsFulfillerConfig.NoiseParams.NoiseType.DETERMINISTIC_TRUNCATED_LAPLACE
            )
        )
        .setImpressionCapMode(ResultsFulfillerConfig.ImpressionCapMode.CUSTOM_CAP)
        .build()

    val expected =
      ResultsFulfillerParams.newBuilder()
        .setDataProvider("dataProviders/edp1")
        .setStorageParams(
          ResultsFulfillerParams.StorageParams.newBuilder()
            .setLabeledImpressionsBlobDetailsUriPrefix("gs://bucket/impressions")
            .setGcsProjectId("project")
        )
        .setConsentParams(
          ResultsFulfillerParams.ConsentParams.newBuilder()
            .setResultCsCertDerResourcePath("result-cert")
            .setResultCsPrivateKeyDerResourcePath("result-key")
            .setPrivateEncryptionKeyResourcePath("encryption-key")
            .setEdpCertificateName("dataProviders/edp1/certificates/cert1")
        )
        .setCmmsConnection(
          TransportLayerSecurityParams.newBuilder()
            .setClientCertResourcePath("client-cert")
            .setClientPrivateKeyResourcePath("client-key")
        )
        .setNoiseParams(
          ResultsFulfillerParams.NoiseParams.newBuilder()
            .setNoiseType(ResultsFulfillerParams.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN)
        )
        .setKAnonymityParams(
          ResultsFulfillerParams.KAnonymityParams.newBuilder()
            .setMinImpressions(10)
            .setMinUsers(5)
            .setReachMaxFrequencyPerUser(3)
        )
        .setImpressionMaxFrequencyPerUser(7)
        .putModelLineMap("external", "internal")
        .setTrusteeParams(
          ResultsFulfillerParams.TrusTeeParams.newBuilder()
            .putKekUriToKeyName("gcp-kms://key", "trustee-key")
        )
        .setMultiPartyConfig(
          ResultsFulfillerParams.MultiPartyConfig.newBuilder()
            .addSupportedNoiseTypes(ResultsFulfillerParams.NoiseParams.NoiseType.NONE)
            .addSupportedNoiseTypes(
              ResultsFulfillerParams.NoiseParams.NoiseType.DETERMINISTIC_TRUNCATED_LAPLACE
            )
        )
        .setImpressionCapMode(ResultsFulfillerParams.ImpressionCapMode.CUSTOM_CAP)
        .build()

    assertThat(config.toV1Alpha()).isEqualTo(expected)
  }
}
