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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.NoiseParams.NoiseType
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.resultsFulfillerParams

@RunWith(JUnit4::class)
class ResultsFulfillerParamsValidatorTest {
  @Test
  fun `valid params pass validation`() {
    ResultsFulfillerParamsValidator.validate(VALID_PARAMS, DATA_PROVIDER)
  }

  @Test
  fun `data provider must match enclosing config`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ResultsFulfillerParamsValidator.validate(VALID_PARAMS, "dataProviders/other")
      }

    assertThat(exception).hasMessageThat().contains("must match the enclosing data provider")
  }

  @Test
  fun `data provider must be a valid resource name`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ResultsFulfillerParamsValidator.validate(VALID_PARAMS.copy { dataProvider = "edp1" })
      }

    assertThat(exception).hasMessageThat().contains("Invalid 'data_provider'")
  }

  @Test
  fun `required nested fields are validated`() {
    val invalidParams =
      VALID_PARAMS.copy { storageParams = storageParams.toBuilder().clear().build() }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ResultsFulfillerParamsValidator.validate(invalidParams)
      }

    assertThat(exception).hasMessageThat().contains("labeled_impressions_blob_details_uri_prefix")
  }

  @Test
  fun `consent params are required`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ResultsFulfillerParamsValidator.validate(
          VALID_PARAMS.toBuilder().clearConsentParams().build()
        )
      }

    assertThat(exception).hasMessageThat().contains("Missing 'consent_params'")
  }

  @Test
  fun `certificate must belong to configured data provider`() {
    val invalidParams =
      VALID_PARAMS.copy {
        consentParams =
          consentParams
            .toBuilder()
            .setEdpCertificateName("dataProviders/other/certificates/cert1")
            .build()
      }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ResultsFulfillerParamsValidator.validate(invalidParams)
      }

    assertThat(exception).hasMessageThat().contains("must belong to $DATA_PROVIDER")
  }

  @Test
  fun `CMMS connection is required`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ResultsFulfillerParamsValidator.validate(
          VALID_PARAMS.toBuilder().clearCmmsConnection().build()
        )
      }

    assertThat(exception).hasMessageThat().contains("Missing 'cmms_connection'")
  }

  @Test
  fun `unspecified noise type is rejected`() {
    val invalidParams = VALID_PARAMS.copy { noiseParams = noiseParams.toBuilder().clear().build() }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ResultsFulfillerParamsValidator.validate(invalidParams)
      }

    assertThat(exception).hasMessageThat().contains("Unsupported noise type")
  }

  @Test
  fun `every k-anonymity threshold must be positive`() {
    val invalidThresholds =
      listOf(
        ResultsFulfillerParams.KAnonymityParams.newBuilder()
          .setMinUsers(0)
          .setMinImpressions(1)
          .setReachMaxFrequencyPerUser(1)
          .build() to "minUsers",
        ResultsFulfillerParams.KAnonymityParams.newBuilder()
          .setMinUsers(1)
          .setMinImpressions(0)
          .setReachMaxFrequencyPerUser(1)
          .build() to "minImpressions",
        ResultsFulfillerParams.KAnonymityParams.newBuilder()
          .setMinUsers(1)
          .setMinImpressions(1)
          .setReachMaxFrequencyPerUser(0)
          .build() to "reachMaxFrequencyPerUser",
      )

    for ((thresholds, fieldName) in invalidThresholds) {
      val exception =
        assertFailsWith<IllegalArgumentException> {
          ResultsFulfillerParamsValidator.validate(
            VALID_PARAMS.copy { kAnonymityParams = thresholds }
          )
        }

      assertThat(exception).hasMessageThat().contains(fieldName)
    }
  }

  @Test
  fun `impression cap must fit supported byte range`() {
    for (invalidCap in listOf(-2, Byte.MAX_VALUE.toInt() + 1)) {
      val exception =
        assertFailsWith<IllegalArgumentException> {
          ResultsFulfillerParamsValidator.validate(
            VALID_PARAMS.copy { impressionMaxFrequencyPerUser = invalidCap }
          )
        }

      assertThat(exception).hasMessageThat().contains("must be between -1 and ${Byte.MAX_VALUE}")
    }
  }

  @Test
  fun `impression cap must agree with explicit mode`() {
    val customWithoutCap =
      VALID_PARAMS.copy {
        impressionCapMode = ResultsFulfillerParams.ImpressionCapMode.CUSTOM_CAP
        impressionMaxFrequencyPerUser = 0
      }
    val uncappedWithCap =
      VALID_PARAMS.copy {
        impressionCapMode = ResultsFulfillerParams.ImpressionCapMode.UNCAPPED
        impressionMaxFrequencyPerUser = 1
      }

    assertThat(
        assertFailsWith<IllegalArgumentException> {
            ResultsFulfillerParamsValidator.validate(customWithoutCap)
          }
          .message
      )
      .contains("greater than zero under CUSTOM_CAP")
    assertThat(
        assertFailsWith<IllegalArgumentException> {
            ResultsFulfillerParamsValidator.validate(uncappedWithCap)
          }
          .message
      )
      .contains("ignored under UNCAPPED")
  }

  @Test
  fun `unsupported multi-party noise type is rejected`() {
    val invalidParams =
      VALID_PARAMS.copy {
        multiPartyConfig =
          ResultsFulfillerParams.MultiPartyConfig.newBuilder()
            .addSupportedNoiseTypes(NoiseType.UNSPECIFIED)
            .build()
      }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ResultsFulfillerParamsValidator.validate(invalidParams)
      }

    assertThat(exception).hasMessageThat().contains("Unsupported multi-party noise type")
  }

  @Test
  fun `invalid TrusTee key mapping is rejected`() {
    val invalidParams =
      VALID_PARAMS.copy {
        trusteeParams =
          ResultsFulfillerParams.TrusTeeParams.newBuilder()
            .putKekUriToKeyName(
              "gcp-kms://projects/project/locations/global/keyRings/ring/cryptoKeys/key",
              "invalid/key",
            )
            .build()
      }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ResultsFulfillerParamsValidator.validate(invalidParams)
      }

    assertThat(exception).hasMessageThat().contains("Invalid key name format")
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp1"
    private val VALID_PARAMS: ResultsFulfillerParams = resultsFulfillerParams {
      dataProvider = DATA_PROVIDER
      storageParams =
        ResultsFulfillerParams.StorageParams.newBuilder()
          .setLabeledImpressionsBlobDetailsUriPrefix("gs://bucket")
          .build()
      consentParams =
        ResultsFulfillerParams.ConsentParams.newBuilder()
          .setResultCsCertDerResourcePath("cert.der")
          .setResultCsPrivateKeyDerResourcePath("private.der")
          .setPrivateEncryptionKeyResourcePath("encryption.tink")
          .setEdpCertificateName("$DATA_PROVIDER/certificates/cert1")
          .build()
      cmmsConnection =
        org.wfanet.measurement.edpaggregator.v1alpha.TransportLayerSecurityParams.newBuilder()
          .setClientCertResourcePath("client.pem")
          .setClientPrivateKeyResourcePath("client.key")
          .build()
      noiseParams =
        ResultsFulfillerParams.NoiseParams.newBuilder()
          .setNoiseType(NoiseType.CONTINUOUS_GAUSSIAN)
          .build()
    }
  }
}
