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
  fun `unspecified noise type is rejected`() {
    val invalidParams = VALID_PARAMS.copy { noiseParams = noiseParams.toBuilder().clear().build() }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ResultsFulfillerParamsValidator.validate(invalidParams)
      }

    assertThat(exception).hasMessageThat().contains("Unsupported noise type")
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
