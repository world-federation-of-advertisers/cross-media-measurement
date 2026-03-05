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

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.HeaderKt.TrusTeeKt.EnvelopeEncryptionKt.awsKmsParams
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient

@RunWith(JUnit4::class)
class TrusTeeConfigTest {

  private val fakeKmsClient = FakeKmsClient()

  private val trusTeeConfig =
    TrusTeeConfig(
      kmsClient = fakeKmsClient,
      workloadIdentityProvider = "test-provider",
      impersonatedServiceAccount = "test-sa@example.com",
      awsKmsParams = null,
    )

  @Test
  fun `buildEncryptionParams remaps GCP KMS KEK URI`() {
    val originalUri =
      "gcp-kms://projects/my-project/locations/us-east1/keyRings/my-ring/cryptoKeys/old-key"
    val kekUriToKeyNameMap = mapOf(originalUri to "new-key")

    val params = trusTeeConfig.buildEncryptionParams(originalUri, kekUriToKeyNameMap)

    assertThat(params.kmsKekUri)
      .isEqualTo(
        "gcp-kms://projects/my-project/locations/us-east1/keyRings/my-ring/cryptoKeys/new-key"
      )
  }

  @Test
  fun `buildEncryptionParams remaps AWS KMS KEK URI`() {
    val originalUri = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/old-key-id"
    val kekUriToKeyNameMap = mapOf(originalUri to "new-key-id")

    val params = trusTeeConfig.buildEncryptionParams(originalUri, kekUriToKeyNameMap)

    assertThat(params.kmsKekUri)
      .isEqualTo("aws-kms://arn:aws:kms:us-east-1:123456789012:key/new-key-id")
  }

  @Test
  fun `buildEncryptionParams returns original URI when not in remap map`() {
    val originalUri =
      "gcp-kms://projects/my-project/locations/us-east1/keyRings/my-ring/cryptoKeys/my-key"
    val kekUriToKeyNameMap = emptyMap<String, String>()

    val params = trusTeeConfig.buildEncryptionParams(originalUri, kekUriToKeyNameMap)

    assertThat(params.kmsKekUri).isEqualTo(originalUri)
  }

  @Test
  fun `buildEncryptionParams returns original URI for unknown format in remap map`() {
    val originalUri = "unknown-kms://some-key"
    val kekUriToKeyNameMap = mapOf(originalUri to "new-key")

    val params = trusTeeConfig.buildEncryptionParams(originalUri, kekUriToKeyNameMap)

    assertThat(params.kmsKekUri).isEqualTo(originalUri)
  }

  @Test
  fun `buildEncryptionParams preserves AWS region and account during remap`() {
    val originalUri = "aws-kms://arn:aws:kms:eu-west-2:999888777666:key/original-key"
    val kekUriToKeyNameMap = mapOf(originalUri to "remapped-key")

    val params = trusTeeConfig.buildEncryptionParams(originalUri, kekUriToKeyNameMap)

    assertThat(params.kmsKekUri)
      .isEqualTo("aws-kms://arn:aws:kms:eu-west-2:999888777666:key/remapped-key")
  }

  @Test
  fun `buildEncryptionParams passes through KMS client and identity fields`() {
    val uri = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/my-key"

    val params = trusTeeConfig.buildEncryptionParams(uri, emptyMap())

    assertThat(params.kmsClient).isSameInstanceAs(fakeKmsClient)
    assertThat(params.workloadIdentityProvider).isEqualTo("test-provider")
    assertThat(params.impersonatedServiceAccount).isEqualTo("test-sa@example.com")
  }

  @Test
  fun `buildEncryptionParams with GCP type has null AWS fields`() {
    val uri = "gcp-kms://projects/my-project/locations/us-east1/keyRings/my-ring/cryptoKeys/my-key"

    val params = trusTeeConfig.buildEncryptionParams(uri, emptyMap())

    assertThat(params.awsKmsParams).isNull()
  }

  @Test
  fun `buildEncryptionParams with AWS type passes AWS fields`() {
    val awsConfig =
      TrusTeeConfig(
        kmsClient = fakeKmsClient,
        workloadIdentityProvider = "test-provider",
        impersonatedServiceAccount = "test-sa@example.com",
        awsKmsParams =
          awsKmsParams {
            roleArn = "arn:aws:iam::123456789012:role/my-role"
            roleSession = "my-session"
            region = "us-east-1"
            audience = "sts.amazonaws.com"
          },
      )
    val uri = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/my-key"

    val params = awsConfig.buildEncryptionParams(uri, emptyMap())

    val resultAwsKmsParams = checkNotNull(params.awsKmsParams)
    assertThat(resultAwsKmsParams.roleArn).isEqualTo("arn:aws:iam::123456789012:role/my-role")
    assertThat(resultAwsKmsParams.roleSession).isEqualTo("my-session")
    assertThat(resultAwsKmsParams.region).isEqualTo("us-east-1")
    assertThat(resultAwsKmsParams.audience).isEqualTo("sts.amazonaws.com")
  }
}
