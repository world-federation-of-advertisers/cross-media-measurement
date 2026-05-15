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

@RunWith(JUnit4::class)
class KmsConstantsTest {

  @Test
  fun `GCP_KMS_KEY_URI_REGEX matches valid GCP KMS URI`() {
    val uri = "gcp-kms://projects/my-project/locations/us-east1/keyRings/my-ring/cryptoKeys/my-key"
    val match = KmsConstants.GCP_KMS_KEY_URI_REGEX.matchEntire(uri)

    assertThat(match).isNotNull()
    assertThat(match!!.groupValues[1]).isEqualTo("my-project")
    assertThat(match.groupValues[2]).isEqualTo("us-east1")
    assertThat(match.groupValues[3]).isEqualTo("my-ring")
    assertThat(match.groupValues[4]).isEqualTo("my-key")
  }

  @Test
  fun `GCP_KMS_KEY_URI_REGEX does not match AWS KMS URI`() {
    val uri = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abcd-1234"
    val match = KmsConstants.GCP_KMS_KEY_URI_REGEX.matchEntire(uri)

    assertThat(match).isNull()
  }

  @Test
  fun `GCP_KMS_KEY_URI_REGEX does not match malformed URI`() {
    val uri = "gcp-kms://projects/my-project/locations/us-east1"
    val match = KmsConstants.GCP_KMS_KEY_URI_REGEX.matchEntire(uri)

    assertThat(match).isNull()
  }

  @Test
  fun `AWS_KMS_KEY_URI_REGEX matches valid AWS KMS URI`() {
    val uri = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abcd-1234-efgh-5678"
    val match = KmsConstants.AWS_KMS_KEY_URI_REGEX.matchEntire(uri)

    assertThat(match).isNotNull()
    assertThat(match!!.groupValues[1]).isEqualTo("us-east-1")
    assertThat(match.groupValues[2]).isEqualTo("123456789012")
    assertThat(match.groupValues[3]).isEqualTo("abcd-1234-efgh-5678")
  }

  @Test
  fun `AWS_KMS_KEY_URI_REGEX matches URI with UUID key ID`() {
    val uri =
      "aws-kms://arn:aws:kms:eu-west-2:999888777666:key/12345678-1234-1234-1234-123456789012"
    val match = KmsConstants.AWS_KMS_KEY_URI_REGEX.matchEntire(uri)

    assertThat(match).isNotNull()
    assertThat(match!!.groupValues[1]).isEqualTo("eu-west-2")
    assertThat(match.groupValues[2]).isEqualTo("999888777666")
    assertThat(match.groupValues[3]).isEqualTo("12345678-1234-1234-1234-123456789012")
  }

  @Test
  fun `AWS_KMS_KEY_URI_REGEX does not match GCP KMS URI`() {
    val uri = "gcp-kms://projects/my-project/locations/us-east1/keyRings/my-ring/cryptoKeys/my-key"
    val match = KmsConstants.AWS_KMS_KEY_URI_REGEX.matchEntire(uri)

    assertThat(match).isNull()
  }

  @Test
  fun `AWS_KMS_KEY_URI_REGEX does not match malformed URI`() {
    val uri = "aws-kms://arn:aws:kms:us-east-1"
    val match = KmsConstants.AWS_KMS_KEY_URI_REGEX.matchEntire(uri)

    assertThat(match).isNull()
  }

  @Test
  fun `AWS_KMS_KEY_URI_REGEX does not match fake-kms URI`() {
    val uri = "fake-kms://key1"
    val match = KmsConstants.AWS_KMS_KEY_URI_REGEX.matchEntire(uri)

    assertThat(match).isNull()
  }
}
