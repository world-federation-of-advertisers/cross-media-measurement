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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.CommandLineTesting.assertThat
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager

@RunWith(JUnit4::class)
class VerifySyntheticDataTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  private fun callCli(args: Array<String>) = CommandLineTesting.capturingOutput(args, ::main)

  @Test
  fun `exits with error when required flags are missing`() {
    val capturedOutput = callCli(arrayOf())

    assertThat(capturedOutput).status().isNotEqualTo(0)
  }

  @Test
  fun `exits with error when AWS kms type is missing role arn`() {
    val capturedOutput =
      callCli(
        arrayOf(
          "--kms-type",
          "AWS",
          "--kek-uri",
          "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
          "--local-storage-path",
          tempFolder.root.absolutePath,
          "--output-bucket",
          "test-bucket",
          "--impression-metadata-base-path",
          "edp/edp-test",
        )
      )

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).err().contains("--aws-role-arn")
  }

  @Test
  fun `exits with error when AWS kms type is missing web identity token file`() {
    val capturedOutput =
      callCli(
        arrayOf(
          "--kms-type",
          "AWS",
          "--kek-uri",
          "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
          "--local-storage-path",
          tempFolder.root.absolutePath,
          "--output-bucket",
          "test-bucket",
          "--impression-metadata-base-path",
          "edp/edp-test",
          "--aws-role-arn",
          "arn:aws:iam::123456789012:role/test-role",
        )
      )

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).err().contains("--aws-web-identity-token-file")
  }

  @Test
  fun `exits with error when AWS kms type is missing region`() {
    val capturedOutput =
      callCli(
        arrayOf(
          "--kms-type",
          "AWS",
          "--kek-uri",
          "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
          "--local-storage-path",
          tempFolder.root.absolutePath,
          "--output-bucket",
          "test-bucket",
          "--impression-metadata-base-path",
          "edp/edp-test",
          "--aws-role-arn",
          "arn:aws:iam::123456789012:role/test-role",
          "--aws-web-identity-token-file",
          "/var/run/secrets/token",
        )
      )

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).err().contains("--aws-region")
  }

  @Test
  fun `exits with error when GCP_TO_AWS is missing role arn`() {
    val capturedOutput =
      callCli(
        arrayOf(
          "--kms-type",
          "GCP_TO_AWS",
          "--kek-uri",
          "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
          "--local-storage-path",
          tempFolder.root.absolutePath,
          "--output-bucket",
          "test-bucket",
          "--impression-metadata-base-path",
          "edp/edp-test",
        )
      )

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).err().contains("--gcp-to-aws-role-arn")
  }

  @Test
  fun `exits with error when GCP_TO_AWS is missing gcp audience`() {
    val capturedOutput =
      callCli(
        arrayOf(
          "--kms-type",
          "GCP_TO_AWS",
          "--kek-uri",
          "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
          "--local-storage-path",
          tempFolder.root.absolutePath,
          "--output-bucket",
          "test-bucket",
          "--impression-metadata-base-path",
          "edp/edp-test",
          "--gcp-to-aws-role-arn",
          "arn:aws:iam::123456789012:role/test-role",
          "--gcp-to-aws-region",
          "us-east-1",
        )
      )

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).err().contains("--gcp-audience")
  }

  @Test
  fun `exits with error when GCP_TO_AWS is missing service account impersonation url`() {
    val capturedOutput =
      callCli(
        arrayOf(
          "--kms-type",
          "GCP_TO_AWS",
          "--kek-uri",
          "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
          "--local-storage-path",
          tempFolder.root.absolutePath,
          "--output-bucket",
          "test-bucket",
          "--impression-metadata-base-path",
          "edp/edp-test",
          "--gcp-to-aws-role-arn",
          "arn:aws:iam::123456789012:role/test-role",
          "--gcp-to-aws-region",
          "us-east-1",
          "--gcp-audience",
          "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/p",
        )
      )

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).err().contains("--gcp-service-account-impersonation-url")
  }

  @Test
  fun `exits with error when GCP_TO_AWS is missing aws audience`() {
    val capturedOutput =
      callCli(
        arrayOf(
          "--kms-type",
          "GCP_TO_AWS",
          "--kek-uri",
          "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
          "--local-storage-path",
          tempFolder.root.absolutePath,
          "--output-bucket",
          "test-bucket",
          "--impression-metadata-base-path",
          "edp/edp-test",
          "--gcp-to-aws-role-arn",
          "arn:aws:iam::123456789012:role/test-role",
          "--gcp-to-aws-region",
          "us-east-1",
          "--gcp-audience",
          "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/p",
          "--gcp-service-account-impersonation-url",
          "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@proj.iam.gserviceaccount.com:generateAccessToken",
        )
      )

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).err().contains("--aws-audience")
  }

  companion object {
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }
  }
}
