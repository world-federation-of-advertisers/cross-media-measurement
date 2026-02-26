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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import picocli.CommandLine

@RunWith(JUnit4::class)
class VerifySyntheticDataTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  @Test
  fun `picocli parses required flags correctly`() {
    val cmd = VerifySyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "FAKE",
        "--kek-uri",
        "fake-kms://key1",
        "--local-storage-path",
        "/tmp/storage",
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
      )

    assertThat(cmd.kmsType).isEqualTo(KmsType.FAKE)
    assertThat(cmd.kekUri).isEqualTo("fake-kms://key1")
    assertThat(cmd.outputBucket).isEqualTo("test-bucket")
    assertThat(cmd.impressionMetadataBasePath).isEqualTo("edp/edp-test")
  }

  @Test
  fun `picocli uses correct AWS default values`() {
    val cmd = VerifySyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "FAKE",
        "--kek-uri",
        "fake-kms://key1",
        "--local-storage-path",
        "/tmp/storage",
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
      )

    assertThat(cmd.awsFlags.awsRoleArn).isEmpty()
    assertThat(cmd.awsFlags.awsWebIdentityTokenFile).isEmpty()
    assertThat(cmd.awsFlags.awsRoleSessionName).isEqualTo("verify-synthetic-data")
    assertThat(cmd.awsFlags.awsRegion).isEmpty()
  }

  @Test
  fun `picocli uses correct GcpToAws default values`() {
    val cmd = VerifySyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "FAKE",
        "--kek-uri",
        "fake-kms://key1",
        "--local-storage-path",
        "/tmp/storage",
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
      )

    assertThat(cmd.gcpToAwsFlags.roleArn).isEmpty()
    assertThat(cmd.gcpToAwsFlags.roleSessionName).isEqualTo("verify-synthetic-data")
    assertThat(cmd.gcpToAwsFlags.region).isEmpty()
    assertThat(cmd.gcpToAwsFlags.gcpAudience).isEmpty()
    assertThat(cmd.gcpToAwsFlags.subjectTokenType).isEqualTo("urn:ietf:params:oauth:token-type:jwt")
    assertThat(cmd.gcpToAwsFlags.tokenUrl).isEqualTo("https://sts.googleapis.com/v1/token")
    assertThat(cmd.gcpToAwsFlags.credentialSourceFilePath)
      .isEqualTo("/run/container_launcher/attestation_verifier_claims_token")
    assertThat(cmd.gcpToAwsFlags.serviceAccountImpersonationUrl).isEmpty()
    assertThat(cmd.gcpToAwsFlags.awsAudience).isEmpty()
  }

  @Test
  fun `picocli parses AWS flags correctly`() {
    val cmd = VerifySyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "AWS",
        "--kek-uri",
        "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
        "--local-storage-path",
        "/tmp/storage",
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
        "--aws-role-arn",
        "arn:aws:iam::123456789012:role/test-role",
        "--aws-web-identity-token-file",
        "/var/run/secrets/token",
        "--aws-role-session-name",
        "my-session",
        "--aws-region",
        "us-west-2",
      )

    assertThat(cmd.kmsType).isEqualTo(KmsType.AWS)
    assertThat(cmd.awsFlags.awsRoleArn).isEqualTo("arn:aws:iam::123456789012:role/test-role")
    assertThat(cmd.awsFlags.awsWebIdentityTokenFile).isEqualTo("/var/run/secrets/token")
    assertThat(cmd.awsFlags.awsRoleSessionName).isEqualTo("my-session")
    assertThat(cmd.awsFlags.awsRegion).isEqualTo("us-west-2")
  }

  @Test
  fun `picocli parses GCP kms type`() {
    val cmd = VerifySyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "GCP",
        "--kek-uri",
        "gcp-kms://projects/p1/locations/l1/keyRings/kr1/cryptoKeys/ck1",
        "--local-storage-path",
        "/tmp/storage",
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
      )

    assertThat(cmd.kmsType).isEqualTo(KmsType.GCP)
    assertThat(cmd.kekUri)
      .isEqualTo("gcp-kms://projects/p1/locations/l1/keyRings/kr1/cryptoKeys/ck1")
  }

  @Test
  fun `picocli parses GCP_TO_AWS flags correctly`() {
    val cmd = VerifySyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "GCP_TO_AWS",
        "--kek-uri",
        "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
        "--local-storage-path",
        "/tmp/storage",
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
        "--gcp-to-aws-role-arn",
        "arn:aws:iam::123456789012:role/gcp-to-aws-role",
        "--gcp-to-aws-role-session-name",
        "my-gcp-to-aws-session",
        "--gcp-to-aws-region",
        "eu-west-1",
        "--gcp-audience",
        "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/p",
        "--gcp-service-account-impersonation-url",
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@proj.iam.gserviceaccount.com:generateAccessToken",
        "--aws-audience",
        "sts.amazonaws.com",
      )

    assertThat(cmd.kmsType).isEqualTo(KmsType.GCP_TO_AWS)
    assertThat(cmd.gcpToAwsFlags.roleArn)
      .isEqualTo("arn:aws:iam::123456789012:role/gcp-to-aws-role")
    assertThat(cmd.gcpToAwsFlags.roleSessionName).isEqualTo("my-gcp-to-aws-session")
    assertThat(cmd.gcpToAwsFlags.region).isEqualTo("eu-west-1")
    assertThat(cmd.gcpToAwsFlags.gcpAudience)
      .isEqualTo(
        "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/p"
      )
    assertThat(cmd.gcpToAwsFlags.serviceAccountImpersonationUrl)
      .isEqualTo(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@proj.iam.gserviceaccount.com:generateAccessToken"
      )
    assertThat(cmd.gcpToAwsFlags.awsAudience).isEqualTo("sts.amazonaws.com")
  }

  @Test
  fun `run fails when AWS kms type is missing role arn`() {
    val cmd = VerifySyntheticData()
    val storageDir = tempFolder.newFolder("storage")
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "AWS",
        "--kek-uri",
        "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
        "--local-storage-path",
        storageDir.absolutePath,
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
      )

    val exception = assertFailsWith<IllegalArgumentException> { cmd.run() }
    assertThat(exception).hasMessageThat().contains("--aws-role-arn")
  }

  @Test
  fun `run fails when AWS kms type is missing web identity token file`() {
    val cmd = VerifySyntheticData()
    val storageDir = tempFolder.newFolder("storage")
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "AWS",
        "--kek-uri",
        "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
        "--local-storage-path",
        storageDir.absolutePath,
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
        "--aws-role-arn",
        "arn:aws:iam::123456789012:role/test-role",
      )

    val exception = assertFailsWith<IllegalArgumentException> { cmd.run() }
    assertThat(exception).hasMessageThat().contains("--aws-web-identity-token-file")
  }

  @Test
  fun `run fails when AWS kms type is missing region`() {
    val cmd = VerifySyntheticData()
    val storageDir = tempFolder.newFolder("storage")
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "AWS",
        "--kek-uri",
        "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
        "--local-storage-path",
        storageDir.absolutePath,
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
        "--aws-role-arn",
        "arn:aws:iam::123456789012:role/test-role",
        "--aws-web-identity-token-file",
        "/var/run/secrets/token",
      )

    val exception = assertFailsWith<IllegalArgumentException> { cmd.run() }
    assertThat(exception).hasMessageThat().contains("--aws-region")
  }

  @Test
  fun `run fails when GCP_TO_AWS is missing role arn`() {
    val cmd = VerifySyntheticData()
    val storageDir = tempFolder.newFolder("storage")
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "GCP_TO_AWS",
        "--kek-uri",
        "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
        "--local-storage-path",
        storageDir.absolutePath,
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
      )

    val exception = assertFailsWith<IllegalArgumentException> { cmd.run() }
    assertThat(exception).hasMessageThat().contains("--gcp-to-aws-role-arn")
  }

  @Test
  fun `run fails when GCP_TO_AWS is missing gcp audience`() {
    val cmd = VerifySyntheticData()
    val storageDir = tempFolder.newFolder("storage")
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "GCP_TO_AWS",
        "--kek-uri",
        "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
        "--local-storage-path",
        storageDir.absolutePath,
        "--output-bucket",
        "test-bucket",
        "--impression-metadata-base-path",
        "edp/edp-test",
        "--gcp-to-aws-role-arn",
        "arn:aws:iam::123456789012:role/test-role",
        "--gcp-to-aws-region",
        "us-east-1",
      )

    val exception = assertFailsWith<IllegalArgumentException> { cmd.run() }
    assertThat(exception).hasMessageThat().contains("--gcp-audience")
  }

  @Test
  fun `run fails when GCP_TO_AWS is missing service account impersonation url`() {
    val cmd = VerifySyntheticData()
    val storageDir = tempFolder.newFolder("storage")
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "GCP_TO_AWS",
        "--kek-uri",
        "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
        "--local-storage-path",
        storageDir.absolutePath,
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

    val exception = assertFailsWith<IllegalArgumentException> { cmd.run() }
    assertThat(exception).hasMessageThat().contains("--gcp-service-account-impersonation-url")
  }

  @Test
  fun `run fails when GCP_TO_AWS is missing aws audience`() {
    val cmd = VerifySyntheticData()
    val storageDir = tempFolder.newFolder("storage")
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "GCP_TO_AWS",
        "--kek-uri",
        "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
        "--local-storage-path",
        storageDir.absolutePath,
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

    val exception = assertFailsWith<IllegalArgumentException> { cmd.run() }
    assertThat(exception).hasMessageThat().contains("--aws-audience")
  }
}
