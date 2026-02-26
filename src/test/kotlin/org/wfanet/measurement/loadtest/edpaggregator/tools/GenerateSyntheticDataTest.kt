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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import picocli.CommandLine

@RunWith(JUnit4::class)
class GenerateSyntheticDataTest {

  @Test
  fun `KmsType enum contains FAKE, GCP, and AWS`() {
    assertThat(KmsType.values().toList())
      .containsExactly(KmsType.FAKE, KmsType.GCP, KmsType.AWS, KmsType.GCP_TO_AWS)
  }

  @Test
  fun `picocli parses required flags correctly`() {
    val cmd = GenerateSyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "FAKE",
        "--event-group-reference-id",
        "eg-1",
        "--model-line",
        "modelProviders/mp1/modelSuites/ms1/modelLines/ml1",
        "--output-bucket",
        "test-bucket",
        "--population-spec-resource-path",
        "pop.textproto",
        "--data-spec-resource-path",
        "data.textproto",
        "--impression-metadata-base-path",
        "/some/path",
      )

    assertThat(cmd.kmsType).isEqualTo(KmsType.FAKE)
    assertThat(cmd.eventGroupReferenceId).isEqualTo("eg-1")
    assertThat(cmd.modelLine).isEqualTo("modelProviders/mp1/modelSuites/ms1/modelLines/ml1")
    assertThat(cmd.outputBucket).isEqualTo("test-bucket")
    assertThat(cmd.populationSpecResourcePath).isEqualTo("pop.textproto")
    assertThat(cmd.dataSpecResourcePath).isEqualTo("data.textproto")
    assertThat(cmd.impressionMetadataBasePath).isEqualTo("/some/path")
  }

  @Test
  fun `picocli uses correct default values`() {
    val cmd = GenerateSyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "FAKE",
        "--event-group-reference-id",
        "eg-1",
        "--model-line",
        "modelProviders/mp1/modelSuites/ms1/modelLines/ml1",
        "--output-bucket",
        "test-bucket",
        "--population-spec-resource-path",
        "pop.textproto",
        "--data-spec-resource-path",
        "data.textproto",
        "--impression-metadata-base-path",
        "/some/path",
      )

    assertThat(cmd.schema).isEqualTo("file:///")
    assertThat(cmd.zoneId).isEqualTo("UTC")
    assertThat(cmd.awsRoleArn).isEmpty()
    assertThat(cmd.awsWebIdentityTokenFile).isEmpty()
    assertThat(cmd.awsRoleSessionName).isEqualTo("generate-synthetic-data")
    assertThat(cmd.awsRegion).isEmpty()
  }

  @Test
  fun `picocli parses AWS flags correctly`() {
    val cmd = GenerateSyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "AWS",
        "--event-group-reference-id",
        "eg-1",
        "--model-line",
        "modelProviders/mp1/modelSuites/ms1/modelLines/ml1",
        "--output-bucket",
        "test-bucket",
        "--population-spec-resource-path",
        "pop.textproto",
        "--data-spec-resource-path",
        "data.textproto",
        "--impression-metadata-base-path",
        "/some/path",
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
    assertThat(cmd.awsRoleArn).isEqualTo("arn:aws:iam::123456789012:role/test-role")
    assertThat(cmd.awsWebIdentityTokenFile).isEqualTo("/var/run/secrets/token")
    assertThat(cmd.awsRoleSessionName).isEqualTo("my-session")
    assertThat(cmd.awsRegion).isEqualTo("us-west-2")
  }

  @Test
  fun `picocli parses GCP kms type`() {
    val cmd = GenerateSyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "GCP",
        "--event-group-reference-id",
        "eg-1",
        "--model-line",
        "modelProviders/mp1/modelSuites/ms1/modelLines/ml1",
        "--output-bucket",
        "test-bucket",
        "--kek-uri",
        "gcp-kms://projects/p1/locations/l1/keyRings/kr1/cryptoKeys/ck1",
        "--population-spec-resource-path",
        "pop.textproto",
        "--data-spec-resource-path",
        "data.textproto",
        "--impression-metadata-base-path",
        "/some/path",
      )

    assertThat(cmd.kmsType).isEqualTo(KmsType.GCP)
    assertThat(cmd.kekUri)
      .isEqualTo("gcp-kms://projects/p1/locations/l1/keyRings/kr1/cryptoKeys/ck1")
  }

  @Test
  fun `picocli rejects invalid kms type`() {
    assertFailsWith<CommandLine.ParameterException> {
      CommandLine(GenerateSyntheticData())
        .parseArgs(
          "--kms-type",
          "INVALID",
          "--event-group-reference-id",
          "eg-1",
          "--model-line",
          "ml1",
          "--output-bucket",
          "bucket",
          "--population-spec-resource-path",
          "pop.textproto",
          "--data-spec-resource-path",
          "data.textproto",
          "--impression-metadata-base-path",
          "/path",
        )
    }
  }

  @Test
  fun `picocli overrides kek-uri default`() {
    val cmd = GenerateSyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "FAKE",
        "--event-group-reference-id",
        "eg-1",
        "--model-line",
        "ml1",
        "--output-bucket",
        "bucket",
        "--kek-uri",
        "aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123",
        "--population-spec-resource-path",
        "pop.textproto",
        "--data-spec-resource-path",
        "data.textproto",
        "--impression-metadata-base-path",
        "/path",
      )

    assertThat(cmd.kekUri).isEqualTo("aws-kms://arn:aws:kms:us-east-1:123456789012:key/abc-123")
  }

  @Test
  fun `picocli overrides schema default`() {
    val cmd = GenerateSyntheticData()
    CommandLine(cmd)
      .parseArgs(
        "--kms-type",
        "FAKE",
        "--event-group-reference-id",
        "eg-1",
        "--model-line",
        "ml1",
        "--output-bucket",
        "bucket",
        "--schema",
        "gs://",
        "--population-spec-resource-path",
        "pop.textproto",
        "--data-spec-resource-path",
        "data.textproto",
        "--impression-metadata-base-path",
        "/path",
      )

    assertThat(cmd.schema).isEqualTo("gs://")
  }
}
