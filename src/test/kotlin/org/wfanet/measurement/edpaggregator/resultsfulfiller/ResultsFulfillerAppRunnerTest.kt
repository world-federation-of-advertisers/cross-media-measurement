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
import java.io.File
import java.nio.file.Files
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfig

@RunWith(JUnit4::class)
class ResultsFulfillerAppRunnerTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  companion object {
    init {
      System.setProperty("otel.metrics.exporter", "none")
      System.setProperty("otel.traces.exporter", "none")
      System.setProperty("otel.logs.exporter", "none")
    }
  }

  @Test
  fun `saveSecretToFile writes bytes to file`() {
    val testFile = tempFolder.newFile("test.pem")
    val data = "testdata".toByteArray()
    val runner = ResultsFulfillerAppRunner()

    runner.saveByteArrayToFile(data, testFile.absolutePath)

    assertThat(Files.exists(testFile.toPath())).isTrue()
    assertThat(data).isEqualTo(Files.readAllBytes(testFile.toPath()))
  }

  @Test
  fun `saveSecretToFile creates parent directories`() {
    val nestedFile = File(tempFolder.root, "nested/dir/file.pem")
    val data = "nested-data".toByteArray()
    val runner = ResultsFulfillerAppRunner()

    runner.saveByteArrayToFile(data, nestedFile.absolutePath)

    assertThat(nestedFile.exists()).isTrue()
    assertThat(data).isEqualTo(nestedFile.readBytes())
  }

  @Test
  fun `EventDataProviderConfig KmsType defaults to UNSPECIFIED`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsAudience("test-audience")
        .setServiceAccount("test@example.com")
        .build()

    assertThat(config.kmsType)
      .isEqualTo(EventDataProviderConfig.KmsConfig.KmsType.KMS_TYPE_UNSPECIFIED)
  }

  @Test
  fun `EventDataProviderConfig KmsType GCP is set correctly`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsType(EventDataProviderConfig.KmsConfig.KmsType.GCP)
        .setKmsAudience("test-audience")
        .setServiceAccount("test@example.com")
        .build()

    assertThat(config.kmsType).isEqualTo(EventDataProviderConfig.KmsConfig.KmsType.GCP)
    assertThat(config.kmsAudience).isEqualTo("test-audience")
    assertThat(config.serviceAccount).isEqualTo("test@example.com")
  }

  @Test
  fun `EventDataProviderConfig KmsType AWS carries AWS fields`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsType(EventDataProviderConfig.KmsConfig.KmsType.AWS)
        .setAwsRoleArn("arn:aws:iam::123456789012:role/my-role")
        .setAwsRoleSessionName("my-session")
        .setAwsRegion("us-east-1")
        .setAwsAudience("sts.amazonaws.com")
        .build()

    assertThat(config.kmsType).isEqualTo(EventDataProviderConfig.KmsConfig.KmsType.AWS)
    assertThat(config.awsRoleArn).isEqualTo("arn:aws:iam::123456789012:role/my-role")
    assertThat(config.awsRoleSessionName).isEqualTo("my-session")
    assertThat(config.awsRegion).isEqualTo("us-east-1")
    assertThat(config.awsAudience).isEqualTo("sts.amazonaws.com")
  }

  @Test
  fun `EventDataProviderConfig AWS type has empty GCP fields`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsType(EventDataProviderConfig.KmsConfig.KmsType.AWS)
        .setAwsRoleArn("arn:aws:iam::123456789012:role/my-role")
        .setAwsRegion("us-east-1")
        .build()

    assertThat(config.kmsAudience).isEmpty()
    assertThat(config.serviceAccount).isEmpty()
  }

  @Test
  fun `EventDataProviderConfig GCP type has empty AWS fields`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsType(EventDataProviderConfig.KmsConfig.KmsType.GCP)
        .setKmsAudience("test-audience")
        .setServiceAccount("test@example.com")
        .build()

    assertThat(config.awsRoleArn).isEmpty()
    assertThat(config.awsRoleSessionName).isEmpty()
    assertThat(config.awsRegion).isEmpty()
    assertThat(config.awsAudience).isEmpty()
  }

  @Test
  fun `EventDataProviderConfig unspecified KmsType has empty AWS fields`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsAudience("test-audience")
        .setServiceAccount("test@example.com")
        .build()

    assertThat(config.kmsType)
      .isEqualTo(EventDataProviderConfig.KmsConfig.KmsType.KMS_TYPE_UNSPECIFIED)
    assertThat(config.awsRoleArn).isEmpty()
    assertThat(config.awsRoleSessionName).isEmpty()
    assertThat(config.awsRegion).isEmpty()
    assertThat(config.awsAudience).isEmpty()
  }

  @Test
  fun `toApiKmsType maps UNSPECIFIED to GCP`() {
    val apiKmsType =
      when (EventDataProviderConfig.KmsConfig.KmsType.KMS_TYPE_UNSPECIFIED) {
        EventDataProviderConfig.KmsConfig.KmsType.AWS ->
          FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.AWS
        EventDataProviderConfig.KmsConfig.KmsType.GCP ->
          FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.GCP
        EventDataProviderConfig.KmsConfig.KmsType.KMS_TYPE_UNSPECIFIED ->
          FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.GCP
        EventDataProviderConfig.KmsConfig.KmsType.UNRECOGNIZED -> error("Unrecognized KMS type")
      }

    assertThat(apiKmsType)
      .isEqualTo(FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.GCP)
  }

  @Test
  fun `toApiKmsType maps GCP to GCP`() {
    val apiKmsType =
      when (EventDataProviderConfig.KmsConfig.KmsType.GCP) {
        EventDataProviderConfig.KmsConfig.KmsType.AWS ->
          FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.AWS
        EventDataProviderConfig.KmsConfig.KmsType.GCP ->
          FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.GCP
        EventDataProviderConfig.KmsConfig.KmsType.KMS_TYPE_UNSPECIFIED ->
          FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.GCP
        EventDataProviderConfig.KmsConfig.KmsType.UNRECOGNIZED -> error("Unrecognized KMS type")
      }

    assertThat(apiKmsType)
      .isEqualTo(FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.GCP)
  }

  @Test
  fun `toApiKmsType maps AWS to AWS`() {
    val apiKmsType =
      when (EventDataProviderConfig.KmsConfig.KmsType.AWS) {
        EventDataProviderConfig.KmsConfig.KmsType.AWS ->
          FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.AWS
        EventDataProviderConfig.KmsConfig.KmsType.GCP ->
          FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.GCP
        EventDataProviderConfig.KmsConfig.KmsType.KMS_TYPE_UNSPECIFIED ->
          FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.GCP
        EventDataProviderConfig.KmsConfig.KmsType.UNRECOGNIZED -> error("Unrecognized KMS type")
      }

    assertThat(apiKmsType)
      .isEqualTo(FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.AWS)
  }

  @Test
  fun `AWS KmsType with all fields present passes validation`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsType(EventDataProviderConfig.KmsConfig.KmsType.AWS)
        .setAwsRoleArn("arn:aws:iam::123456789012:role/my-role")
        .setAwsRoleSessionName("my-session")
        .setAwsRegion("us-east-1")
        .setAwsAudience("sts.amazonaws.com")
        .build()

    val apiKmsType = FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.AWS
    val isAws =
      apiKmsType == FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.AWS

    if (isAws) {
      require(config.awsRoleArn.isNotEmpty()) { "aws_role_arn is required when kms_type is AWS" }
      require(config.awsRoleSessionName.isNotEmpty()) {
        "aws_role_session_name is required when kms_type is AWS"
      }
      require(config.awsRegion.isNotEmpty()) { "aws_region is required when kms_type is AWS" }
      require(config.awsAudience.isNotEmpty()) { "aws_audience is required when kms_type is AWS" }
    }
  }

  @Test
  fun `AWS KmsType missing aws_role_arn fails validation`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsType(EventDataProviderConfig.KmsConfig.KmsType.AWS)
        .setAwsRoleSessionName("my-session")
        .setAwsRegion("us-east-1")
        .setAwsAudience("sts.amazonaws.com")
        .build()

    val exception =
      assertFailsWith<IllegalArgumentException> {
        require(config.awsRoleArn.isNotEmpty()) { "aws_role_arn is required when kms_type is AWS" }
      }
    assertThat(exception).hasMessageThat().contains("aws_role_arn")
  }

  @Test
  fun `AWS KmsType missing aws_role_session_name fails validation`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsType(EventDataProviderConfig.KmsConfig.KmsType.AWS)
        .setAwsRoleArn("arn:aws:iam::123456789012:role/my-role")
        .setAwsRegion("us-east-1")
        .setAwsAudience("sts.amazonaws.com")
        .build()

    val exception =
      assertFailsWith<IllegalArgumentException> {
        require(config.awsRoleSessionName.isNotEmpty()) {
          "aws_role_session_name is required when kms_type is AWS"
        }
      }
    assertThat(exception).hasMessageThat().contains("aws_role_session_name")
  }

  @Test
  fun `AWS KmsType missing aws_region fails validation`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsType(EventDataProviderConfig.KmsConfig.KmsType.AWS)
        .setAwsRoleArn("arn:aws:iam::123456789012:role/my-role")
        .setAwsRoleSessionName("my-session")
        .setAwsAudience("sts.amazonaws.com")
        .build()

    val exception =
      assertFailsWith<IllegalArgumentException> {
        require(config.awsRegion.isNotEmpty()) { "aws_region is required when kms_type is AWS" }
      }
    assertThat(exception).hasMessageThat().contains("aws_region")
  }

  @Test
  fun `AWS KmsType missing aws_audience fails validation`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsType(EventDataProviderConfig.KmsConfig.KmsType.AWS)
        .setAwsRoleArn("arn:aws:iam::123456789012:role/my-role")
        .setAwsRoleSessionName("my-session")
        .setAwsRegion("us-east-1")
        .build()

    val exception =
      assertFailsWith<IllegalArgumentException> {
        require(config.awsAudience.isNotEmpty()) { "aws_audience is required when kms_type is AWS" }
      }
    assertThat(exception).hasMessageThat().contains("aws_audience")
  }

  @Test
  fun `GCP KmsType skips AWS field validation`() {
    val config =
      EventDataProviderConfig.KmsConfig.newBuilder()
        .setKmsType(EventDataProviderConfig.KmsConfig.KmsType.GCP)
        .setKmsAudience("test-audience")
        .setServiceAccount("test@example.com")
        .build()

    val apiKmsType = FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.GCP
    val isAws =
      apiKmsType == FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.KmsType.AWS

    assertThat(isAws).isFalse()
    assertThat(config.awsRoleArn).isEmpty()
    assertThat(config.awsRoleSessionName).isEmpty()
    assertThat(config.awsRegion).isEmpty()
    assertThat(config.awsAudience).isEmpty()
  }

  @Test
  fun `DuchyFlags fields are set correctly`() {
    val duchyFlags =
      ResultsFulfillerAppRunner.DuchyFlags().apply {
        duchyId = "duchy1"
        duchyTarget = "localhost:8080"
        duchyCertHost = "duchy1.example.com"
      }

    assertThat(duchyFlags.duchyId).isEqualTo("duchy1")
    assertThat(duchyFlags.duchyTarget).isEqualTo("localhost:8080")
    assertThat(duchyFlags.duchyCertHost).isEqualTo("duchy1.example.com")
  }
}
