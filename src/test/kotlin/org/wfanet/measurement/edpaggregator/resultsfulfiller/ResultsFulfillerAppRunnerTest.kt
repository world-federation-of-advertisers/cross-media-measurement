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
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfig
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfigKt.kmsConfig
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

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
  fun `KmsType defaults to UNSPECIFIED`() {
    val config = kmsConfig {
      kmsAudience = "test-audience"
      serviceAccount = "test@example.com"
    }

    assertThat(config.kmsType)
      .isEqualTo(EventDataProviderConfig.KmsConfig.KmsType.KMS_TYPE_UNSPECIFIED)
  }

  @Test
  fun `GCP KmsType is set correctly`() {
    val config = kmsConfig {
      kmsAudience = "test-audience"
      serviceAccount = "test@example.com"
      kmsType = EventDataProviderConfig.KmsConfig.KmsType.GCP
    }

    assertThat(config.kmsType).isEqualTo(EventDataProviderConfig.KmsConfig.KmsType.GCP)
    assertThat(config.kmsAudience).isEqualTo("test-audience")
    assertThat(config.serviceAccount).isEqualTo("test@example.com")
  }

  @Test
  fun `AWS KmsType carries AWS fields`() {
    val config = kmsConfig {
      kmsType = EventDataProviderConfig.KmsConfig.KmsType.AWS
      awsRoleArn = "arn:aws:iam::123456789012:role/my-role"
      awsRoleSessionName = "my-session"
      awsRegion = "us-east-1"
      awsAudience = "sts.amazonaws.com"
    }

    assertThat(config.kmsType).isEqualTo(EventDataProviderConfig.KmsConfig.KmsType.AWS)
    assertThat(config.awsRoleArn).isEqualTo("arn:aws:iam::123456789012:role/my-role")
    assertThat(config.awsRoleSessionName).isEqualTo("my-session")
    assertThat(config.awsRegion).isEqualTo("us-east-1")
    assertThat(config.awsAudience).isEqualTo("sts.amazonaws.com")
  }

  @Test
  fun `AWS type has empty GCP fields`() {
    val config = kmsConfig {
      kmsType = EventDataProviderConfig.KmsConfig.KmsType.AWS
      awsRoleArn = "arn:aws:iam::123456789012:role/my-role"
      awsRegion = "us-east-1"
    }

    assertThat(config.kmsAudience).isEmpty()
    assertThat(config.serviceAccount).isEmpty()
  }

  @Test
  fun `AWS_CONFIDENTIAL_SPACE KmsType carries AWS fields`() {
    val config = kmsConfig {
      kmsType = EventDataProviderConfig.KmsConfig.KmsType.AWS_CONFIDENTIAL_SPACE
      awsRoleArn = "arn:aws:iam::123456789012:role/my-role"
      awsRoleSessionName = "my-session"
      awsRegion = "us-east-1"
      awsAudience = "https://example.com"
    }

    assertThat(config.kmsType)
      .isEqualTo(EventDataProviderConfig.KmsConfig.KmsType.AWS_CONFIDENTIAL_SPACE)
    assertThat(config.awsRoleArn).isEqualTo("arn:aws:iam::123456789012:role/my-role")
    assertThat(config.awsRoleSessionName).isEqualTo("my-session")
    assertThat(config.awsRegion).isEqualTo("us-east-1")
    assertThat(config.awsAudience).isEqualTo("https://example.com")
  }

  @Test
  fun `AWS_CONFIDENTIAL_SPACE type has empty GCP fields`() {
    val config = kmsConfig {
      kmsType = EventDataProviderConfig.KmsConfig.KmsType.AWS_CONFIDENTIAL_SPACE
      awsRoleArn = "arn:aws:iam::123456789012:role/my-role"
      awsRoleSessionName = "my-session"
      awsRegion = "us-east-1"
      awsAudience = "https://example.com"
    }

    assertThat(config.kmsAudience).isEmpty()
    assertThat(config.serviceAccount).isEmpty()
  }

  @Test
  fun `GCP type has empty AWS fields`() {
    val config = kmsConfig {
      kmsType = EventDataProviderConfig.KmsConfig.KmsType.GCP
      kmsAudience = "test-audience"
      serviceAccount = "test@example.com"
    }

    assertThat(config.awsRoleArn).isEmpty()
    assertThat(config.awsRoleSessionName).isEmpty()
    assertThat(config.awsRegion).isEmpty()
    assertThat(config.awsAudience).isEmpty()
  }

  @Test
  fun `UNSPECIFIED KmsType has empty AWS fields`() {
    val config = kmsConfig {
      kmsAudience = "test-audience"
      serviceAccount = "test@example.com"
    }

    assertThat(config.awsRoleArn).isEmpty()
    assertThat(config.awsRoleSessionName).isEmpty()
    assertThat(config.awsRegion).isEmpty()
    assertThat(config.awsAudience).isEmpty()
  }

  @Test
  fun `AWS KmsType with all fields present passes validation`() {
    val config = kmsConfig {
      kmsType = EventDataProviderConfig.KmsConfig.KmsType.AWS
      awsRoleArn = "arn:aws:iam::123456789012:role/my-role"
      awsRoleSessionName = "my-session"
      awsRegion = "us-east-1"
      awsAudience = "sts.amazonaws.com"
    }

    assertThat(config.kmsType).isEqualTo(EventDataProviderConfig.KmsConfig.KmsType.AWS)
    assertThat(config.awsRoleArn).isEqualTo("arn:aws:iam::123456789012:role/my-role")
    assertThat(config.awsRoleSessionName).isEqualTo("my-session")
    assertThat(config.awsRegion).isEqualTo("us-east-1")
    assertThat(config.awsAudience).isEqualTo("sts.amazonaws.com")
  }

  @Test
  fun `GCP KmsType skips AWS field validation`() {
    val config = kmsConfig {
      kmsType = EventDataProviderConfig.KmsConfig.KmsType.GCP
      kmsAudience = "test-audience"
      serviceAccount = "test@example.com"
    }

    assertThat(config.kmsType).isEqualTo(EventDataProviderConfig.KmsConfig.KmsType.GCP)
  }

  @Test
  fun `buildModelLineMap caches population spec and VID index by population-spec URI`() {
    runBlocking {
      val runner = ResultsFulfillerAppRunner()
      val eventTemplateTypeName = TestEvent.getDescriptor().fullName

      // A and B share a population-spec URI but use different descriptors; C uses a different
      // population-spec URI but shares a descriptor URI with A.
      val modelLineA =
        ResultsFulfillerAppRunner.ModelLineFlags().apply {
          modelLine = "modelLineA"
          populationSpecFileBlobUri = "population-spec-1"
          eventTemplateDescriptorBlobUri = "descriptor-x"
          this.eventTemplateTypeName = eventTemplateTypeName
        }
      val modelLineB =
        ResultsFulfillerAppRunner.ModelLineFlags().apply {
          modelLine = "modelLineB"
          populationSpecFileBlobUri = "population-spec-1"
          eventTemplateDescriptorBlobUri = "descriptor-y"
          this.eventTemplateTypeName = eventTemplateTypeName
        }
      val modelLineC =
        ResultsFulfillerAppRunner.ModelLineFlags().apply {
          modelLine = "modelLineC"
          populationSpecFileBlobUri = "population-spec-2"
          eventTemplateDescriptorBlobUri = "descriptor-x"
          this.eventTemplateTypeName = eventTemplateTypeName
        }

      var loadDescriptorSetCallCount = 0
      var loadPopulationSpecCallCount = 0
      var buildVidIndexMapCallCount = 0

      val result =
        runner.buildModelLineMap(
          modelLines = listOf(modelLineA, modelLineB, modelLineC),
          loadDescriptorSet = { _ ->
            loadDescriptorSetCallCount++
            listOf(TestEvent.getDescriptor())
          },
          loadPopulationSpec = { uri, _ ->
            loadPopulationSpecCallCount++
            populationSpec {
              subpopulations +=
                PopulationSpecKt.subPopulation {
                  vidRanges +=
                    PopulationSpecKt.vidRange {
                      startVid = uri.hashCode().toLong()
                      endVidInclusive = uri.hashCode().toLong()
                    }
                }
            }
          },
          buildVidIndexMap = { _ ->
            buildVidIndexMapCallCount++
            object : VidIndexMap by VidIndexMap.EMPTY {}
          },
        )

      assertThat(result.keys).containsExactly("modelLineA", "modelLineB", "modelLineC")
      // Each descriptor URI ("descriptor-x", "descriptor-y") and population-spec URI
      // ("population-spec-1", "population-spec-2") is loaded exactly once, despite 3 model lines.
      assertThat(loadDescriptorSetCallCount).isEqualTo(2)
      assertThat(loadPopulationSpecCallCount).isEqualTo(2)
      assertThat(buildVidIndexMapCallCount).isEqualTo(2)

      // A and B share a population-spec URI, so they share the same PopulationSpec and VidIndexMap
      // even though they use different event descriptors.
      assertThat(result.getValue("modelLineA").populationSpec)
        .isSameInstanceAs(result.getValue("modelLineB").populationSpec)
      assertThat(result.getValue("modelLineA").vidIndexMap)
        .isSameInstanceAs(result.getValue("modelLineB").vidIndexMap)

      // C uses a different population-spec URI, so it gets distinct instances.
      assertThat(result.getValue("modelLineC").populationSpec)
        .isNotSameInstanceAs(result.getValue("modelLineA").populationSpec)
      assertThat(result.getValue("modelLineC").vidIndexMap)
        .isNotSameInstanceAs(result.getValue("modelLineA").vidIndexMap)

      assertThat(result.values.map { it.localAlias }).containsExactly(null, null, null)
    }
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
