// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.struct
import com.google.protobuf.value
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import org.junit.Test
import org.measurement.integration.k8s.testing.CloudTestDataConfigKt.entityKeySpec
import org.measurement.integration.k8s.testing.CloudTestDataConfigKt.syntheticEventGroup
import org.measurement.integration.k8s.testing.cloudTestDataConfig
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.loadtest.edpaggregator.tools.GenerateSyntheticData

class ImpressionTestDataConfigsTest {

  private val specA = SyntheticEventGroupSpec.getDefaultInstance()
  private val specB = SyntheticEventGroupSpec.newBuilder().build()

  private val specResolver: (String) -> SyntheticEventGroupSpec = { path ->
    when (path) {
      "small_data_spec.textproto" -> specA
      "small_data_spec_2.textproto" -> specB
      else -> error("Unknown spec path: $path")
    }
  }

  private val entityMetadata = struct {
    fields["placement"] = value { stringValue = "homepage_top" }
  }

  @Test
  fun `toEventGroupMap returns correct map structure`() {
    val config = buildConfig()
    val result = ImpressionTestDataConfigs.toEventGroupMap(config, specResolver)

    assertThat(result).hasSize(4)

    val legacy = result["edpa-eg-reference-id-1"]
    assertIs<EventGroupConfig.LegacySpec>(legacy)
    assertThat(legacy.spec).isEqualTo(specA)

    val singleEntityKey = result["creative-id-edpa-eg-creative-id-1"]
    assertIs<EventGroupConfig.MultiEntityKey>(singleEntityKey)
    assertThat(singleEntityKey.entityKeySpecs).hasSize(1)
    assertThat(singleEntityKey.entityKeySpecs[0].entityKey.entityType).isEqualTo("creative-id")
    assertThat(singleEntityKey.entityKeySpecs[0].entityKey.entityId)
      .isEqualTo("edpa-eg-creative-id-1")
    assertThat(singleEntityKey.entityKeySpecs[0].entityMetadata).isEqualTo(entityMetadata)

    val multiCreative = result["multi-creative"]
    assertIs<EventGroupConfig.MultiEntityKey>(multiCreative)
    assertThat(multiCreative.entityKeySpecs).hasSize(2)
    assertThat(multiCreative.entityKeySpecs[0].entityKey.entityId)
      .isEqualTo("edpa-eg-multi-creative-1")
    assertThat(multiCreative.entityKeySpecs[0].spec).isEqualTo(specA)
    assertThat(multiCreative.entityKeySpecs[1].entityKey.entityId)
      .isEqualTo("edpa-eg-multi-creative-2")
    assertThat(multiCreative.entityKeySpecs[1].spec).isEqualTo(specB)

    val edpaMeta = result["edpa-eg-reference-id-2"]
    assertIs<EventGroupConfig.LegacySpec>(edpaMeta)
  }

  @Test
  fun `toFlatEventGroupMap flattens multi-entity-key entries`() {
    val config = buildConfig()
    val result = ImpressionTestDataConfigs.toFlatEventGroupMap(config, specResolver)

    assertThat(result).hasSize(5)
    assertThat(result).containsKey("edpa-eg-reference-id-1")
    assertThat(result).containsKey("creative-id-edpa-eg-creative-id-1")
    assertThat(result).containsKey("creative-id-edpa-eg-multi-creative-1")
    assertThat(result).containsKey("creative-id-edpa-eg-multi-creative-2")
    assertThat(result).containsKey("edpa-eg-reference-id-2")

    val multiA = result["creative-id-edpa-eg-multi-creative-1"]
    assertIs<EventGroupConfig.MultiEntityKey>(multiA)
    assertThat(multiA.entityKeySpecs).hasSize(1)
    assertThat(multiA.entityKeySpecs[0].spec).isEqualTo(specA)

    val multiB = result["creative-id-edpa-eg-multi-creative-2"]
    assertIs<EventGroupConfig.MultiEntityKey>(multiB)
    assertThat(multiB.entityKeySpecs).hasSize(1)
    assertThat(multiB.entityKeySpecs[0].spec).isEqualTo(specB)
  }

  @Test
  fun `toEventGroupMap legacy event group has no entity metadata`() {
    val config = buildConfig()
    val result = ImpressionTestDataConfigs.toEventGroupMap(config, specResolver)

    val legacy = result["edpa-eg-reference-id-1"]
    assertIs<EventGroupConfig.LegacySpec>(legacy)
  }

  @Test
  fun `toFlatEventGroupMap preserves entity metadata on resolved entries`() {
    val config = buildConfig()
    val result = ImpressionTestDataConfigs.toFlatEventGroupMap(config, specResolver)

    val multiA = result["creative-id-edpa-eg-multi-creative-1"]
    assertIs<EventGroupConfig.MultiEntityKey>(multiA)
    assertThat(multiA.entityKeySpecs[0].entityMetadata).isEqualTo(entityMetadata)
  }

  @Test
  fun `buildSpecsFromConfig produces correct output keys for legacy event group`() {
    val config = buildConfig()
    val specs = GenerateSyntheticData.buildSpecsFromConfig(config, "edp7")

    val legacy = specs.first { it.eventGroupReferenceId == "edpa-eg-reference-id-1" }
    assertThat(legacy.outputKey).isEmpty()
    assertThat(legacy.entityKey.entityType).isEqualTo("campaign")
  }

  @Test
  fun `buildSpecsFromConfig produces correct output keys for entity-key event groups`() {
    val config = buildConfig()
    val specs = GenerateSyntheticData.buildSpecsFromConfig(config, "edp7")

    val creative = specs.first { it.eventGroupReferenceId == "creative-id-edpa-eg-creative-id-1" }
    assertThat(creative.outputKey).isEqualTo("creative")

    val multi1 = specs.first { it.eventGroupReferenceId == "creative-id-edpa-eg-multi-creative-1" }
    assertThat(multi1.outputKey).isEqualTo("multi-creative")

    val multi2 = specs.first { it.eventGroupReferenceId == "creative-id-edpa-eg-multi-creative-2" }
    assertThat(multi2.outputKey).isEqualTo("multi-creative-2")
  }

  @Test
  fun `buildSpecsFromConfig filters by edp name`() {
    val config = buildConfig()
    val edp7Specs = GenerateSyntheticData.buildSpecsFromConfig(config, "edp7")
    val metaSpecs = GenerateSyntheticData.buildSpecsFromConfig(config, "edpa_meta")

    assertThat(edp7Specs).hasSize(4)
    assertThat(metaSpecs).hasSize(1)
    assertThat(metaSpecs[0].eventGroupReferenceId).isEqualTo("edpa-eg-reference-id-2")
  }

  @Test
  fun `buildSpecsFromConfig fails for unknown edp`() {
    val config = buildConfig()
    assertFailsWith<IllegalArgumentException> {
      GenerateSyntheticData.buildSpecsFromConfig(config, "unknown-edp")
    }
  }

  private fun buildConfig() = cloudTestDataConfig {
    populationSpecResourcePath = "small_population_spec.textproto"
    eventGroups += syntheticEventGroup {
      eventGroupReferenceId = "edpa-eg-reference-id-1"
      dataSpecResourcePath = "small_data_spec.textproto"
      edpName = "edp7"
    }
    eventGroups += syntheticEventGroup {
      eventGroupReferenceId = "creative-id-edpa-eg-creative-id-1"
      edpName = "edp7"
      this.entityMetadata = this@ImpressionTestDataConfigsTest.entityMetadata
      entityKeySpecs += entityKeySpec {
        entityType = "creative-id"
        entityId = "edpa-eg-creative-id-1"
        dataSpecResourcePath = "small_data_spec.textproto"
        outputKey = "creative"
      }
    }
    eventGroups += syntheticEventGroup {
      eventGroupReferenceId = "multi-creative"
      edpName = "edp7"
      this.entityMetadata = this@ImpressionTestDataConfigsTest.entityMetadata
      entityKeySpecs += entityKeySpec {
        entityType = "creative-id"
        entityId = "edpa-eg-multi-creative-1"
        dataSpecResourcePath = "small_data_spec.textproto"
        outputKey = "multi-creative"
      }
      entityKeySpecs += entityKeySpec {
        entityType = "creative-id"
        entityId = "edpa-eg-multi-creative-2"
        dataSpecResourcePath = "small_data_spec_2.textproto"
        outputKey = "multi-creative-2"
      }
    }
    eventGroups += syntheticEventGroup {
      eventGroupReferenceId = "edpa-eg-reference-id-2"
      dataSpecResourcePath = "small_data_spec.textproto"
      edpName = "edpa_meta"
    }
  }
}
