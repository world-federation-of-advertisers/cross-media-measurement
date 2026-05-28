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

  private val specResolver: (String) -> SyntheticEventGroupSpec = { path ->
    when (path) {
      "small_data_spec.textproto" -> SPEC_A
      "small_data_spec_2.textproto" -> SPEC_B
      else -> error("Unknown spec path: $path")
    }
  }

  @Test
  fun `toEventGroupMap returns correct map structure`() {
    val result = ImpressionTestDataConfigs.toEventGroupMap(CONFIG, specResolver)

    assertThat(result).hasSize(4)

    val legacy = result["edpa-eg-reference-id-1"]
    assertIs<EventGroupConfig.LegacySpec>(legacy)
    assertThat(legacy.spec).isEqualTo(SPEC_A)

    val singleEntityKey = result["creative-id-edpa-eg-creative-id-1"]
    assertIs<EventGroupConfig.MultiEntityKey>(singleEntityKey)
    assertThat(singleEntityKey.entityKeySpecs).hasSize(1)
    assertThat(singleEntityKey.entityKeySpecs[0].entityKey.entityType).isEqualTo("creative-id")
    assertThat(singleEntityKey.entityKeySpecs[0].entityKey.entityId)
      .isEqualTo("edpa-eg-creative-id-1")
    assertThat(singleEntityKey.entityKeySpecs[0].entityMetadata).isEqualTo(ENTITY_METADATA)

    val multiCreative = result["multi-creative"]
    assertIs<EventGroupConfig.MultiEntityKey>(multiCreative)
    assertThat(multiCreative.entityKeySpecs).hasSize(2)
    assertThat(multiCreative.entityKeySpecs[0].entityKey.entityId)
      .isEqualTo("edpa-eg-multi-creative-1")
    assertThat(multiCreative.entityKeySpecs[0].spec).isEqualTo(SPEC_A)
    assertThat(multiCreative.entityKeySpecs[1].entityKey.entityId)
      .isEqualTo("edpa-eg-multi-creative-2")
    assertThat(multiCreative.entityKeySpecs[1].spec).isEqualTo(SPEC_B)

    val edpaMeta = result["edpa-eg-reference-id-2"]
    assertIs<EventGroupConfig.LegacySpec>(edpaMeta)
  }

  @Test
  fun `toFlatEventGroupMap flattens multi-entity-key entries`() {
    val result = ImpressionTestDataConfigs.toFlatEventGroupMap(CONFIG, specResolver)

    assertThat(result).hasSize(5)
    assertThat(result).containsKey("edpa-eg-reference-id-1")
    assertThat(result).containsKey("creative-id-edpa-eg-creative-id-1")
    assertThat(result).containsKey("creative-id-edpa-eg-multi-creative-1")
    assertThat(result).containsKey("creative-id-edpa-eg-multi-creative-2")
    assertThat(result).containsKey("edpa-eg-reference-id-2")

    val multiA = result["creative-id-edpa-eg-multi-creative-1"]
    assertIs<EventGroupConfig.MultiEntityKey>(multiA)
    assertThat(multiA.entityKeySpecs).hasSize(1)
    assertThat(multiA.entityKeySpecs[0].spec).isEqualTo(SPEC_A)

    val multiB = result["creative-id-edpa-eg-multi-creative-2"]
    assertIs<EventGroupConfig.MultiEntityKey>(multiB)
    assertThat(multiB.entityKeySpecs).hasSize(1)
    assertThat(multiB.entityKeySpecs[0].spec).isEqualTo(SPEC_B)
  }

  @Test
  fun `toEventGroupMap legacy event group has no entity metadata`() {
    val result = ImpressionTestDataConfigs.toEventGroupMap(CONFIG, specResolver)

    val legacy = result["edpa-eg-reference-id-1"]
    assertIs<EventGroupConfig.LegacySpec>(legacy)
  }

  @Test
  fun `toFlatEventGroupMap preserves entity metadata on resolved entries`() {
    val result = ImpressionTestDataConfigs.toFlatEventGroupMap(CONFIG, specResolver)

    val multiA = result["creative-id-edpa-eg-multi-creative-1"]
    assertIs<EventGroupConfig.MultiEntityKey>(multiA)
    assertThat(multiA.entityKeySpecs[0].entityMetadata).isEqualTo(ENTITY_METADATA)
  }

  @Test
  fun `buildSpecsFromConfig produces correct output keys for legacy event group`() {
    val specs = GenerateSyntheticData.buildSpecsFromConfig(CONFIG, "edp7")

    val legacy = specs.first { it.eventGroupReferenceId == "edpa-eg-reference-id-1" }
    assertThat(legacy.outputKey).isEmpty()
    assertThat(legacy.subSpecs).hasSize(1)
    assertThat(legacy.subSpecs[0].entityKey.entityType).isEqualTo("campaign")
  }

  @Test
  fun `buildSpecsFromConfig produces correct output keys for entity-key event groups`() {
    val specs = GenerateSyntheticData.buildSpecsFromConfig(CONFIG, "edp7")

    val creative = specs.first { it.eventGroupReferenceId == "creative-id-edpa-eg-creative-id-1" }
    assertThat(creative.outputKey).isEqualTo("creative")
    assertThat(creative.subSpecs).hasSize(1)

    val multi = specs.first { it.eventGroupReferenceId == "multi-creative" }
    assertThat(multi.outputKey).isEqualTo("multi-creative")
    assertThat(multi.subSpecs).hasSize(2)
    assertThat(multi.subSpecs[0].entityKey.entityId).isEqualTo("edpa-eg-multi-creative-1")
    assertThat(multi.subSpecs[1].entityKey.entityId).isEqualTo("edpa-eg-multi-creative-2")
  }

  @Test
  fun `buildSpecsFromConfig filters by edp name`() {
    val edp7Specs = GenerateSyntheticData.buildSpecsFromConfig(CONFIG, "edp7")
    val metaSpecs = GenerateSyntheticData.buildSpecsFromConfig(CONFIG, "edpa_meta")

    assertThat(edp7Specs).hasSize(3)
    assertThat(metaSpecs).hasSize(1)
    assertThat(metaSpecs[0].eventGroupReferenceId).isEqualTo("edpa-eg-reference-id-2")
  }

  @Test
  fun `buildSpecsFromConfig fails for unknown edp`() {
    assertFailsWith<IllegalArgumentException> {
      GenerateSyntheticData.buildSpecsFromConfig(CONFIG, "unknown-edp")
    }
  }

  companion object {
    private val SPEC_A = SyntheticEventGroupSpec.getDefaultInstance()
    private val SPEC_B = SyntheticEventGroupSpec.newBuilder().setDescription("spec-b").build()

    private val ENTITY_METADATA = struct {
      fields["placement"] = value { stringValue = "homepage_top" }
    }

    private val CONFIG = cloudTestDataConfig {
      populationSpecResourcePath = "small_population_spec.textproto"
      eventGroups += syntheticEventGroup {
        eventGroupReferenceId = "edpa-eg-reference-id-1"
        dataSpecResourcePath = "small_data_spec.textproto"
        edpName = "edp7"
      }
      eventGroups += syntheticEventGroup {
        eventGroupReferenceId = "creative-id-edpa-eg-creative-id-1"
        edpName = "edp7"
        outputKey = "creative"
        entityMetadata = ENTITY_METADATA
        entityKeySpecs += entityKeySpec {
          entityType = "creative-id"
          entityId = "edpa-eg-creative-id-1"
          dataSpecResourcePath = "small_data_spec.textproto"
        }
      }
      eventGroups += syntheticEventGroup {
        eventGroupReferenceId = "multi-creative"
        edpName = "edp7"
        outputKey = "multi-creative"
        entityMetadata = ENTITY_METADATA
        entityKeySpecs += entityKeySpec {
          entityType = "creative-id"
          entityId = "edpa-eg-multi-creative-1"
          dataSpecResourcePath = "small_data_spec.textproto"
        }
        entityKeySpecs += entityKeySpec {
          entityType = "creative-id"
          entityId = "edpa-eg-multi-creative-2"
          dataSpecResourcePath = "small_data_spec_2.textproto"
        }
      }
      eventGroups += syntheticEventGroup {
        eventGroupReferenceId = "edpa-eg-reference-id-2"
        dataSpecResourcePath = "small_data_spec.textproto"
        edpName = "edpa_meta"
      }
    }
  }
}
