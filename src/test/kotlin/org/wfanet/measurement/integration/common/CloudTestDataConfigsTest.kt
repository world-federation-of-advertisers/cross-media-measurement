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
import kotlin.test.assertIs
import org.junit.Test
import org.measurement.integration.k8s.testing.CloudTestDataConfig
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec

class CloudTestDataConfigsTest {

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

  private fun buildConfig(): CloudTestDataConfig {
    return CloudTestDataConfig.newBuilder()
      .setPopulationSpecResourcePath("small_population_spec.textproto")
      .addEventGroups(
        CloudTestDataConfig.SyntheticEventGroup.newBuilder()
          .setEventGroupReferenceId("edpa-eg-reference-id-1")
          .setDataSpecResourcePath("small_data_spec.textproto")
          .setEdpName("edp7")
      )
      .addEventGroups(
        CloudTestDataConfig.SyntheticEventGroup.newBuilder()
          .setEventGroupReferenceId("creative-id-edpa-eg-creative-id-1")
          .setEdpName("edp7")
          .setEntityMetadata(entityMetadata)
          .addEntityKeySpecs(
            CloudTestDataConfig.EntityKeySpec.newBuilder()
              .setEntityType("creative-id")
              .setEntityId("edpa-eg-creative-id-1")
              .setDataSpecResourcePath("small_data_spec.textproto")
          )
      )
      .addEventGroups(
        CloudTestDataConfig.SyntheticEventGroup.newBuilder()
          .setEventGroupReferenceId("multi-creative")
          .setEdpName("edp7")
          .setEntityMetadata(entityMetadata)
          .addEntityKeySpecs(
            CloudTestDataConfig.EntityKeySpec.newBuilder()
              .setEntityType("creative-id")
              .setEntityId("edpa-eg-multi-creative-1")
              .setDataSpecResourcePath("small_data_spec.textproto")
          )
          .addEntityKeySpecs(
            CloudTestDataConfig.EntityKeySpec.newBuilder()
              .setEntityType("creative-id")
              .setEntityId("edpa-eg-multi-creative-2")
              .setDataSpecResourcePath("small_data_spec_2.textproto")
          )
      )
      .addEventGroups(
        CloudTestDataConfig.SyntheticEventGroup.newBuilder()
          .setEventGroupReferenceId("edpa-eg-reference-id-2")
          .setDataSpecResourcePath("small_data_spec.textproto")
          .setEdpName("edpa_meta")
      )
      .build()
  }

  @Test
  fun `toSyntheticEventGroupMap returns correct map structure`() {
    val config = buildConfig()
    val result = CloudTestDataConfigs.toSyntheticEventGroupMap(config, specResolver)

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
  fun `toResolvedEventGroupMap flattens multi-entity-key entries`() {
    val config = buildConfig()
    val result = CloudTestDataConfigs.toResolvedEventGroupMap(config, specResolver)

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
  fun `toGenerateSyntheticDataArgs groups by EDP and produces correct CLI args`() {
    val config = buildConfig()
    val result = CloudTestDataConfigs.toGenerateSyntheticDataArgs(config)

    assertThat(result).hasSize(2)
    assertThat(result).containsKey("edp7")
    assertThat(result).containsKey("edpa_meta")

    val edp7Args = result["edp7"]!!
    assertThat(edp7Args).contains("--event-group-reference-id=edpa-eg-reference-id-1")
    assertThat(edp7Args).contains("--data-spec-resource-path=small_data_spec.textproto")
    assertThat(edp7Args).contains("--entity-key-type=campaign")
    assertThat(edp7Args).contains("--entity-key-id=edpa-eg-reference-id-1")

    assertThat(edp7Args).contains("--event-group-reference-id=creative-id-edpa-eg-creative-id-1")
    assertThat(edp7Args).contains("--entity-key-type=creative-id")
    assertThat(edp7Args).contains("--entity-key-id=edpa-eg-creative-id-1")

    assertThat(edp7Args).contains("--event-group-reference-id=creative-id-edpa-eg-multi-creative-1")
    assertThat(edp7Args).contains("--event-group-reference-id=creative-id-edpa-eg-multi-creative-2")

    val edpaMetaArgs = result["edpa_meta"]!!
    assertThat(edpaMetaArgs).contains("--event-group-reference-id=edpa-eg-reference-id-2")
    assertThat(edpaMetaArgs).contains("--data-spec-resource-path=small_data_spec.textproto")
  }

  @Test
  fun `toSyntheticEventGroupMap legacy event group has no entity metadata`() {
    val config = buildConfig()
    val result = CloudTestDataConfigs.toSyntheticEventGroupMap(config, specResolver)

    val legacy = result["edpa-eg-reference-id-1"]
    assertIs<EventGroupConfig.LegacySpec>(legacy)
  }

  @Test
  fun `toResolvedEventGroupMap preserves entity metadata on resolved entries`() {
    val config = buildConfig()
    val result = CloudTestDataConfigs.toResolvedEventGroupMap(config, specResolver)

    val multiA = result["creative-id-edpa-eg-multi-creative-1"]
    assertIs<EventGroupConfig.MultiEntityKey>(multiA)
    assertThat(multiA.entityKeySpecs[0].entityMetadata).isEqualTo(entityMetadata)
  }
}
