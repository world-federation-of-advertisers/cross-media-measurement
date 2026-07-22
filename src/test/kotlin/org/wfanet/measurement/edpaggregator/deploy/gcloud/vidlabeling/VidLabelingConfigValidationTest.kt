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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.util.Durations
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigKt
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfig
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn

@RunWith(JUnit4::class)
class VidLabelingConfigValidationTest {
  @Test
  fun `passes for a fully-populated model line config`() {
    requireValidModelLineConfigs(configWith(validModelLineConfig()))
  }

  @Test
  fun `throws when a model line omits event_id_id`() {
    val config =
      configWith(
        validModelLineConfig().toBuilder().apply { clearLabelerInputFieldMapping() }.build()
      )

    val exception =
      assertFailsWith<IllegalArgumentException> { requireValidModelLineConfigs(config) }
    assertThat(exception).hasMessageThat().contains("event_id.id")
    assertThat(exception).hasMessageThat().contains(MODEL_LINE)
  }

  @Test
  fun `throws when a model line omits event_template_descriptor_blob_uri`() {
    val config =
      configWith(
        validModelLineConfig().toBuilder().apply { clearEventTemplateDescriptorBlobUri() }.build()
      )

    val exception =
      assertFailsWith<IllegalArgumentException> { requireValidModelLineConfigs(config) }
    assertThat(exception).hasMessageThat().contains("event_template_descriptor_blob_uri")
  }

  @Test
  fun `throws when a model line has no entity-key mapping`() {
    val config =
      configWith(
        validModelLineConfig().toBuilder().apply { clearRequiredEntityKeyFieldMapping() }.build()
      )

    val exception =
      assertFailsWith<IllegalArgumentException> { requireValidModelLineConfigs(config) }
    assertThat(exception).hasMessageThat().contains("entity_key_field_mapping")
  }

  @Test
  fun `staleness passes at the minimum threshold`() {
    requireValidStalenessThreshold(configWithStaleness(Durations.fromHours(24)))
  }

  @Test
  fun `staleness passes above the minimum threshold`() {
    requireValidStalenessThreshold(configWithStaleness(Durations.fromDays(2)))
  }

  @Test
  fun `staleness throws when not set`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        requireValidStalenessThreshold(vidLabelingConfig { dataProvider = DATA_PROVIDER })
      }
    assertThat(exception).hasMessageThat().contains("staleness_threshold must be set")
    assertThat(exception).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `staleness throws below the minimum threshold`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        requireValidStalenessThreshold(configWithStaleness(Durations.fromHours(23)))
      }
    assertThat(exception).hasMessageThat().contains("at least")
    assertThat(exception).hasMessageThat().contains(DATA_PROVIDER)
  }

  private fun configWithStaleness(threshold: com.google.protobuf.Duration): VidLabelingConfig =
    vidLabelingConfig {
      dataProvider = DATA_PROVIDER
      stalenessThreshold = threshold
    }

  private fun configWith(modelLineConfig: VidLabelingConfig.ModelLineConfig): VidLabelingConfig =
    vidLabelingConfig {
      dataProvider = DATA_PROVIDER
      modelLineConfigs[MODEL_LINE] = modelLineConfig
    }

  private fun validModelLineConfig(): VidLabelingConfig.ModelLineConfig =
    VidLabelingConfigKt.modelLineConfig {
      labelerInputFieldMapping +=
        LabelerInputFieldMapping.newBuilder()
          .setFieldPath("event_id.id")
          .setScalar(ScalarColumn.newBuilder().setColumn("event_id_col"))
          .build()
      eventTemplateDescriptorBlobUri = "gs://descriptors/event-template-set.binpb"
      eventTemplateType = "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
      requiredEntityKeyFieldMapping["person"] = "person_col"
    }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp7"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
  }
}
