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
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigKt
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfig

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

  private fun configWith(modelLineConfig: VidLabelingConfig.ModelLineConfig): VidLabelingConfig =
    vidLabelingConfig {
      dataProvider = DATA_PROVIDER
      modelLineConfigs[MODEL_LINE] = modelLineConfig
    }

  private fun validModelLineConfig(): VidLabelingConfig.ModelLineConfig =
    VidLabelingConfigKt.modelLineConfig {
      labelerInputFieldMapping["event_id.id"] = "event_id_col"
      eventTemplateDescriptorBlobUri = "gs://descriptors/event-template-set.binpb"
      eventTemplateType = "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
      requiredEntityKeyFieldMapping["person"] = "person_col"
    }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp7"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
  }
}
