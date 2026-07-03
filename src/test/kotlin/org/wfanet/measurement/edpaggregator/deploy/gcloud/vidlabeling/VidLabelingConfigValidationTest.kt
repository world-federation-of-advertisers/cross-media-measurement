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
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigKt
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfig
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn

@RunWith(JUnit4::class)
class VidLabelingConfigValidationTest {
  @Test
  fun `requireValidModelLineConfigs passes when every model line maps event_id_id`() {
    val config = vidLabelingConfig {
      dataProvider = DATA_PROVIDER
      modelLineConfigs[MODEL_LINE] =
        VidLabelingConfigKt.modelLineConfig {
          labelerInputFieldMapping +=
            LabelerInputFieldMapping.newBuilder()
              .setFieldPath("event_id.id")
              .setScalar(ScalarColumn.newBuilder().setColumn("event_id_col"))
              .build()
        }
    }

    requireValidModelLineConfigs(config)
  }

  @Test
  fun `requireValidModelLineConfigs throws when a model line omits event_id_id`() {
    val config = vidLabelingConfig {
      dataProvider = DATA_PROVIDER
      modelLineConfigs[MODEL_LINE] =
        VidLabelingConfigKt.modelLineConfig {
          labelerInputFieldMapping +=
            LabelerInputFieldMapping.newBuilder()
              .setFieldPath("age")
              .setScalar(ScalarColumn.newBuilder().setColumn("age_col"))
              .build()
        }
    }

    val exception =
      assertFailsWith<IllegalArgumentException> { requireValidModelLineConfigs(config) }
    assertThat(exception).hasMessageThat().contains("event_id.id")
    assertThat(exception).hasMessageThat().contains(MODEL_LINE)
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp7"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
  }
}
