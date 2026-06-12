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
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt

// TODO(world-federation-of-advertisers/cross-media-measurement#3899): Add full integration tests
// using FunctionsFrameworkInvokerProcess once the following prerequisites are met:
//   1. Create testing/InvokeVidLabelingDispatcherFunction java_binary (similar to
//      dataavailability/testing/InvokeDataAvailabilitySyncFunction).
//   2. Add poolAssignmentJobStub to the VidLabelingDispatcher constructor call in
//      VidLabelingDispatcherFunction (currently missing, which would cause a compile error).
//   3. Add VidLabelingConfig test fixtures with file-system storage params.
//
// Until then, this file covers the VidLabelingConfig validation that the function performs
// before delegating to VidLabelingDispatcher (which is thoroughly tested in
// VidLabelingDispatcherTest).

@RunWith(JUnit4::class)
class VidLabelingDispatcherFunctionTest {

  @Test
  fun `config with zero numberOfShards fails validation`() {
    val config = vidLabelingConfig {
      dataProvider = "dataProviders/edp1"
      numberOfShards = 0
      modelSuite = "modelProviders/mp1/modelSuites/ms1"
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        require(config.numberOfShards > 0) {
          "number_of_shards must be positive for data provider: ${config.dataProvider}"
        }
      }
    assertThat(exception).hasMessageThat().contains("number_of_shards must be positive")
    assertThat(exception).hasMessageThat().contains("dataProviders/edp1")
  }

  @Test
  fun `config with positive numberOfShards passes validation`() {
    val config = vidLabelingConfig {
      dataProvider = "dataProviders/edp1"
      numberOfShards = 3
      modelSuite = "modelProviders/mp1/modelSuites/ms1"
    }

    require(config.numberOfShards > 0) {
      "number_of_shards must be positive for data provider: ${config.dataProvider}"
    }
  }

  @Test
  fun `convertModelLineConfigs maps config model line configs to VidLabelerParams model line configs`() {
    val configModelLines: Map<String, VidLabelingConfig.ModelLineConfig> =
      mapOf(
        "modelProviders/mp1/modelSuites/ms1/modelLines/ml1" to
          VidLabelingConfigKt.modelLineConfig {
            labelerInputFieldMapping["age"] = "user_age"
            labelerInputFieldMapping["gender"] = "user_gender"
          },
        "modelProviders/mp1/modelSuites/ms1/modelLines/ml2" to
          VidLabelingConfigKt.modelLineConfig {
            labelerInputFieldMapping["age"] = "user_age"
            eventTemplateFieldMapping["event_type"] = "event_category"
          },
      )

    val result: Map<String, VidLabelerParams.ModelLineConfig> =
      configModelLines.mapValues { (_, configModelLine) ->
        VidLabelerParamsKt.modelLineConfig {
          labelerInputFieldMapping.putAll(configModelLine.labelerInputFieldMappingMap)
          eventTemplateFieldMapping.putAll(configModelLine.eventTemplateFieldMappingMap)
        }
      }

    assertThat(result).hasSize(2)
    val ml1Config = result["modelProviders/mp1/modelSuites/ms1/modelLines/ml1"]!!
    assertThat(ml1Config.labelerInputFieldMappingMap).containsEntry("age", "user_age")
    assertThat(ml1Config.labelerInputFieldMappingMap).containsEntry("gender", "user_gender")

    val ml2Config = result["modelProviders/mp1/modelSuites/ms1/modelLines/ml2"]!!
    assertThat(ml2Config.labelerInputFieldMappingMap).containsEntry("age", "user_age")
    assertThat(ml2Config.eventTemplateFieldMappingMap).containsEntry("event_type", "event_category")
  }
}
