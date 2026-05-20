// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.common.service

import java.nio.file.Paths
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.testing.v2.ImpressionQualificationFiltersServiceTest

@RunWith(JUnit4::class)
class ImpressionQualificationFiltersServiceTest :
  ImpressionQualificationFiltersServiceTest<ImpressionQualificationFiltersService>() {

  override fun newService(): ImpressionQualificationFiltersService {
    val impressionQualificationFilterMapping =
      ImpressionQualificationFilterMapping(IQF_CONFIG, TestEvent.getDescriptor())

    return ImpressionQualificationFiltersService(impressionQualificationFilterMapping)
  }

  companion object {

    private val CONFIG_PATH =
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    private val IQF_CONFIG: ImpressionQualificationFilterConfig by lazy {
      val configFile =
        getRuntimePath(CONFIG_PATH.resolve("impression_qualification_filter_config.textproto"))!!
          .toFile()
      parseTextProto(configFile, ImpressionQualificationFilterConfig.getDefaultInstance())
    }
  }
}
