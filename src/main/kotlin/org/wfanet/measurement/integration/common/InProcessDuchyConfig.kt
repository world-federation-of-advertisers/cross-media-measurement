// Copyright 2022 The Cross-Media Measurement Authors
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

import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig

class InProcessDuchyConfig : TestRule {
  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        DuchyIds.setForTest(ALL_DUCHY_NAMES)
        Llv2ProtocolConfig.setForTest(
          LLV2_PROTOCOL_CONFIG_CONFIG.protocolConfig,
          LLV2_PROTOCOL_CONFIG_CONFIG.duchyProtocolConfig
        )
        DuchyInfo.setForTest(ALL_DUCHY_NAMES.toSet())
        base.evaluate()
      }
    }
  }
}
