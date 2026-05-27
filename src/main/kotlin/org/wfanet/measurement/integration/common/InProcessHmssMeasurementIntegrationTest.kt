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

import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

abstract class InProcessHmssMeasurementIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
) :
  InProcessLifeOfAMeasurementIntegrationTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    hmssEnabled = true,
    trusTeeEnabled = false,
  ) {

  @Test
  fun `create a Hmss RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testReachAndFrequency(
        "1234",
        ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE,
      )
    }

  @Test
  fun `create a Hmss reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testReachOnly(
        "1234",
        ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE,
      )
    }

  @Test
  fun `create a Hmss RF measurement of invalid params and check the result contains error info`() =
    runBlocking {
      mcSimulator.testInvalidReachAndFrequency(
        "1234",
        ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE,
      )
    }

  @Test
  fun `create a Hmss RF measurement with wrapping vid interval and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testReachAndFrequency(
        "1234",
        ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE,
        vidSamplingInterval {
          start = 0.5f
          width = 1.0f
        },
      )
    }
}
