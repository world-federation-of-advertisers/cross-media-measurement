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

import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

abstract class InProcessEdpAggregatorTrusteeMeasurementTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
) :
  InProcessEdpAggregatorLifeOfAMeasurementIntegrationTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    secureComputationDatabaseAdmin,
    hmssEnabled = false,
    trusTeeEnabled = true,
  ) {

  @Test
  fun `create a TrusTee reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testReachOnly(
        "1234",
        ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE,
        eventGroupFilter = {
          it.eventGroupReferenceId != MULTIPARTY_NO_NOISE_EDP_EVENT_GROUP_REF_ID &&
            it.eventGroupReferenceId !in MULTI_ENTITY_KEY_REF_IDS
        },
      )
    }

  @Test
  fun `create a TrusTee RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testReachAndFrequency(
        "1234",
        ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE,
        eventGroupFilter = {
          it.eventGroupReferenceId != MULTIPARTY_NO_NOISE_EDP_EVENT_GROUP_REF_ID &&
            it.eventGroupReferenceId !in MULTI_ENTITY_KEY_REF_IDS
        },
      )
    }

  @Test
  fun `TrusTee measurement fails when EDP requires no noise`() {
    assertFailsWith<IllegalStateException>("Expected measurement to fail") {
      runBlocking {
        mcSimulator.testReachOnly(
          "trustee-no-noise-1234",
          ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE,
          eventGroupFilter = { it.eventGroupReferenceId in TRUSTEE_NO_NOISE_EVENT_GROUP_REF_IDS },
        )
      }
    }
  }
}
