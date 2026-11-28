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
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Tests a TrusTEE v2 multimeasurement end to end: the `MeasurementConsumer` creates it through the
 * Kingdom public API, the EDP Aggregator fulfills one `Requisition` per base vector, and the
 * TrusTEE Duchy evaluates the `Query`s over those vectors.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessEdpAggregatorMultimeasurementTest(
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
    // Makes every DataProvider fulfill without noise. `InProcessDuchy` runs the TrusTEE v2 Mill
    // with `NoiseMechanism.NONE`, so the expected results are exact.
    resultMinimumThresholdsByEdp = SUPPRESSING_RESULT_MINIMUM_THRESHOLDS_BY_EDP,
  ) {

  @Test
  fun `createMeasurement for a TrusTEE v2 multimeasurement completes with exact results`() =
    runBlocking {
      mcSimulator.testMultimeasurement(
        "trustee-v2-multimeasurement-1234",
        eventGroupFilter = { it.eventGroupReferenceId in VECTOR_EVENT_GROUP_REF_IDS },
      )
    }

  companion object {
    /**
     * One base vector per entry, from two TrusTEE v2 DataProviders whose data come from different
     * synthetic specs, so that their union differs from either vector.
     */
    private val VECTOR_EVENT_GROUP_REF_IDS =
      setOf(TRUSTEE_V2_SPEC_2_EVENT_GROUP_REF_ID, "ad_group/edpa-eg-reference-id-3")
  }
}
