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

package org.wfanet.measurement.integration.deploy.gcloud

import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.Timeout
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.InProcessEdpAggregatorLifeOfAMeasurementIntegrationTest

/**
 * Implementation of [InProcessEdpAggregatorLifeOfAMeasurementIntegrationTest] for GCloud backends
 * with Spanner database.
 */
class GCloudEdpAggregatorLifeOfAMeasurementIntegrationTest :
  InProcessEdpAggregatorLifeOfAMeasurementIntegrationTest(
    kingdomDataServicesRule = KingdomDataServicesProviderRule(spannerEmulator),
    duchyDependenciesRule = SpannerDuchyDependencyProviderRule(spannerEmulator, ALL_DUCHY_NAMES),
    secureComputationDatabaseAdmin = spannerEmulator,
  ) {

  /**
   * Rule to enforce test method timeout.
   *
   * TODO(Kotlin/kotlinx.coroutines#3865): Switch back to CoroutinesTimeout when fixed.
   */
  @get:Rule val timeout: Timeout = Timeout.seconds(180)

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
