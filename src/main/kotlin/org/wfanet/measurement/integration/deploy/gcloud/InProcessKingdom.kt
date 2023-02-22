// Copyright 2021 The Cross-Media Measurement Authors
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

import java.time.Clock
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.integration.common.InProcessKingdom
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerDataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata

private const val REDIRECT_URI = "https://localhost:2048"

fun buildKingdomSpannerEmulatorDatabaseRule(): SpannerEmulatorDatabaseRule {
  return SpannerEmulatorDatabaseRule(Schemata.KINGDOM_CHANGELOG_PATH)
}

fun buildSpannerInProcessKingdom(
  databaseRule: SpannerEmulatorDatabaseRule,
  clock: Clock = Clock.systemUTC(),
  verboseGrpcLogging: Boolean = false
): InProcessKingdom {
  return InProcessKingdom(
    dataServicesProvider = {
      SpannerDataServices(clock, RandomIdGenerator(clock), databaseRule.databaseClient)
    },
    verboseGrpcLogging = verboseGrpcLogging,
    REDIRECT_URI,
  )
}
