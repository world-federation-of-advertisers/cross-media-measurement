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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing

import java.time.Clock
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.kingdom.deploy.common.service.KingdomDataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerDataServices

class KingdomDataServicesProviderRule : ProviderRule<KingdomDataServices> {
  private val spannerDatabase = SpannerEmulatorDatabaseRule(Schemata.KINGDOM_CHANGELOG_PATH)
  private val clock = Clock.systemUTC()
  private val idGenerator = RandomIdGenerator(clock)
  private val databaseClient: AsyncDatabaseClient by lazy { spannerDatabase.databaseClient }

  override val value by lazy {
    SpannerDataServices(clock, idGenerator, databaseClient).buildDataServices()
  }

  override fun apply(base: Statement, description: Description): Statement {
    return spannerDatabase.apply(base, description)
  }
}
