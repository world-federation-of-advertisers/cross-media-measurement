// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.gcloud

import java.time.Clock
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerDataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata

class KingdomDataServicesProviderRule : ProviderRule<DataServices> {
  private val spannerDatabase = SpannerEmulatorDatabaseRule(Schemata.KINGDOM_CHANGELOG_PATH)

  private val dataServices by lazy {
    SpannerDataServices(
      Clock.systemUTC(),
      RandomIdGenerator(Clock.systemUTC()),
      spannerDatabase.databaseClient
    )
  }

  override val value
    get() = dataServices

  override fun apply(base: Statement, description: Description): Statement {
    return spannerDatabase.apply(base, description)
  }
}
