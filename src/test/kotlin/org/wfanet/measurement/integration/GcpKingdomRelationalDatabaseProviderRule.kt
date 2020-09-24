// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.integration

import java.time.Clock
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.db.gcp.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.gcp.GcpKingdomRelationalDatabase

private const val SCHEMA_RESOURCE_PATH = "/src/main/db/gcp/kingdom.sdl"

class GcpKingdomRelationalDatabaseProviderRule : ProviderRule<KingdomRelationalDatabase> {
  private val spannerDatabase = SpannerEmulatorDatabaseRule(SCHEMA_RESOURCE_PATH)

  override val value: KingdomRelationalDatabase by lazy {
    GcpKingdomRelationalDatabase(
      Clock.systemUTC(),
      RandomIdGenerator(Clock.systemUTC()),
      spannerDatabase.databaseClient
    )
  }

  override fun apply(base: Statement, description: Description): Statement {
    return spannerDatabase.apply(base, description)
  }
}
