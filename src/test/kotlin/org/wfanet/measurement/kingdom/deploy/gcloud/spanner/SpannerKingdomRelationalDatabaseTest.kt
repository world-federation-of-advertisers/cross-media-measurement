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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import java.time.Clock
import org.junit.Before
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase
import org.wfanet.measurement.kingdom.db.testing.AbstractKingdomRelationalDatabaseTest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KINGDOM_SCHEMA

@RunWith(JUnit4::class)
class SpannerKingdomRelationalDatabaseTest : AbstractKingdomRelationalDatabaseTest() {
  @get:Rule
  val spannerEmulatorDatabase = SpannerEmulatorDatabaseRule(KINGDOM_SCHEMA)

  private lateinit var spannerKingdomDatabase: SpannerKingdomRelationalDatabase

  override val database: KingdomRelationalDatabase
    get() = spannerKingdomDatabase

  @Before
  fun initKingdomDatabase() {
    spannerKingdomDatabase =
      SpannerKingdomRelationalDatabase(
        Clock.systemUTC(),
        RandomIdGenerator(),
        spannerEmulatorDatabase.databaseClient
      )
  }
}
