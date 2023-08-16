// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.postgres

import java.time.Clock
import kotlin.random.Random
import org.junit.ClassRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.duchy.deploy.common.postgres.testing.Schemata
import org.wfanet.measurement.duchy.service.internal.testing.ContinuationTokensServiceTest

@RunWith(JUnit4::class)
class PostgresContinuationTokensServiceTest :
  ContinuationTokensServiceTest<PostgresContinuationTokensService>() {

  override fun newService(): PostgresContinuationTokensService {
    val client: PostgresDatabaseClient = databaseProvider.createDatabase()
    val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))
    return PostgresContinuationTokensService(client, idGenerator)
  }

  companion object {
    @get:ClassRule
    @JvmStatic
    val databaseProvider = PostgresDatabaseProviderRule(Schemata.DUCHY_CHANGELOG_PATH)
  }
}
