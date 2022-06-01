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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import java.time.Clock
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.internal.testing.ApiKeysServiceTest

@RunWith(JUnit4::class)
class SpannerApiKeysServiceTest : ApiKeysServiceTest<SpannerApiKeysService>() {

  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(Schemata.KINGDOM_CHANGELOG_PATH)
  private val clock = Clock.systemUTC()

  override fun newServices(idGenerator: IdGenerator): Services<SpannerApiKeysService> {
    val spannerServices =
      SpannerDataServices(clock, idGenerator, spannerDatabase.databaseClient).buildDataServices()

    return Services(
      spannerServices.apiKeysService as SpannerApiKeysService,
      spannerServices.measurementConsumersService,
      spannerServices.accountsService,
    )
  }
}
