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

<<<<<<< HEAD
<<<<<<< HEAD
import java.time.Clock
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KINGDOM_SCHEMA
import org.wfanet.measurement.kingdom.service.internal.testing.DataProvidersServiceTest

@RunWith(JUnit4::class)
class SpannerDataProvidersServiceTest : DataProvidersServiceTest<SpannerDataProvidersService>() {

  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(KINGDOM_SCHEMA)
  private val clock = Clock.systemUTC()

  override fun newService(idGenerator: IdGenerator): SpannerDataProvidersService {
    return SpannerDataServices(clock, idGenerator, spannerDatabase.databaseClient)
      .buildDataServices()
      .dataProvidersService as
      SpannerDataProvidersService
  }
=======
=======
import java.time.Clock
>>>>>>> 0e04f4f4 (rebased and synced)
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KINGDOM_SCHEMA
import org.wfanet.measurement.kingdom.service.internal.testing.DataProvidersServiceTest

@RunWith(JUnit4::class)
class SpannerDataProvidersServiceTest :
  DataProvidersServiceTest<SpannerDataProvidersService>() {

  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(KINGDOM_SCHEMA)
  private val clock = Clock.systemUTC()

<<<<<<< HEAD
  override val dataProvidersService: DataProvidersCoroutineImplBase
    get() = spannerDataServicesProviderRule.value.dataProvidersService
>>>>>>> 030d4904 (ready)
=======
  override fun newService(idGenerator: IdGenerator): SpannerDataProvidersService {
    return SpannerDataServices(clock, idGenerator, spannerDatabase.databaseClient)
      .buildDataServices()
      .dataProvidersService as
      SpannerDataProvidersService
  }
>>>>>>> 0e04f4f4 (rebased and synced)
}
