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
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.kingdom.deploy.common.service.KingdomDataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KINGDOM_SCHEMA
import org.wfanet.measurement.kingdom.service.internal.testing.ExchangeStepsServiceTest

@RunWith(JUnit4::class)
class SpannerExchangeStepsServiceTest : ExchangeStepsServiceTest() {

  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(KINGDOM_SCHEMA)
  private val clock = Clock.systemUTC()

  override fun newExchangeStepsService(idGenerator: IdGenerator): ExchangeStepsCoroutineImplBase {
    return makeKingdomDataServices(idGenerator).exchangeStepsService
  }

  override fun newRecurringExchangesService(
    idGenerator: IdGenerator
  ): RecurringExchangesCoroutineImplBase {
    return makeKingdomDataServices(idGenerator).recurringExchangesService
  }

  override fun newDataProvidersService(idGenerator: IdGenerator): DataProvidersCoroutineImplBase {
    return makeKingdomDataServices(idGenerator).dataProvidersService
  }

  override fun newModelProvidersService(idGenerator: IdGenerator): ModelProvidersCoroutineImplBase {
    return makeKingdomDataServices(idGenerator).modelProvidersService
  }

  private fun makeKingdomDataServices(idGenerator: IdGenerator): KingdomDataServices {
    return SpannerDataServices(clock, idGenerator, spannerDatabase.databaseClient)
      .buildDataServices()
  }
}
