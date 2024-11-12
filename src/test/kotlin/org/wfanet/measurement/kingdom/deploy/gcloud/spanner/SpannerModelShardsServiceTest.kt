/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import java.time.Clock
import org.junit.ClassRule
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.internal.testing.ModelShardsServiceTest

@RunWith(JUnit4::class)
class SpannerModelShardsServiceTest : ModelShardsServiceTest<SpannerModelShardsService>() {

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.KINGDOM_CHANGELOG_PATH)

  override fun newServices(
    clock: Clock,
    idGenerator: IdGenerator,
  ): Services<SpannerModelShardsService> {
    val spannerServices =
      SpannerDataServices(clock, idGenerator, spannerDatabase.databaseClient).buildDataServices()

    return Services(
      spannerServices.modelShardsService as SpannerModelShardsService,
      spannerServices.dataProvidersService,
      spannerServices.modelProvidersService,
      spannerServices.modelSuitesService,
      spannerServices.modelReleasesService,
      spannerServices.populationsService,
    )
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
