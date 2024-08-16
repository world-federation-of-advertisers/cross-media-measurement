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
import org.junit.Rule
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.internal.testing.ModelRolloutsServiceTest

class SpannerModelRolloutsServiceTest : ModelRolloutsServiceTest<SpannerModelRolloutsService>() {

  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(Schemata.KINGDOM_CHANGELOG_PATH)

  override fun newServices(
    testClock: Clock,
    idGenerator: IdGenerator
  ): Services<SpannerModelRolloutsService> {
    val spannerServices =
      SpannerDataServices(testClock, idGenerator, spannerDatabase.databaseClient)
        .buildDataServices()

    return Services(
      spannerServices.modelRolloutsService as SpannerModelRolloutsService,
      spannerServices.modelProvidersService,
      spannerServices.modelSuitesService,
      spannerServices.modelLinesService,
      spannerServices.modelReleasesService,
      spannerServices.populationsService,
      spannerServices.dataProvidersService
    )
  }
}
