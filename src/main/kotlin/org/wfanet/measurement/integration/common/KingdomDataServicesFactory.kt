/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

import java.time.Clock
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.kingdom.deploy.common.service.KingdomDataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerDataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata

/**
 * TestRule that creates and starts a Spanner-backed [KingdomDataServices] implementation using a
 * Spanner emulator.
 *
 * @param spannerDatabaseAdmin The [SpannerDatabaseAdmin] to use.
 * @param idGenerator The [IdGenerator] to use.
 */
class KingdomDataServicesFactory(
  private val spannerDatabaseAdmin: SpannerDatabaseAdmin,
  private val idGenerator: IdGenerator,
) : TestRule {
  private val spannerDatabase =
    SpannerEmulatorDatabaseRule(Schemata.KINGDOM_CHANGELOG_PATH, spannerDatabaseAdmin)

  /**
   * Creates a [KingdomDataServices] instance.
   *
   * @param clock The [Clock] to use.
   * @return A [KingdomDataServices] instance.
   */
  fun create(clock: Clock): KingdomDataServices {
    val spannerDataServices =
      SpannerDataServices(clock, idGenerator, spannerDatabase.databaseClient)
    return spannerDataServices.buildDataServices()
  }

  override fun apply(base: Statement, description: Description): Statement {
    return spannerDatabase.apply(base, description)
  }
}
