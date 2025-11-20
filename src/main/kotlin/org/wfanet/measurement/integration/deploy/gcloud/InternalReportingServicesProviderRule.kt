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

package org.wfanet.measurement.integration.deploy.gcloud

import kotlin.coroutines.EmptyCoroutineContext
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.reporting.deploy.v2.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata.REPORTING_CHANGELOG_PATH
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping

class InternalReportingServicesProviderRule(
  emulatorDatabaseAdmin: SpannerDatabaseAdmin,
  private val postgresDatabaseProvider: PostgresDatabaseProviderRule,
  private val impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
) : ProviderRule<Services> {
  private val spannerDatabase =
    SpannerEmulatorDatabaseRule(emulatorDatabaseAdmin, REPORTING_CHANGELOG_PATH)

  private lateinit var services: Services

  override val value
    get() = services

  override fun apply(base: Statement, description: Description): Statement {
    val statement =
      object : Statement() {
        override fun evaluate() {
          services =
            DataServices.create(
              RandomIdGenerator(),
              postgresDatabaseProvider.createDatabase(),
              spannerDatabase.databaseClient,
              impressionQualificationFilterMapping,
              TestEvent.getDescriptor(),
              false,
              EmptyCoroutineContext,
            )
          base.evaluate()
        }
      }
    return spannerDatabase.apply(statement, description)
  }
}
