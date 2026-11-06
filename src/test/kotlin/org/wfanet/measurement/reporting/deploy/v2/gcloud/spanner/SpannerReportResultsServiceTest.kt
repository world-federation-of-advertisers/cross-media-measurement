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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import io.grpc.BindableService
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.testing.v2.ReportResultsServiceTest

class SpannerReportResultsServiceTest : ReportResultsServiceTest() {
  private var duplicateGeneratedIds = false

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.REPORTING_CHANGELOG_PATH)

  override fun createServices(
    idGenerator: IdGenerator,
    impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  ): List<BindableService> {
    val spannerClient = spannerDatabase.databaseClient
    val postgresClient = postgresDatabaseProvider.createDatabase()
    val integerIdGenerator = org.wfanet.measurement.common.identity.RandomIdGenerator()
    val reportResultsIdGenerator =
      if (duplicateGeneratedIds) {
        object : IdGenerator {
          private var repeatedId: Long? = null

          override fun generateId(): Long {
            repeatedId?.let {
              repeatedId = null
              return it
            }
            return idGenerator.generateId().also { repeatedId = it }
          }
        }
      } else {
        idGenerator
      }

    return listOf(
      SpannerReportResultsService(
        spannerClient,
        impressionQualificationFilterMapping,
        idGenerator = reportResultsIdGenerator,
        processedResultBatchSize = 2,
      ),
      SpannerBasicReportsService(
        spannerClient,
        postgresClient,
        impressionQualificationFilterMapping,
        idGenerator = idGenerator,
      ),
      PostgresReportingSetsService(integerIdGenerator, postgresClient),
      PostgresMeasurementConsumersService(integerIdGenerator, postgresClient),
    )
  }

  @Test
  fun `batch creation retries window ID collisions within the same transaction`() {
    duplicateGeneratedIds = true
    super.`batchCreateReportingSetResults creates ReportingSetResults`()
  }

  companion object {
    @get:JvmStatic @get:ClassRule val spannerEmulator = SpannerEmulatorRule()

    @get:JvmStatic
    @get:ClassRule
    val postgresDatabaseProvider =
      PostgresDatabaseProviderRule(PostgresSchemata.REPORTING_CHANGELOG_PATH)
  }
}
