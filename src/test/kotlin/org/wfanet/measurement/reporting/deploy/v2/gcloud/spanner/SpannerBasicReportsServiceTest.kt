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

import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.EventTemplateFieldKt.fieldValue
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.eventFilter
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.eventTemplateField
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilter
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec
import org.wfanet.measurement.config.reporting.impressionQualificationFilterConfig
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.testing.v2.BasicReportsServiceTest

@RunWith(JUnit4::class)
class SpannerBasicReportsServiceTest : BasicReportsServiceTest<SpannerBasicReportsService>() {

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.REPORTING_CHANGELOG_PATH)

  override fun newServices(idGenerator: IdGenerator): Services<SpannerBasicReportsService> {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    val postgresDatabaseClient = postgresDatabaseProvider.createDatabase()
    return Services(
      SpannerBasicReportsService(
        spannerDatabaseClient,
        postgresDatabaseClient,
        IMPRESSION_QUALIFICATION_FILTER_MAPPING,
      ),
      PostgresMeasurementConsumersService(idGenerator, postgresDatabaseClient),
      PostgresReportingSetsService(idGenerator, postgresDatabaseClient),
    )
  }

  companion object {
    @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @JvmStatic
    val postgresDatabaseProvider =
      PostgresDatabaseProviderRule(PostgresSchemata.REPORTING_CHANGELOG_PATH)

    @get:ClassRule
    @JvmStatic
    val ruleChain: TestRule = chainRulesSequentially(spannerEmulator, postgresDatabaseProvider)

    private val AMI_IQF = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.VIDEO }
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.DISPLAY }
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.OTHER }
    }

    private val MRC_IQF = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "mrc"
      impressionQualificationFilterId = 2
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
        filters += eventFilter {
          terms += eventTemplateField {
            path = "banner_ad.viewable_fraction_1_second"
            value = fieldValue { floatValue = 0.5F }
          }
        }
      }
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.VIDEO
        filters += eventFilter {
          terms += eventTemplateField {
            path = "video.viewable_fraction_1_second"
            value = fieldValue { floatValue = 1.0F }
          }
        }
      }
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.OTHER }
    }

    private val IMPRESSION_QUALIFICATION_FILTER_CONFIG = impressionQualificationFilterConfig {
      impressionQualificationFilters += AMI_IQF
      impressionQualificationFilters += MRC_IQF
    }

    private val IMPRESSION_QUALIFICATION_FILTER_MAPPING =
      ImpressionQualificationFilterMapping(IMPRESSION_QUALIFICATION_FILTER_CONFIG)
  }
}
