// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.kingdom.gcp

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.MetricDefinition
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate

@RunWith(JUnit4::class)
class ReadRequisitionTemplatesQueryTest : KingdomDatabaseTestBase() {
  companion object {
    const val ADVERTISER_ID = 1L
    const val REPORT_CONFIG_ID = 2L
    const val EXTERNAL_ADVERTISER_ID = 3L
    const val EXTERNAL_REPORT_CONFIG_ID = 4L
    const val DATA_PROVIDER_ID = 5L
    const val EXTERNAL_DATA_PROVIDER_ID = 6L
    const val CAMPAIGN_ID1 = 7L
    const val EXTERNAL_CAMPAIGN_ID1 = 8L
    const val CAMPAIGN_ID2 = 9L
    const val EXTERNAL_CAMPAIGN_ID2 = 10L

    private val METRIC_DEFINITIONS: List<MetricDefinition> = listOf(
      MetricDefinition.newBuilder().apply { sketchBuilder.sketchConfigId = 11 }.build(),
      MetricDefinition.newBuilder().apply { sketchBuilder.sketchConfigId = 12 }.build(),
      MetricDefinition.newBuilder().apply { sketchBuilder.sketchConfigId = 13 }.build()
    )

    val REPORT_CONFIG_DETAILS: ReportConfigDetails =
      ReportConfigDetails.newBuilder()
        .addAllMetricDefinitions(METRIC_DEFINITIONS)
        .build()

    val REQUISITION_TEMPLATES: List<RequisitionTemplate> = listOf(
      buildRequisitionTemplate(EXTERNAL_CAMPAIGN_ID1, METRIC_DEFINITIONS[0]),
      buildRequisitionTemplate(EXTERNAL_CAMPAIGN_ID1, METRIC_DEFINITIONS[1]),
      buildRequisitionTemplate(EXTERNAL_CAMPAIGN_ID1, METRIC_DEFINITIONS[2]),
      buildRequisitionTemplate(EXTERNAL_CAMPAIGN_ID2, METRIC_DEFINITIONS[0]),
      buildRequisitionTemplate(EXTERNAL_CAMPAIGN_ID2, METRIC_DEFINITIONS[1]),
      buildRequisitionTemplate(EXTERNAL_CAMPAIGN_ID2, METRIC_DEFINITIONS[2])
    )

    private fun buildRequisitionTemplate(
      externalCampaignId: Long,
      metricDefinition: MetricDefinition
    ): RequisitionTemplate {
      val builder =
        RequisitionTemplate
          .newBuilder()
          .setExternalDataProviderId(EXTERNAL_DATA_PROVIDER_ID)
          .setExternalCampaignId(externalCampaignId)
      builder.requisitionDetailsBuilder.metricDefinition = metricDefinition
      return builder.build()
    }
  }

  @Before
  fun populateDatabase() {
    insertAdvertiser(
      ADVERTISER_ID,
      EXTERNAL_ADVERTISER_ID
    )
    insertReportConfig(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      EXTERNAL_REPORT_CONFIG_ID,
      numRequisitions = 6,
      reportConfigDetails = REPORT_CONFIG_DETAILS
    )
    insertDataProvider(
      DATA_PROVIDER_ID,
      EXTERNAL_DATA_PROVIDER_ID
    )
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID1, EXTERNAL_CAMPAIGN_ID1, ADVERTISER_ID)
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID2, EXTERNAL_CAMPAIGN_ID2, ADVERTISER_ID)
    insertReportConfigCampaign(ADVERTISER_ID, REPORT_CONFIG_ID, DATA_PROVIDER_ID, CAMPAIGN_ID1)
    insertReportConfigCampaign(ADVERTISER_ID, REPORT_CONFIG_ID, DATA_PROVIDER_ID, CAMPAIGN_ID2)
  }

  @Test
  fun success() = runBlocking<Unit> {
    val results =
      ReadRequisitionTemplatesQuery()
        .execute(databaseClient.singleUse(), ExternalId(EXTERNAL_REPORT_CONFIG_ID))

    assertThat(results)
      .containsExactlyElementsIn(REQUISITION_TEMPLATES)
  }
}
