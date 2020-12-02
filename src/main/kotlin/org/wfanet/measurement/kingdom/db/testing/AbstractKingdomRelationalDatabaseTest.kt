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

package org.wfanet.measurement.kingdom.db.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.MetricDefinition
import org.wfanet.measurement.internal.SketchMetricDefinition
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase

const val DUCHY_ID = "duchy-1"
const val COMBINED_PUBLIC_KEY_RESOURCE_ID = "combined-public-key-1"
const val SKETCH_CONFIG_ID = 123L
const val PROVIDED_CAMPAIGN_ID = "Campaign 1"

val METRIC_DEFINITION: MetricDefinition = MetricDefinition.newBuilder().apply {
  sketchBuilder.apply {
    type = SketchMetricDefinition.Type.IMPRESSION_REACH_AND_FREQUENCY
    sketchConfigId = SKETCH_CONFIG_ID
  }
}.build()

/** Abstract base class for [KingdomRelationalDatabase] tests. */
@RunWith(JUnit4::class)
abstract class AbstractKingdomRelationalDatabaseTest {
  /** [KingdomRelationalDatabase] instance. */
  abstract val database: KingdomRelationalDatabase

  protected suspend fun buildRequisitionWithParents(): Requisition {
    val advertiser = database.createAdvertiser()
    val dataProvider = database.createDataProvider()
    val campaign =
      database.createCampaign(
        ExternalId(dataProvider.externalDataProviderId),
        ExternalId(advertiser.externalAdvertiserId),
        PROVIDED_CAMPAIGN_ID
      )

    return Requisition.newBuilder().apply {
      externalDataProviderId = campaign.externalDataProviderId
      externalCampaignId = campaign.externalCampaignId
      combinedPublicKeyResourceId = COMBINED_PUBLIC_KEY_RESOURCE_ID
      state = RequisitionState.UNFULFILLED
      windowStartTimeBuilder.seconds = 100
      windowEndTimeBuilder.seconds = 200

      requisitionDetailsBuilder.apply {
        metricDefinition = METRIC_DEFINITION
      }
    }.build()
  }

  @Test
  fun `createRequisition returns new Requisition`() = runBlocking {
    val inputRequisition = buildRequisitionWithParents()

    val requisition = database.createRequisition(inputRequisition)

    assertThat(requisition).comparingExpectedFieldsOnly().isEqualTo(inputRequisition)
    assertThat(requisition.externalRequisitionId).isNotEqualTo(0L)
    assertThat(requisition.createTime.seconds).isNotEqualTo(0L)
    assertThat(requisition.providedCampaignId).isEqualTo(PROVIDED_CAMPAIGN_ID)
  }

  @Test
  fun `createRequisition returns existing Requisition`() = runBlocking {
    val inputRequisition = buildRequisitionWithParents()
    val insertedRequisition = database.createRequisition(inputRequisition)

    val requisition = database.createRequisition(insertedRequisition)

    assertThat(requisition).isEqualTo(insertedRequisition)
  }

  @Test
  fun `getRequisition returns inserted Requisition`() = runBlocking {
    val insertedRequisition = database.createRequisition(buildRequisitionWithParents())

    val requisition = database.getRequisition(ExternalId(insertedRequisition.externalRequisitionId))

    assertThat(requisition).isEqualTo(insertedRequisition)
  }

  @Test
  fun `fulfillRequisition returns fulfilled Requisition`() = runBlocking {
    val initialRequisition = database.createRequisition(buildRequisitionWithParents())
    val externalRequisitionId = ExternalId(initialRequisition.externalRequisitionId)

    val update = database.fulfillRequisition(externalRequisitionId, DUCHY_ID)

    assertThat(update.original).isEqualTo(initialRequisition)
    assertThat(update.current.state).isEqualTo(RequisitionState.FULFILLED)
    assertThat(update.current).isEqualTo(database.getRequisition(externalRequisitionId))
  }
}
