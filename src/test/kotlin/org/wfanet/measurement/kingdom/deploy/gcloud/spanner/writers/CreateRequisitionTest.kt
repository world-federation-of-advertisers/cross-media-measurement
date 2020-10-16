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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.Timestamp
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.buildRequisitionDetails

private const val DATA_PROVIDER_ID = 1L
private const val EXTERNAL_DATA_PROVIDER_ID = 2L
private const val CAMPAIGN_ID = 3L
private const val EXTERNAL_CAMPAIGN_ID = 4L

private const val REQUISITION_ID = 5L
private const val EXTERNAL_REQUISITION_ID = 6L
private const val NEW_REQUISITION_ID = 7L
private const val NEW_EXTERNAL_REQUISITION_ID = 8L

private const val ADVERTISER_ID = 9L

private val WINDOW_START_TIME: Instant = Instant.ofEpochSecond(123)
private val WINDOW_END_TIME: Instant = Instant.ofEpochSecond(456)

private val REQUISITION_DETAILS = buildRequisitionDetails(10101L)
private val NEW_REQUISITION_DETAILS = buildRequisitionDetails(20202L)

private const val COMBINED_PUBLIC_KEY_RESOURCE_ID = "combined-public-key-1"
private const val NEW_COMBINED_PUBLIC_KEY_RESOURCE_ID = "combined-public-key-2"

private val REQUISITION: Requisition = Requisition.newBuilder().apply {
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  externalCampaignId = EXTERNAL_CAMPAIGN_ID
  externalRequisitionId = EXTERNAL_REQUISITION_ID
  combinedPublicKeyResourceId = COMBINED_PUBLIC_KEY_RESOURCE_ID
  windowStartTime = WINDOW_START_TIME.toProtoTime()
  windowEndTime = WINDOW_END_TIME.toProtoTime()
  state = RequisitionState.UNFULFILLED
  requisitionDetails = REQUISITION_DETAILS
  requisitionDetailsJson = REQUISITION_DETAILS.toJson()
}.build()

private val INPUT_REQUISITION: Requisition =
  REQUISITION.toBuilder()
    .clearExternalRequisitionId()
    .build()

private val NEW_TIMESTAMP: Timestamp = Timestamp.ofTimeSecondsAndNanos(999, 0)

@RunWith(JUnit4::class)
class CreateRequisitionTest : KingdomDatabaseTestBase() {
  private val idGenerator =
    FixedIdGenerator(InternalId(NEW_REQUISITION_ID), ExternalId(NEW_EXTERNAL_REQUISITION_ID))

  private fun createRequisition(requisition: Requisition): Requisition = runBlocking {
    CreateRequisition(requisition).execute(databaseClient, idGenerator)
  }

  @Before
  fun populateDatabase() = runBlocking {
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID, EXTERNAL_CAMPAIGN_ID, ADVERTISER_ID)
  }

  private suspend fun insertTheRequisition() {
    insertRequisition(
      dataProviderId = DATA_PROVIDER_ID,
      campaignId = CAMPAIGN_ID,
      requisitionId = REQUISITION_ID,
      externalRequisitionId = EXTERNAL_REQUISITION_ID,
      combinedPublicKeyResourceId = COMBINED_PUBLIC_KEY_RESOURCE_ID,
      windowStartTime = WINDOW_START_TIME,
      windowEndTime = WINDOW_END_TIME,
      state = RequisitionState.UNFULFILLED,
      requisitionDetails = REQUISITION_DETAILS
    )
  }

  @Test
  fun `no requisitions exist yet`() {
    val timestampBefore = currentSpannerTimestamp
    val requisition = createRequisition(INPUT_REQUISITION)
    val timestampAfter = currentSpannerTimestamp

    val expectedRequisition =
      REQUISITION.toBuilder()
        .setExternalRequisitionId(NEW_EXTERNAL_REQUISITION_ID)
        .setCreateTime(requisition.createTime)
        .build()

    assertThat(requisition)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedRequisition)

    val createTime = requisition.createTime.toInstant()
    assertThat(createTime).isGreaterThan(timestampBefore)
    assertThat(createTime).isLessThan(timestampAfter)

    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(expectedRequisition)
  }

  @Test
  fun `requisition already exists`() = runBlocking<Unit> {
    insertTheRequisition()
    val requisition = createRequisition(INPUT_REQUISITION)

    assertThat(requisition)
      .comparingExpectedFieldsOnly()
      .isEqualTo(REQUISITION)
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION)
  }

  @Test
  fun `start time used in idempotency`() = runBlocking<Unit> {
    insertTheRequisition()

    val newRequisition = INPUT_REQUISITION.toBuilder().apply {
      externalRequisitionId = NEW_EXTERNAL_REQUISITION_ID
      windowStartTime = NEW_TIMESTAMP.toProto()
    }.build()
    val newRequisitionWithoutId = newRequisition.toBuilder().clearExternalRequisitionId().build()

    val requisition = createRequisition(newRequisitionWithoutId)

    assertThat(requisition)
      .comparingExpectedFieldsOnly()
      .isEqualTo(newRequisition)

    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION, newRequisition)
  }

  @Test
  fun `end time used in idempotency`() = runBlocking<Unit> {
    insertTheRequisition()

    val newRequisition = INPUT_REQUISITION.toBuilder().apply {
      externalRequisitionId = NEW_EXTERNAL_REQUISITION_ID
      windowEndTime = NEW_TIMESTAMP.toProto()
    }.build()
    val newRequisitionWithoutId = newRequisition.toBuilder().clearExternalRequisitionId().build()

    val requisition = createRequisition(newRequisitionWithoutId)

    assertThat(requisition)
      .comparingExpectedFieldsOnly()
      .isEqualTo(newRequisition)

    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION, newRequisition)
  }

  @Test
  fun `details used in idempotency`() = runBlocking<Unit> {
    insertTheRequisition()

    val newRequisition = INPUT_REQUISITION.toBuilder().apply {
      externalRequisitionId = NEW_EXTERNAL_REQUISITION_ID
      requisitionDetails = NEW_REQUISITION_DETAILS
    }.build()
    val newRequisitionWithoutId = newRequisition.toBuilder().clearExternalRequisitionId().build()

    val requisition = createRequisition(newRequisitionWithoutId)

    assertThat(requisition)
      .comparingExpectedFieldsOnly()
      .isEqualTo(newRequisition)

    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION, newRequisition)
  }

  @Test
  fun `combined public key ID used in idempotency`() = runBlocking<Unit> {
    insertTheRequisition()

    val newRequisition = INPUT_REQUISITION.toBuilder().apply {
      externalRequisitionId = NEW_EXTERNAL_REQUISITION_ID
      combinedPublicKeyResourceId = NEW_COMBINED_PUBLIC_KEY_RESOURCE_ID
    }.build()
    val newRequisitionWithoutId = newRequisition.toBuilder().clearExternalRequisitionId().build()

    val requisition = createRequisition(newRequisitionWithoutId)

    assertThat(requisition)
      .comparingExpectedFieldsOnly()
      .isEqualTo(newRequisition)

    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION, newRequisition)
  }
}
