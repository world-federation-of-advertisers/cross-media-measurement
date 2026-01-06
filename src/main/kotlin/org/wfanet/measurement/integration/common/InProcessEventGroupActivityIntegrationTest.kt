/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.date
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub as PublicAccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub as PublicApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupActivityServiceGrpcKt.EventGroupActivityServiceCoroutineStub as PublicEventGroupActivitiesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as PublicEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as PublicMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupActivity
import org.wfanet.measurement.api.v2alpha.updateEventGroupActivityRequest
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup

private const val REDIRECT_URI = "https://localhost:2050"

abstract class InProcessEventGroupActivityIntegrationTest {
  protected abstract val kingdomDataServicesRule: ProviderRule<DataServices>

  private val kingdomDataServices: DataServices
    get() = kingdomDataServicesRule.value

  private val kingdom: InProcessKingdom =
    InProcessKingdom(
      dataServicesProvider = { kingdomDataServices },
      REDIRECT_URI,
      verboseGrpcLogging = false,
    )

  @get:Rule
  val ruleChain: TestRule by lazy { chainRulesSequentially(kingdomDataServicesRule, kingdom) }

  private val publicMeasurementConsumersClient by lazy {
    PublicMeasurementConsumersCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicEventGroupsClient by lazy {
    PublicEventGroupsCoroutineStub(kingdom.publicApiChannel).withPrincipalName(edpResourceName)
  }
  private val publicEventGroupActivitiesClient by lazy {
    PublicEventGroupActivitiesCoroutineStub(kingdom.publicApiChannel)
      .withPrincipalName(edpResourceName)
  }
  private val publicAccountsClient by lazy { PublicAccountsCoroutineStub(kingdom.publicApiChannel) }
  private val publicApiKeysClient by lazy { PublicApiKeysCoroutineStub(kingdom.publicApiChannel) }

  private lateinit var mcResourceName: String
  private lateinit var edpDisplayName: String
  private lateinit var edpResourceName: String
  private lateinit var eventGroupResourceName: String

  @Before
  fun createAllResources() = runBlocking {
    val resourceSetup =
      ResourceSetup(
        internalAccountsClient = kingdom.internalAccountsClient,
        internalDataProvidersClient = kingdom.internalDataProvidersClient,
        internalCertificatesClient = kingdom.internalCertificatesClient,
        accountsClient = publicAccountsClient,
        apiKeysClient = publicApiKeysClient,
        measurementConsumersClient = publicMeasurementConsumersClient,
        runId = "36890",
        requiredDuchies = listOf(),
      )
    // Create the MC.
    val measurementConsumer =
      resourceSetup
        .createMeasurementConsumer(MC_ENTITY_CONTENT, resourceSetup.createAccountWithRetries())
        .measurementConsumer

    mcResourceName = measurementConsumer.name

    // Create EDP Resource
    edpDisplayName = ALL_EDP_DISPLAY_NAMES[0]
    val internalDataProvider =
      resourceSetup.createInternalDataProvider(createEntityContent(edpDisplayName))
    val dataProviderId = externalIdToApiId(internalDataProvider.externalDataProviderId)
    edpResourceName = DataProviderKey(dataProviderId).toName()

    // Create Event Group Resource
    val eventGroup =
      publicEventGroupsClient.createEventGroup(
        createEventGroupRequest {
          parent = edpResourceName
          eventGroup = eventGroup { this.measurementConsumer = mcResourceName }
        }
      )
    eventGroupResourceName = eventGroup.name
  }

  @Test
  fun `batchUpdate returns newly created and updated EventGroupActivity`() = runBlocking {
    val activityDate1 = date {
      year = 2023
      month = 1
      day = 1
    }
    val activityName1 = "$eventGroupResourceName/eventGroupActivities/2023-01-01"

    val activityDate2 = date {
      year = 2025
      month = 1
      day = 1
    }
    val activityName2 = "$eventGroupResourceName/eventGroupActivities/2025-01-01"

    val createRequest = batchUpdateEventGroupActivitiesRequest {
      parent = eventGroupResourceName
      requests += updateEventGroupActivityRequest {
        eventGroupActivity = eventGroupActivity {
          name = activityName1
          date = activityDate1
        }
        allowMissing = true
      }
      requests += updateEventGroupActivityRequest {
        eventGroupActivity = eventGroupActivity {
          name = activityName2
          date = activityDate2
        }
      }
    }

    val createResponse =
      publicEventGroupActivitiesClient.batchUpdateEventGroupActivities(createRequest)

    assertThat(createResponse)
      .isEqualTo(
        batchUpdateEventGroupActivitiesResponse {
          eventGroupActivities += eventGroupActivity {
            name = activityName1
            date = activityDate1
          }
          eventGroupActivities += eventGroupActivity {
            name = activityName2
            date = activityDate2
          }
        }
      )
  }

  companion object {
    private val MC_ENTITY_CONTENT: EntityContent = createEntityContent(MC_DISPLAY_NAME)
  }
}
