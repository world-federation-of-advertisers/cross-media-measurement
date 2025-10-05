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

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub as PublicAccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub as PublicApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as PublicEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as PublicMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.deleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup

private const val REDIRECT_URI = "https://localhost:2050"

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAnEventGroupIntegrationTest {
  abstract val kingdomDataServicesRule: ProviderRule<DataServices>

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
  private val publicAccountsClient by lazy { PublicAccountsCoroutineStub(kingdom.publicApiChannel) }
  private val publicApiKeysClient by lazy { PublicApiKeysCoroutineStub(kingdom.publicApiChannel) }

  private lateinit var mcResourceName: String
  private lateinit var edpDisplayName: String
  private lateinit var edpResourceName: String

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
        runId = "67890",
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
  }

  @Test
  fun `delete transitions EventGroup state to DELETED`(): Unit = runBlocking {
    val createdEventGroup = createEventGroup("1")
    val deletedEventGroup = deleteEventGroup(createdEventGroup.name)
    val readEventGroup = getEventGroup(createdEventGroup.name)

    assertThat(deletedEventGroup.state).isEqualTo(EventGroup.State.DELETED)
    assertThat(deletedEventGroup).isEqualTo(readEventGroup)
  }

  @Test
  fun `list does not return deleted EventGroups`(): Unit = runBlocking {
    val createdEventGroup1 = createEventGroup("1")
    deleteEventGroup(createdEventGroup1.name)
    val createdEventGroup2 = createEventGroup("2")

    val activeEventGroups =
      publicEventGroupsClient
        .listEventGroups(listEventGroupsRequest { parent = edpResourceName })
        .eventGroupsList
        .toList()

    assertThat(activeEventGroups).containsExactly(createdEventGroup2)
  }

  @Test
  fun `list returns deleted EventGroups when show_deleted is true`(): Unit = runBlocking {
    val createdEventGroup1 = createEventGroup("1")
    val deletedEventGroup1 = deleteEventGroup(createdEventGroup1.name)
    val createdEventGroup2 = createEventGroup("2")

    val allEventGroups =
      publicEventGroupsClient
        .listEventGroups(
          listEventGroupsRequest {
            parent = edpResourceName
            showDeleted = true
          }
        )
        .eventGroupsList
        .toList()

    assertThat(allEventGroups).containsExactly(createdEventGroup2, deletedEventGroup1)
  }

  private suspend fun createEventGroup(name: String): EventGroup {
    return publicEventGroupsClient.createEventGroup(
      createEventGroupRequest {
        parent = edpResourceName
        eventGroup = eventGroup {
          this.measurementConsumer = mcResourceName
          this.name = name
        }
      }
    )
  }

  private suspend fun deleteEventGroup(name: String): EventGroup {
    return publicEventGroupsClient.deleteEventGroup(deleteEventGroupRequest { this.name = name })
  }

  private suspend fun getEventGroup(name: String): EventGroup {
    return publicEventGroupsClient.getEventGroup(getEventGroupRequest { this.name = name })
  }

  companion object {
    private val MC_ENTITY_CONTENT: EntityContent = createEntityContent(MC_DISPLAY_NAME)
  }
}
