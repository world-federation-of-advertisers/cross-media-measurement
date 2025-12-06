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

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.type.copy
import com.google.type.interval
import java.security.cert.X509Certificate
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.junit.runners.model.Statement
import org.wfanet.measurement.access.client.v1alpha.TrustedPrincipalAuthInterceptor
import org.wfanet.measurement.access.v1alpha.PoliciesGrpc
import org.wfanet.measurement.access.v1alpha.PolicyKt
import org.wfanet.measurement.access.v1alpha.PrincipalKt
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpc
import org.wfanet.measurement.access.v1alpha.RolesGrpc
import org.wfanet.measurement.access.v1alpha.createPolicyRequest
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.createRoleRequest
import org.wfanet.measurement.access.v1alpha.policy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt as CmmsEventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpc as CmmsEventGroupsGrpc
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata as cmmsEventGroupMetadata
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.TrustedPrincipalCallCredentials
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.keyPair
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.principalKeyPairs
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessKingdom
import org.wfanet.measurement.integration.common.SECRET_FILES_PATH
import org.wfanet.measurement.integration.common.createEntityContent
import org.wfanet.measurement.integration.common.loadTestCertCollection
import org.wfanet.measurement.internal.kingdom.Account as InternalAccount
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices as KingdomInternalServices
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services as ReportingInternalServices
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpc
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest

@RunWith(JUnit4::class)
abstract class InProcessEventGroupsTest(
  private val kingdomInternalServicesRule: ProviderRule<KingdomInternalServices>,
  private val reportingInternalServicesRule: ProviderRule<ReportingInternalServices>,
  private val accessServicesFactory: AccessServicesFactory,
) {
  private val kingdom: InProcessKingdom =
    InProcessKingdom(
      dataServicesProvider = { kingdomInternalServicesRule.value },
      REDIRECT_URI,
      verboseGrpcLogging = false,
    )

  private val reportingRule =
    object : TestRule {
      lateinit var reportingServer: InProcessReportingServer
        private set

      lateinit var measurementConsumerName: String
        private set

      lateinit var dataProvider1Name: String
        private set

      lateinit var dataProvider2Name: String
        private set

      private fun buildReportingServer(): InProcessReportingServer {
        val cmmsAccountsStub = AccountsGrpcKt.AccountsCoroutineStub(kingdom.publicApiChannel)
        val cmmsApiKeysStub = ApiKeysGrpcKt.ApiKeysCoroutineStub(kingdom.publicApiChannel)
        val cmmsMeasurementConsumersStub =
          MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(kingdom.publicApiChannel)
        val resourceSetup =
          ResourceSetup(
            internalAccountsClient = kingdom.internalAccountsClient,
            internalDataProvidersClient = kingdom.internalDataProvidersClient,
            internalCertificatesClient = kingdom.internalCertificatesClient,
            accountsClient = cmmsAccountsStub,
            apiKeysClient = cmmsApiKeysStub,
            measurementConsumersClient = cmmsMeasurementConsumersStub,
            runId = "12345",
            requiredDuchies = emptyList(),
          )
        val internalAccount: InternalAccount = runBlocking {
          resourceSetup.createAccountWithRetries()
        }
        val (measurementConsumer, mcApiKey) =
          runBlocking {
            resourceSetup.createMeasurementConsumer(MC_ENTITY_CONTENT, internalAccount)
          }
        measurementConsumerName = measurementConsumer.name

        val dataProvider1: InternalDataProvider = runBlocking {
          resourceSetup.createInternalDataProvider(EDP1_ENTITY_CONTENT)
        }
        dataProvider1Name =
          DataProviderKey(ExternalId(dataProvider1.externalDataProviderId).apiId.value).toName()
        val dataProvider2: InternalDataProvider = runBlocking {
          resourceSetup.createInternalDataProvider(EDP2_ENTITY_CONTENT)
        }
        dataProvider2Name =
          DataProviderKey(ExternalId(dataProvider2.externalDataProviderId).apiId.value).toName()

        val encryptionKeyPairConfig = encryptionKeyPairConfig {
          principalKeyPairs += principalKeyPairs {
            principal = measurementConsumer.name
            keyPairs += keyPair {
              publicKeyFile = "mc_enc_public.tink"
              privateKeyFile = "mc_enc_private.tink"
            }
          }
        }
        val measurementConsumerConfig = measurementConsumerConfig {
          apiKey = mcApiKey
          signingCertificateName = measurementConsumer.certificate
          signingPrivateKeyPath = MC_SIGNING_PRIVATE_KEY_PATH
        }

        return InProcessReportingServer(
          reportingInternalServicesRule.value,
          accessServicesFactory,
          kingdom.publicApiChannel,
          encryptionKeyPairConfig,
          SECRET_FILES_PATH.toFile(),
          measurementConsumerConfig,
          TRUSTED_CERTIFICATES,
          kingdom.knownEventGroupMetadataTypes,
          TestEvent.getDescriptor(),
          "",
          verboseGrpcLogging = false,
          populationDataProviderName = "",
        )
      }

      override fun apply(base: Statement, description: Description): Statement {
        return object : Statement() {
          override fun evaluate() {
            reportingServer = buildReportingServer()
            reportingServer.apply(base, description).evaluate()
          }
        }
      }
    }

  @get:Rule
  val ruleChain =
    chainRulesSequentially(
      kingdomInternalServicesRule,
      kingdom,
      reportingInternalServicesRule,
      reportingRule,
    )

  private val cmmsEventGroupsStub: CmmsEventGroupsGrpc.EventGroupsBlockingStub by lazy {
    CmmsEventGroupsGrpc.newBlockingStub(kingdom.publicApiChannel)
  }
  private val eventGroupsStub: EventGroupsGrpc.EventGroupsBlockingStub by lazy {
    EventGroupsGrpc.newBlockingStub(reportingRule.reportingServer.publicApiChannel)
  }

  private lateinit var callCredentials: TrustedPrincipalAuthInterceptor.Credentials

  @Before
  fun createAccessPolicy() {
    val accessChannel = reportingRule.reportingServer.accessChannel

    val rolesStub = RolesGrpc.newBlockingStub(accessChannel)
    val eventGroupReaderRole =
      rolesStub.createRole(
        createRoleRequest {
          roleId = "mcEventGroupReader"
          role = role {
            resourceTypes += "halo.wfanet.org/MeasurementConsumer"
            permissions += "permissions/reporting.eventGroups.get"
            permissions += "permissions/reporting.eventGroups.list"
          }
        }
      )

    val principalsStub = PrincipalsGrpc.newBlockingStub(accessChannel)
    val principal =
      principalsStub.createPrincipal(
        createPrincipalRequest {
          principalId = "mc-user"
          this.principal = principal {
            user =
              PrincipalKt.oAuthUser {
                issuer = "example.com"
                subject = "mc-user@example.com"
              }
          }
        }
      )

    val policiesStub = PoliciesGrpc.newBlockingStub(accessChannel)
    policiesStub.createPolicy(
      createPolicyRequest {
        policyId = "test-mc-policy"
        policy = policy {
          protectedResource = reportingRule.measurementConsumerName
          bindings +=
            PolicyKt.binding {
              this.role = eventGroupReaderRole.name
              members += principal.name
            }
        }
      }
    )

    callCredentials = TrustedPrincipalAuthInterceptor.Credentials(principal, setOf("reporting.*"))
  }

  @Test
  fun `listEventGroups returns filtered EventGroups in order`() {
    val now = Instant.now()
    val cmmsEventGroups: List<CmmsEventGroup> = populateTestEventGroups(now)

    val response: ListEventGroupsResponse =
      eventGroupsStub
        .withCallCredentials(callCredentials)
        .listEventGroups(
          listEventGroupsRequest {
            parent = reportingRule.measurementConsumerName
            structuredFilter =
              ListEventGroupsRequestKt.filter {
                cmmsDataProviderIn += reportingRule.dataProvider1Name
                mediaTypesIntersect += MediaType.DISPLAY
                mediaTypesIntersect += MediaType.VIDEO
                metadataSearchQuery = "log"
              }
            orderBy =
              ListEventGroupsRequestKt.orderBy {
                field = ListEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
                descending = true
              }
          }
        )

    assertThat(response.eventGroupsList.map { it.name })
      .containsExactlyElementsIn(
        cmmsEventGroups
          .filter {
            it.name.startsWith(reportingRule.dataProvider1Name) &&
              (it.mediaTypesList.contains(CmmsMediaType.VIDEO) ||
                it.mediaTypesList.contains(CmmsMediaType.DISPLAY)) &&
              (it.eventGroupMetadata.adMetadata.campaignMetadata.campaignName.contains(
                "log",
                ignoreCase = true,
              ) ||
                it.eventGroupMetadata.adMetadata.campaignMetadata.brandName.contains(
                  "log",
                  ignoreCase = true,
                ))
          }
          .sortedByDescending { it.dataAvailabilityInterval.startTime.toInstant() }
          .map {
            val cmmsEventGroupKey = CmmsEventGroupKey.fromName(it.name)!!
            "${reportingRule.measurementConsumerName}/eventGroups/${cmmsEventGroupKey.eventGroupId}"
          }
      )
      .inOrder()
  }

  private fun populateTestEventGroups(now: Instant): List<CmmsEventGroup> {
    val dataProvider1Credentials = TrustedPrincipalCallCredentials(reportingRule.dataProvider1Name)
    val dataProvider2Credentials = TrustedPrincipalCallCredentials(reportingRule.dataProvider2Name)

    val eventGroup1 =
      cmmsEventGroupsStub
        .withCallCredentials(dataProvider1Credentials)
        .createEventGroup(
          createEventGroupRequest {
            parent = reportingRule.dataProvider1Name
            eventGroup = cmmsEventGroup {
              measurementConsumer = reportingRule.measurementConsumerName
              dataAvailabilityInterval = interval {
                startTime = now.minus(90L, ChronoUnit.DAYS).toProtoTime()
                endTime = now.minus(60L, ChronoUnit.DAYS).toProtoTime()
              }
              mediaTypes += CmmsMediaType.VIDEO
              eventGroupMetadata = cmmsEventGroupMetadata {
                adMetadata =
                  CmmsEventGroupMetadataKt.adMetadata {
                    campaignMetadata =
                      CmmsEventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                        brandName = "Log, from Blammo!"
                        campaignName = "Better Than Bad"
                      }
                  }
              }
            }
          }
        )

    val eventGroup2 =
      cmmsEventGroupsStub
        .withCallCredentials(dataProvider2Credentials)
        .createEventGroup(
          createEventGroupRequest {
            parent = reportingRule.dataProvider2Name
            eventGroup =
              eventGroup1.copy {
                dataAvailabilityInterval =
                  dataAvailabilityInterval.copy {
                    startTime = now.minus(91L, ChronoUnit.DAYS).toProtoTime()
                    clearEndTime()
                  }
                mediaTypes.clear()
                mediaTypes += CmmsMediaType.OTHER
              }
          }
        )

    val eventGroup3 =
      cmmsEventGroupsStub
        .withCallCredentials(dataProvider1Credentials)
        .createEventGroup(
          createEventGroupRequest {
            parent = reportingRule.dataProvider1Name
            eventGroup =
              eventGroup1.copy {
                dataAvailabilityInterval =
                  dataAvailabilityInterval.copy {
                    startTime = now.minus(92L, ChronoUnit.DAYS).toProtoTime()
                  }
                mediaTypes.clear()
                mediaTypes += CmmsMediaType.DISPLAY
                eventGroupMetadata =
                  eventGroupMetadata.copy {
                    adMetadata =
                      adMetadata.copy {
                        campaignMetadata = campaignMetadata.copy { campaignName = "It's Good" }
                      }
                  }
              }
          }
        )

    val eventGroup4 =
      cmmsEventGroupsStub
        .withCallCredentials(dataProvider1Credentials)
        .createEventGroup(
          createEventGroupRequest {
            parent = reportingRule.dataProvider1Name
            eventGroup =
              eventGroup1.copy {
                dataAvailabilityInterval =
                  dataAvailabilityInterval.copy {
                    startTime = now.minus(93L, ChronoUnit.DAYS).toProtoTime()
                    clearEndTime()
                  }
                eventGroupMetadata =
                  eventGroupMetadata.copy {
                    adMetadata =
                      adMetadata.copy {
                        campaignMetadata =
                          campaignMetadata.copy {
                            brandName = "Flod"
                            campaignName = "The most perfect cube of fat"
                          }
                      }
                  }
              }
          }
        )

    return listOf(eventGroup1, eventGroup2, eventGroup3, eventGroup4)
  }

  companion object {
    private const val REDIRECT_URI = "https://localhost:2050"
    private const val MC_SIGNING_PRIVATE_KEY_PATH = "mc_cs_private.der"

    private val MC_ENTITY_CONTENT: EntityContent = createEntityContent("mc")
    private val EDP1_ENTITY_CONTENT = createEntityContent("edp1")
    private val EDP2_ENTITY_CONTENT = createEntityContent("edp2")
    private val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      loadTestCertCollection("all_root_certs.pem").associateBy { it.subjectKeyIdentifier!! }

    init {
      DuchyIds.setForTest(emptyList())
    }
  }
}
