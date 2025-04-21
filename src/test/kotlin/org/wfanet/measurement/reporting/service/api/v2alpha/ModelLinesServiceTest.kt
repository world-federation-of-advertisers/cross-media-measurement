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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.copy
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.whenever
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.client.v1alpha.testing.PrincipalMatcher.Companion.hasPrincipal
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as KingdomDataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as KingdomDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupKey as KingdomEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase as KingdomEventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as KingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub as KingdomModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase as KingdomModelRolloutsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub as KingdomModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineStub as KingdomModelSuitesCoroutineStub
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.api.v2alpha.testing.withMetadataPrincipalIdentities
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub as InternalKingdomModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase as InternalKingdomModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineStub as InternalKingdomModelReleasesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub as InternalKingdomModelRolloutsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineStub as InternalKingdomModelSuitesCoroutineStub
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerModelLinesService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerModelProvidersService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerModelReleasesService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerModelRolloutsService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerModelSuitesService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerPopulationsService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelLinesService as KingdomModelLinesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelReleasesService as KingdomModelReleasesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelRolloutsService as KingdomModelRolloutsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelSuitesService as KingdomModelSuitesService
import org.wfanet.measurement.reporting.service.api.Errors
import org.wfanet.measurement.reporting.service.api.EventGroupNotFoundException
import org.wfanet.measurement.reporting.v2alpha.listValidModelLinesRequest
import org.wfanet.measurement.reporting.v2alpha.listValidModelLinesResponse

class ModelLinesServiceTest {
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.KINGDOM_CHANGELOG_PATH)

  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    // Grant all permissions for PRINCIPAL.
    onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doAnswer
      { invocation ->
        val request: CheckPermissionsRequest = invocation.getArgument(0)
        checkPermissionsResponse { permissions += request.permissionsList }
      }
  }

  private val eventGroupsServiceMock: KingdomEventGroupsCoroutineImplBase = mockService {
    onBlocking { getEventGroup(any()) }
      .thenReturn(
        eventGroup {
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          }
        }
      )
  }

  private val dataProvidersServiceMock: KingdomDataProvidersCoroutineImplBase = mockService {
    onBlocking { getDataProvider(any()) }
      .thenReturn(
        dataProvider {
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          }
        }
      )
  }

  private val modelRolloutsServiceMock: KingdomModelRolloutsCoroutineImplBase = mockService {
    onBlocking { listModelRollouts(any()) }
      .thenReturn(listModelRolloutsResponse { modelRollouts += modelRollout {} })
  }

  private lateinit var internalModelProvidersService: InternalKingdomModelProvidersCoroutineImplBase

  val internalGrpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))
    val clock = Clock.systemUTC()

    internalModelProvidersService = SpannerModelProvidersService(idGenerator, spannerDatabaseClient)

    addService(internalModelProvidersService)
    addService(SpannerModelSuitesService(idGenerator, spannerDatabaseClient))
    addService(SpannerModelLinesService(clock, idGenerator, spannerDatabaseClient))
    addService(SpannerModelReleasesService(idGenerator, spannerDatabaseClient))
    addService(SpannerModelRolloutsService(clock, idGenerator, spannerDatabaseClient))
    addService(SpannerPopulationsService(idGenerator, spannerDatabaseClient))
  }

  val grpcTestServerRule = GrpcTestServerRule {
    addService(permissionsServiceMock)
    addService(
      KingdomModelSuitesService(
          InternalKingdomModelSuitesCoroutineStub(internalGrpcTestServerRule.channel)
        )
        .withMetadataPrincipalIdentities()
    )
    addService(
      KingdomModelLinesService(
          InternalKingdomModelLinesCoroutineStub(internalGrpcTestServerRule.channel)
        )
        .withMetadataPrincipalIdentities()
    )
    addService(
      KingdomModelRolloutsService(
          InternalKingdomModelRolloutsCoroutineStub(internalGrpcTestServerRule.channel)
        )
        .withMetadataPrincipalIdentities()
    )
    addService(
      KingdomModelReleasesService(
          InternalKingdomModelReleasesCoroutineStub(internalGrpcTestServerRule.channel)
        )
        .withMetadataPrincipalIdentities()
    )
    addService(modelRolloutsServiceMock)
    addService(eventGroupsServiceMock)
    addService(dataProvidersServiceMock)
  }

  @get:Rule
  val serverRuleChain: TestRule =
    chainRulesSequentially(spannerDatabase, internalGrpcTestServerRule, grpcTestServerRule)

  private lateinit var authorization: Authorization

  private lateinit var modelLinesStub: KingdomModelLinesCoroutineStub
  private lateinit var modelRolloutsStub: KingdomModelRolloutsCoroutineStub
  private lateinit var eventGroupsStub: KingdomEventGroupsCoroutineStub
  private lateinit var dataProvidersStub: KingdomDataProvidersCoroutineStub

  private lateinit var modelProviderName: String
  private lateinit var modelSuite: ModelSuite

  private lateinit var service: ModelLinesService

  @Before
  fun initService() {
    runBlocking {
      val modelProvider = internalModelProvidersService.createModelProvider(modelProvider {})
      modelProviderName =
        ModelProviderKey(externalIdToApiId(modelProvider.externalModelProviderId)).toName()
      val modelSuitesStub = KingdomModelSuitesCoroutineStub(grpcTestServerRule.channel)
      modelSuite =
        modelSuitesStub
          .withPrincipalName(modelProviderName)
          .createModelSuite(
            createModelSuiteRequest {
              parent =
                ModelProviderKey(externalIdToApiId(modelProvider.externalModelProviderId)).toName()
              this.modelSuite = modelSuite {
                displayName = "modelSuite"
                description = "test"
              }
            }
          )
    }

    authorization =
      Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel))

    modelLinesStub =
      KingdomModelLinesCoroutineStub(grpcTestServerRule.channel)
        .withPrincipalName(modelProviderName)
    modelRolloutsStub = KingdomModelRolloutsCoroutineStub(grpcTestServerRule.channel)
    eventGroupsStub = KingdomEventGroupsCoroutineStub(grpcTestServerRule.channel)
    dataProvidersStub = KingdomDataProvidersCoroutineStub(grpcTestServerRule.channel)

    service =
      ModelLinesService(
        modelLinesStub.withPrincipalName(modelProviderName),
        modelRolloutsStub.withPrincipalName(modelProviderName),
        eventGroupsStub,
        dataProvidersStub,
        modelSuite.name,
        authorization,
      )
  }

  @Test
  fun `listValidModelLines returns model lines in order`(): Unit = runBlocking {
    val eventGroupName = EventGroupKey("1234", "1234").toName()

    val futureTime = Clock.systemUTC().instant().plusSeconds(100).toProtoTime()

    whenever(eventGroupsServiceMock.getEventGroup(any()))
      .thenReturn(
        eventGroup {
          name = KingdomEventGroupKey("1234", "1234").toName()
          dataAvailabilityInterval = interval {
            startTime = futureTime.copy { seconds += 50 }
            endTime = futureTime.copy { seconds += 100 }
          }
        }
      )

    whenever(dataProvidersServiceMock.getDataProvider(any()))
      .thenReturn(
        dataProvider {
          dataAvailabilityInterval = interval {
            startTime = futureTime.copy { seconds += 50 }
            endTime = futureTime.copy { seconds += 100 }
          }
        }
      )

    var modelLine = modelLine {
      displayName = "modelLine"
      description = "modelLine"
      activeStartTime = futureTime.copy { seconds += 25 }
      type = ModelLine.Type.PROD
    }

    modelLine =
      modelLinesStub.createModelLine(
        createModelLineRequest {
          parent = modelSuite.name
          this.modelLine = modelLine
        }
      )

    var modelLine2 = modelLine {
      displayName = "modelLine"
      description = "modelLine"
      activeStartTime = futureTime.copy { seconds += 40 }
      type = ModelLine.Type.PROD
    }

    modelLine2 =
      modelLinesStub.createModelLine(
        createModelLineRequest {
          parent = modelSuite.name
          this.modelLine = modelLine2
        }
      )

    var modelLine3 = modelLine {
      displayName = "modelLine"
      description = "modelLine"
      activeStartTime = futureTime.copy { seconds += 25 }
      type = ModelLine.Type.DEV
    }

    modelLine3 =
      modelLinesStub.createModelLine(
        createModelLineRequest {
          parent = modelSuite.name
          this.modelLine = modelLine3
        }
      )

    var modelLine4 = modelLine {
      displayName = "modelLine"
      description = "modelLine"
      activeStartTime = futureTime.copy { seconds += 25 }
      type = ModelLine.Type.HOLDBACK
    }

    modelLine4 =
      modelLinesStub.createModelLine(
        createModelLineRequest {
          parent = modelSuite.name
          this.modelLine = modelLine4
        }
      )

    val listValidModelLinesResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.listValidModelLines(
          listValidModelLinesRequest {
            timeInterval = interval {
              startTime = futureTime.copy { seconds += 75 }
              endTime = futureTime.copy { seconds += 80 }
            }
            eventGroups += eventGroupName
            eventGroups += eventGroupName
          }
        )
      }

    assertThat(listValidModelLinesResponse)
      .isEqualTo(
        listValidModelLinesResponse {
          modelLines += modelLine2
          modelLines += modelLine
          modelLines += modelLine4
          modelLines += modelLine3
        }
      )
  }

  @Test
  fun `listValidModelLines does not return model line if 2 rollouts`(): Unit = runBlocking {
    val eventGroupName = EventGroupKey("1234", "1234").toName()

    val futureTime = Clock.systemUTC().instant().plusSeconds(100).toProtoTime()

    whenever(eventGroupsServiceMock.getEventGroup(any()))
      .thenReturn(
        eventGroup {
          name = KingdomEventGroupKey("1234", "1234").toName()
          dataAvailabilityInterval = interval {
            startTime = futureTime.copy { seconds += 50 }
            endTime = futureTime.copy { seconds += 100 }
          }
        }
      )

    whenever(dataProvidersServiceMock.getDataProvider(any()))
      .thenReturn(
        dataProvider {
          dataAvailabilityInterval = interval {
            startTime = futureTime.copy { seconds += 50 }
            endTime = futureTime.copy { seconds += 100 }
          }
        }
      )

    whenever(modelRolloutsServiceMock.listModelRollouts(any()))
      .thenReturn(
        listModelRolloutsResponse {
          modelRollouts += modelRollout {}
          modelRollouts += modelRollout {}
        }
      )

    var modelLine = modelLine {
      displayName = "modelLine"
      description = "modelLine"
      activeStartTime = futureTime.copy { seconds += 25 }
      type = ModelLine.Type.PROD
    }

    modelLine =
      modelLinesStub.createModelLine(
        createModelLineRequest {
          parent = modelSuite.name
          this.modelLine = modelLine
        }
      )

    val listValidModelLinesResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.listValidModelLines(
          listValidModelLinesRequest {
            timeInterval = interval {
              startTime = futureTime.copy { seconds += 75 }
              endTime = futureTime.copy { seconds += 80 }
            }
            eventGroups += eventGroupName
            eventGroups += eventGroupName
          }
        )
      }

    assertThat(listValidModelLinesResponse).isEqualTo(listValidModelLinesResponse {})
  }

  @Test
  fun `listValidModelLines does not return model line if 0 rollouts`(): Unit = runBlocking {
    val eventGroupName = EventGroupKey("1234", "1234").toName()

    val futureTime = Clock.systemUTC().instant().plusSeconds(100).toProtoTime()

    whenever(eventGroupsServiceMock.getEventGroup(any()))
      .thenReturn(
        eventGroup {
          name = KingdomEventGroupKey("1234", "1234").toName()
          dataAvailabilityInterval = interval {
            startTime = futureTime.copy { seconds += 50 }
            endTime = futureTime.copy { seconds += 100 }
          }
        }
      )

    whenever(dataProvidersServiceMock.getDataProvider(any()))
      .thenReturn(
        dataProvider {
          dataAvailabilityInterval = interval {
            startTime = futureTime.copy { seconds += 50 }
            endTime = futureTime.copy { seconds += 100 }
          }
        }
      )

    whenever(modelRolloutsServiceMock.listModelRollouts(any()))
      .thenReturn(listModelRolloutsResponse {})

    var modelLine = modelLine {
      displayName = "modelLine"
      description = "modelLine"
      activeStartTime = futureTime.copy { seconds += 25 }
      type = ModelLine.Type.PROD
    }

    modelLine =
      modelLinesStub.createModelLine(
        createModelLineRequest {
          parent = modelSuite.name
          this.modelLine = modelLine
        }
      )

    val listValidModelLinesResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.listValidModelLines(
          listValidModelLinesRequest {
            timeInterval = interval {
              startTime = futureTime.copy { seconds += 75 }
              endTime = futureTime.copy { seconds += 80 }
            }
            eventGroups += eventGroupName
            eventGroups += eventGroupName
          }
        )
      }

    assertThat(listValidModelLinesResponse).isEqualTo(listValidModelLinesResponse {})
  }

  @Test
  fun `listValidModelLines does not return model line if time_interval not contained`(): Unit =
    runBlocking {
      val eventGroupName = EventGroupKey("1234", "1234").toName()

      val futureTime = Clock.systemUTC().instant().plusSeconds(100).toProtoTime()

      whenever(eventGroupsServiceMock.getEventGroup(any()))
        .thenReturn(
          eventGroup {
            name = KingdomEventGroupKey("1234", "1234").toName()
            dataAvailabilityInterval = interval {
              startTime = futureTime.copy { seconds += 50 }
              endTime = futureTime.copy { seconds += 100 }
            }
          }
        )

      whenever(dataProvidersServiceMock.getDataProvider(any()))
        .thenReturn(
          dataProvider {
            dataAvailabilityInterval = interval {
              startTime = futureTime.copy { seconds += 50 }
              endTime = futureTime.copy { seconds += 100 }
            }
          }
        )

      var modelLine = modelLine {
        displayName = "modelLine"
        description = "modelLine"
        activeStartTime = futureTime.copy { seconds += 25 }
        type = ModelLine.Type.PROD
      }

      modelLine =
        modelLinesStub.createModelLine(
          createModelLineRequest {
            parent = modelSuite.name
            this.modelLine = modelLine
          }
        )

      val listValidModelLinesResponse =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.listValidModelLines(
            listValidModelLinesRequest {
              timeInterval = interval {
                startTime = futureTime
                endTime = futureTime.copy { seconds += 80 }
              }
              eventGroups += eventGroupName
              eventGroups += eventGroupName
            }
          )
        }

      assertThat(listValidModelLinesResponse).isEqualTo(listValidModelLinesResponse {})
    }

  @Test
  fun `listValidModelLines does not return model line if eg data availability not contained`():
    Unit = runBlocking {
    val eventGroupName = EventGroupKey("1234", "1234").toName()

    val futureTime = Clock.systemUTC().instant().plusSeconds(100).toProtoTime()

    whenever(eventGroupsServiceMock.getEventGroup(any()))
      .thenReturn(
        eventGroup {
          name = KingdomEventGroupKey("1234", "1234").toName()
          dataAvailabilityInterval = interval {
            startTime = futureTime
            endTime = futureTime.copy { seconds += 100 }
          }
        }
      )

    whenever(dataProvidersServiceMock.getDataProvider(any()))
      .thenReturn(
        dataProvider {
          dataAvailabilityInterval = interval {
            startTime = futureTime.copy { seconds += 50 }
            endTime = futureTime.copy { seconds += 100 }
          }
        }
      )

    var modelLine = modelLine {
      displayName = "modelLine"
      description = "modelLine"
      activeStartTime = futureTime.copy { seconds += 25 }
      type = ModelLine.Type.PROD
    }

    modelLine =
      modelLinesStub.createModelLine(
        createModelLineRequest {
          parent = modelSuite.name
          this.modelLine = modelLine
        }
      )

    val listValidModelLinesResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.listValidModelLines(
          listValidModelLinesRequest {
            timeInterval = interval {
              startTime = futureTime.copy { seconds += 75 }
              endTime = futureTime.copy { seconds += 80 }
            }
            eventGroups += eventGroupName
            eventGroups += eventGroupName
          }
        )
      }

    assertThat(listValidModelLinesResponse).isEqualTo(listValidModelLinesResponse {})
  }

  @Test
  fun `listValidModelLines does not return model line if edp data availability not contained`():
    Unit = runBlocking {
    val eventGroupName = EventGroupKey("1234", "1234").toName()

    val futureTime = Clock.systemUTC().instant().plusSeconds(100).toProtoTime()

    whenever(eventGroupsServiceMock.getEventGroup(any()))
      .thenReturn(
        eventGroup {
          name = KingdomEventGroupKey("1234", "1234").toName()
          dataAvailabilityInterval = interval {
            startTime = futureTime.copy { seconds += 50 }
            endTime = futureTime.copy { seconds += 100 }
          }
        }
      )

    whenever(dataProvidersServiceMock.getDataProvider(any()))
      .thenReturn(
        dataProvider {
          dataAvailabilityInterval = interval {
            startTime = futureTime
            endTime = futureTime.copy { seconds += 100 }
          }
        }
      )

    var modelLine = modelLine {
      displayName = "modelLine"
      description = "modelLine"
      activeStartTime = futureTime.copy { seconds += 25 }
      type = ModelLine.Type.PROD
    }

    modelLine =
      modelLinesStub.createModelLine(
        createModelLineRequest {
          parent = modelSuite.name
          this.modelLine = modelLine
        }
      )

    val listValidModelLinesResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.listValidModelLines(
          listValidModelLinesRequest {
            timeInterval = interval {
              startTime = futureTime.copy { seconds += 75 }
              endTime = futureTime.copy { seconds += 80 }
            }
            eventGroups += eventGroupName
            eventGroups += eventGroupName
          }
        )
      }

    assertThat(listValidModelLinesResponse).isEqualTo(listValidModelLinesResponse {})
  }

  @Test
  fun `listValidModelLines throws NOT_FOUND when event group not found`(): Unit = runBlocking {
    val eventGroupName = EventGroupKey("1234", "1234").toName()

    whenever(eventGroupsServiceMock.getEventGroup(any()))
      .thenThrow(
        EventGroupNotFoundException(eventGroupName).asStatusRuntimeException(Status.Code.NOT_FOUND)
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.listValidModelLines(
            listValidModelLinesRequest {
              timeInterval = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
              eventGroups += eventGroupName
            }
          )
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.EVENT_GROUP_NOT_FOUND.name
          metadata[Errors.Metadata.EVENT_GROUP.key] = eventGroupName
        }
      )
  }

  @Test
  fun `listValidModelLines throws PERMISSION_DENIED when caller does not have permission`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(
          PRINCIPAL.copy { name = "principals/other-model-suite-user" },
          SCOPES,
        ) {
          runBlocking {
            service.listValidModelLines(
              listValidModelLinesRequest {
                timeInterval = interval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 200 }
                }
                eventGroups += EventGroupKey("1234", "1234").toName()
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception).hasMessageThat().contains(ModelLinesService.Permission.LIST)
  }

  @Test
  fun `listValidModelLines throws INVALID_ARGUMENT when time_interval is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listValidModelLines(
              listValidModelLinesRequest { eventGroups += EventGroupKey("1234", "1234").toName() }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "time_interval"
        }
      )
  }

  @Test
  fun `listValidModelLines throws INVALID_ARGUMENT when time_interval start_time is invalid`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              service.listValidModelLines(
                listValidModelLinesRequest {
                  timeInterval = interval {
                    startTime = timestamp { seconds = Long.MAX_VALUE }
                    endTime = timestamp { seconds = 200 }
                  }
                  eventGroups += EventGroupKey("1234", "1234").toName()
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "time_interval.start_time"
          }
        )
    }

  @Test
  fun `listValidModelLines throws INVALID_ARGUMENT when time_interval end_time is invalid`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              service.listValidModelLines(
                listValidModelLinesRequest {
                  timeInterval = interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = Long.MAX_VALUE }
                  }
                  eventGroups += EventGroupKey("1234", "1234").toName()
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "time_interval.end_time"
          }
        )
    }

  @Test
  fun `listValidModelLines throws INVALID_ARGUMENT when time_interval start equals end`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              service.listValidModelLines(
                listValidModelLinesRequest {
                  timeInterval = interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = 100 }
                  }
                  eventGroups += EventGroupKey("1234", "1234").toName()
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "time_interval"
          }
        )
    }

  @Test
  fun `listValidModelLines throws INVALID_ARGUMENT when time_interval start greater than end`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              service.listValidModelLines(
                listValidModelLinesRequest {
                  timeInterval = interval {
                    startTime = timestamp { seconds = 200 }
                    endTime = timestamp { seconds = 100 }
                  }
                  eventGroups += EventGroupKey("1234", "1234").toName()
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "time_interval"
          }
        )
    }

  @Test
  fun `listValidModelLines throws INVALID_ARGUMENT when event_groups is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listValidModelLines(
              listValidModelLinesRequest {
                timeInterval = interval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 200 }
                }
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "event_groups"
        }
      )
  }

  @Test
  fun `listValidModelLines throws INVALID_ARGUMENT when event_groups has invalid name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              service.listValidModelLines(
                listValidModelLinesRequest {
                  timeInterval = interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = 200 }
                  }
                  eventGroups += "invalid_name"
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "event_groups"
          }
        )
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val ALL_PERMISSIONS = setOf(ModelLinesService.Permission.LIST)
    private val SCOPES = ALL_PERMISSIONS
    private val PRINCIPAL = principal { name = "principals/model-suite-user" }
  }
}
