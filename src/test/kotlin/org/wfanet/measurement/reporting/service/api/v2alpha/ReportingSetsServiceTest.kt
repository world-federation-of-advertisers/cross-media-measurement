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
package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.stub
import org.mockito.kotlin.whenever
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.client.v1alpha.testing.PrincipalMatcher.Companion.hasPrincipal
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt as InternalReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest as internalCreateReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException
import org.wfanet.measurement.reporting.v2alpha.GetReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsPageTokenKt.previousPageEnd
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.getReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsResponse
import org.wfanet.measurement.reporting.v2alpha.reportingSet

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000
private const val PAGE_SIZE = 2

@RunWith(JUnit4::class)
class ReportingSetsServiceTest {

  private val internalReportingSetsMock: ReportingSetsCoroutineImplBase = mockService {
    onBlocking { createReportingSet(any()) }.thenReturn(INTERNAL_ROOT_COMPOSITE_REPORTING_SET)
    onBlocking { batchGetReportingSets(any()) }
      .thenAnswer {
        val request = it.arguments[0] as BatchGetReportingSetsRequest
        val internalReportingSetsMap =
          mapOf(
            INTERNAL_COMPOSITE_REPORTING_SET.externalReportingSetId to
              INTERNAL_COMPOSITE_REPORTING_SET,
            INTERNAL_COMPOSITE_REPORTING_SET2.externalReportingSetId to
              INTERNAL_COMPOSITE_REPORTING_SET2,
          ) +
            INTERNAL_PRIMITIVE_REPORTING_SETS.associateBy { internalReportingSet ->
              internalReportingSet.externalReportingSetId
            }
        batchGetReportingSetsResponse {
          reportingSets +=
            request.externalReportingSetIdsList.map { externalReportingSetId ->
              internalReportingSetsMap.getValue(externalReportingSetId)
            }
        }
      }
    onBlocking { streamReportingSets(any()) }
      .thenAnswer {
        val request = it.arguments[0] as StreamReportingSetsRequest
        val externalReportingSetIdAfter = request.filter.externalReportingSetIdAfter
        val index =
          INTERNAL_PRIMITIVE_REPORTING_SETS.map { reportingSet ->
              reportingSet.externalReportingSetId
            }
            .indexOf(externalReportingSetIdAfter)
        INTERNAL_PRIMITIVE_REPORTING_SETS.drop(index + 1).asFlow()
      }
  }

  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    onBlocking { checkPermissions(any()) } doReturn CheckPermissionsResponse.getDefaultInstance()
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalReportingSetsMock)
    addService(permissionsServiceMock)
  }

  private lateinit var service: ReportingSetsService

  @Before
  fun initService() {
    service =
      ReportingSetsService(
        ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel)),
      )
  }

  @Test
  fun `createReportingSet returns primitive reporting set`() = runBlocking {
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
        checkPermissionsResponse { permissions += PermissionName.CREATE_PRIMITIVE }
    }
    whenever(internalReportingSetsMock.createReportingSet(any()))
      .thenReturn(INTERNAL_PRIMITIVE_REPORTING_SETS.first())

    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = PRIMITIVE_REPORTING_SETS.first().copy { clearName() }
      reportingSetId = "reporting-set-id"
    }

    val result: ReportingSet =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createReportingSet(request) }

    val expected = PRIMITIVE_REPORTING_SETS.first()

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::createReportingSet,
      )
      .isEqualTo(
        internalCreateReportingSetRequest {
          reportingSet =
            INTERNAL_PRIMITIVE_REPORTING_SETS.first().copy {
              clearExternalReportingSetId()
              weightedSubsetUnions.clear()
            }
          externalReportingSetId = "reporting-set-id"
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createReportingSet returns reporting set when primitiveReportingSetBases are not unique`() {
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
        checkPermissionsResponse { permissions += PermissionName.CREATE_PRIMITIVE }
    }
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
      reportingSetId = "reporting-set-id"
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.createReportingSet(request) }
      }

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::createReportingSet,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateReportingSetRequest {
          reportingSet =
            INTERNAL_ROOT_COMPOSITE_REPORTING_SET.copy { clearExternalReportingSetId() }
          externalReportingSetId = "reporting-set-id"
        }
      )

    assertThat(result).isEqualTo(ROOT_COMPOSITE_REPORTING_SET)
  }

  @Test
  fun `createReportingSet returns reporting set when primitiveReportingSetBases are unique`() =
    runBlocking {
      val internalCompositeReportingSet2 =
        INTERNAL_COMPOSITE_REPORTING_SET2.copy { filter = "GENDER==FEMALE" }

      val internalReportingSetsMap =
        mapOf(
          INTERNAL_COMPOSITE_REPORTING_SET.externalReportingSetId to
            INTERNAL_COMPOSITE_REPORTING_SET,
          internalCompositeReportingSet2.externalReportingSetId to internalCompositeReportingSet2,
        ) +
          INTERNAL_PRIMITIVE_REPORTING_SETS.associateBy { internalReportingSet ->
            internalReportingSet.externalReportingSetId
          }

      val internalRootCompositeReportingSet =
        INTERNAL_ROOT_COMPOSITE_REPORTING_SET.copy {
          clearExternalReportingSetId()
          weightedSubsetUnions.clear()
          weightedSubsetUnions +=
            InternalReportingSetKt.weightedSubsetUnion {
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
                  filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
                }
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
                  filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
                }
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
                  filters += internalCompositeReportingSet2.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
                }
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
                  filters += internalCompositeReportingSet2.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
                }
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[2].externalReportingSetId
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[2].filter
                }
              weight = 1
              binaryRepresentation = (1 shl 5) - 1
            }
          weightedSubsetUnions +=
            InternalReportingSetKt.weightedSubsetUnion {
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
                  filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
                }
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
                  filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
                }
              weight = -1
              binaryRepresentation = (1 shl 3) + (1 shl 4)
            }
        }

      permissionsServiceMock.stub {
        onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
          checkPermissionsResponse { permissions += PermissionName.CREATE_PRIMITIVE }
      }
      whenever(internalReportingSetsMock.batchGetReportingSets(any())).thenAnswer {
        val request = it.arguments[0] as BatchGetReportingSetsRequest
        batchGetReportingSetsResponse {
          reportingSets +=
            request.externalReportingSetIdsList.map { externalReportingSetId ->
              internalReportingSetsMap.getValue(externalReportingSetId)
            }
        }
      }

      val request = createReportingSetRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
        reportingSetId = "reporting-set-id"
      }

      val result: ReportingSet =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createReportingSet(request) }

      verifyProtoArgument(
          internalReportingSetsMock,
          ReportingSetsCoroutineImplBase::createReportingSet,
        )
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          internalCreateReportingSetRequest {
            reportingSet = internalRootCompositeReportingSet
            externalReportingSetId = "reporting-set-id"
          }
        )

      assertThat(result).isEqualTo(ROOT_COMPOSITE_REPORTING_SET)
    }

  @Test
  fun `createReportingSet returns ReportingSet when rhs operand is not set`() {
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
        checkPermissionsResponse { permissions += PermissionName.CREATE_COMPOSITE }
    }
    val reportingSetId = "reporting-set-id"
    val internalReportingSet =
      INTERNAL_ROOT_COMPOSITE_REPORTING_SET.copy {
        externalReportingSetId = reportingSetId
        composite = composite.copy { clearRhs() }

        val weightedSubsetUnions = weightedSubsetUnions
        this.weightedSubsetUnions.clear()
        this.weightedSubsetUnions += weightedSubsetUnions.first()
      }
    wheneverBlocking { internalReportingSetsMock.createReportingSet(any()) } doReturn
      internalReportingSet
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite = composite.copy { expression = expression.copy { clearRhs() } }
        }
      this.reportingSetId = reportingSetId
    }

    val response: ReportingSet = runBlocking {
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createReportingSet(request) }
    }

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::createReportingSet,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateReportingSetRequest {
          reportingSet = internalReportingSet.copy { clearExternalReportingSetId() }
          externalReportingSetId = request.reportingSetId
        }
      )
    assertThat(response)
      .ignoringFields(ReportingSet.NAME_FIELD_NUMBER)
      .isEqualTo(request.reportingSet)
  }

  @Test
  fun `createReportingSet returns ReportingSet when it is a Campaign Group`() = runBlocking {
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
        checkPermissionsResponse { permissions += PermissionName.CREATE_PRIMITIVE }
    }
    val reportingSetResourceId = "reporting-set-id"
    val internalReportingSet =
      INTERNAL_PRIMITIVE_REPORTING_SETS.first().copy {
        externalReportingSetId = reportingSetResourceId
        externalCampaignGroupId = externalReportingSetId
      }
    whenever(internalReportingSetsMock.createReportingSet(any())).thenReturn(internalReportingSet)
    val measurementConsumerKey = MEASUREMENT_CONSUMER_KEYS.first()
    val request = createReportingSetRequest {
      parent = measurementConsumerKey.toName()
      reportingSetId = reportingSetResourceId
      reportingSet =
        PRIMITIVE_REPORTING_SETS.first().copy {
          clearName()
          campaignGroup = ReportingSetKey(measurementConsumerKey, reportingSetResourceId).toName()
        }
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.createReportingSet(request) }
      }

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::createReportingSet,
      )
      .isEqualTo(
        internalCreateReportingSetRequest {
          reportingSet =
            internalReportingSet.copy {
              clearExternalReportingSetId()
              weightedSubsetUnions.clear()
            }
          externalReportingSetId = reportingSetResourceId
        }
      )

    assertThat(result)
      .isEqualTo(
        request.reportingSet.copy {
          name = request.reportingSet.campaignGroup
          campaignGroup = name
        }
      )
  }

  @Test
  fun `createReportingSet throws UNAUTHENTICATED when no principal is found`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
      reportingSetId = "reporting-set-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createReportingSet(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createReportingSet throws PERMISSION_DENIED when MC caller doesn't match`() {
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
        checkPermissionsResponse { permissions += PermissionName.CREATE_COMPOSITE }
    }
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
      reportingSetId = "reporting-set-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "$name-wrong" }, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception)
      .hasMessageThat()
      .ignoringCase()
      .contains(ReportingSetsService.Permission.CREATE_COMPOSITE)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when parent is missing`() {
    val request = createReportingSetRequest {
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
      reportingSetId = "reporting-set-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when resource ID is missing`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when resource ID starts with number`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
      reportingSetId = "1s"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when resource ID is too long`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
      reportingSetId = "s".repeat(100)
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when resource ID contains invalid char`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
      reportingSetId = "contain_invalid_char"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if ReportingSet is not specified`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSetId = "reporting-set-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if ReportingSet value is not specified`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          clearValue()
        }
      reportingSetId = "reporting-set-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if expression in composite is not specified`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite = ReportingSetKt.composite {}
        }
      reportingSetId = "reporting-set-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if operation in expression is not specified`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite = this.composite.copy { expression = this.expression.copy { clearOperation() } }
        }
      reportingSetId = "reporting-set-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if lhs in expression is not specified`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite = this.composite.copy { expression = this.expression.copy { clearLhs() } }
        }
      reportingSetId = "reporting-set-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if name of child reporting set is not valid`() {
    val invalidReportingSetName = "invalid"
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite =
            this.composite.copy {
              expression =
                this.expression.copy {
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = invalidReportingSetName
                    }
                }
            }
        }
      reportingSetId = "reporting-set-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).contains(invalidReportingSetName)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if child ReportingSet has incorrect parent`() {
    val reportingSetNameWithWrongParent =
      ReportingSetKey(
          MEASUREMENT_CONSUMER_KEYS.last().measurementConsumerId,
          ExternalId(400L).apiId.value,
        )
        .toName()
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite =
            this.composite.copy {
              expression =
                this.expression.copy {
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = reportingSetNameWithWrongParent
                    }
                }
            }
        }
      reportingSetId = "reporting-set-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws FAILED_PRECONDITION when child ReportingSet cannot be found`() =
    runBlocking {
      permissionsServiceMock.stub {
        onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
          checkPermissionsResponse { permissions += PermissionName.CREATE_COMPOSITE }
      }
      whenever(internalReportingSetsMock.batchGetReportingSets(any()))
        .thenThrow(StatusRuntimeException(Status.NOT_FOUND))
      val request = createReportingSetRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
        reportingSetId = "reporting-set-id"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createReportingSet(request) }
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `createReportingSet throws FAILED_PRECONDITION when child reporting cannot be found during creation`() =
    runBlocking {
      val cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().toName()
      permissionsServiceMock.stub {
        onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
          checkPermissionsResponse { permissions += PermissionName.CREATE_COMPOSITE }
      }
      whenever(internalReportingSetsMock.createReportingSet(any()))
        .thenThrow(
          ReportingSetNotFoundException(cmmsMeasurementConsumerId, "absent")
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        )

      val request = createReportingSetRequest {
        parent = cmmsMeasurementConsumerId
        reportingSet =
          ROOT_COMPOSITE_REPORTING_SET.copy {
            name = "$cmmsMeasurementConsumerId/reportingSets/absent"
          }
        reportingSetId = "reporting-set-id"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createReportingSet(request) }
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when EventGroups in primitive is empty`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        PRIMITIVE_REPORTING_SETS.first().copy {
          clearName()
          primitive = ReportingSetKt.primitive {}
        }
      reportingSetId = "reporting-set-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when there is any invalid EventGroup`() {
    val invalidEventGroupName = "invalid"
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        PRIMITIVE_REPORTING_SETS.first().copy {
          clearName()
          primitive = ReportingSetKt.primitive { cmmsEventGroups += invalidEventGroupName }
        }
      reportingSetId = "reporting-set-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).contains(invalidEventGroupName)
  }

  @Test
  fun `getReportingSet returns reporting set`() {
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
        checkPermissionsResponse { permissions += PermissionName.GET }
    }
    val request = getReportingSetRequest { name = PRIMITIVE_REPORTING_SETS.first().name }

    val reportingSet =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getReportingSet(request) } }

    assertThat(reportingSet).isEqualTo(PRIMITIVE_REPORTING_SETS.first())
  }

  @Test
  fun `getReportingSet returns ReportingSet with Campaign Group reference`() {
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
        checkPermissionsResponse { permissions += PermissionName.GET }
    }
    val reportingSetKey = ReportingSetKey(MEASUREMENT_CONSUMER_KEYS.first(), "reporting-set-id")
    val reportingSetName = reportingSetKey.toName()
    val internalReportingSet =
      INTERNAL_PRIMITIVE_REPORTING_SETS.first().copy {
        externalReportingSetId = reportingSetKey.reportingSetId
        externalCampaignGroupId = "campaign-group-id"
      }
    wheneverBlocking { internalReportingSetsMock.batchGetReportingSets(any()) }
      .thenReturn(batchGetReportingSetsResponse { reportingSets += internalReportingSet })

    val response: ReportingSet =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.getReportingSet(getReportingSetRequest { name = reportingSetName }) }
      }

    assertThat(response)
      .isEqualTo(
        PRIMITIVE_REPORTING_SETS.first().copy {
          name = reportingSetName
          campaignGroup =
            ReportingSetKey(reportingSetKey.parentKey, internalReportingSet.externalCampaignGroupId)
              .toName()
        }
      )
  }

  @Test
  fun `getReportingSet throws NOT_FOUND when reporting set not found`() = runBlocking {
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
        checkPermissionsResponse { permissions += PermissionName.GET }
    }
    whenever(internalReportingSetsMock.batchGetReportingSets(any()))
      .thenThrow(Status.NOT_FOUND.asRuntimeException())
    val request = getReportingSetRequest { name = PRIMITIVE_REPORTING_SETS.first().name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.getReportingSet(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getReportingSet throws UNAUTHENTICATED when no principal is found`() {
    val request = getReportingSetRequest { name = PRIMITIVE_REPORTING_SETS.first().name }
    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.getReportingSet(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getReportingSet throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    val request = getReportingSetRequest { name = PRIMITIVE_REPORTING_SETS.first().name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "$name-wrong" }, SCOPES) {
          runBlocking { service.getReportingSet(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getReportingSet throws INVALID_ARGUMENT when name is unspecified`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.getReportingSet(GetReportingSetRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listReportingSets returns without a next page token when there is no previous page token`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val request = listReportingSetsRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportingSets(request) }
      }

    val expected = listReportingSetsResponse { reportingSets += PRIMITIVE_REPORTING_SETS }

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::streamReportingSets,
      )
      .isEqualTo(
        streamReportingSetsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          filter =
            StreamReportingSetsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReportingSets returns with a next page token when there is no previous page token`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val request = listReportingSetsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageSize = PAGE_SIZE
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportingSets(request) }
      }

    val expected = listReportingSetsResponse {
      reportingSets += (0 until PAGE_SIZE).map { PRIMITIVE_REPORTING_SETS[it] }
      nextPageToken =
        listReportingSetsPageToken {
            pageSize = PAGE_SIZE
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            lastReportingSet = previousPageEnd {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              externalReportingSetId =
                INTERNAL_PRIMITIVE_REPORTING_SETS[PAGE_SIZE - 1].externalReportingSetId
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::streamReportingSets,
      )
      .isEqualTo(
        streamReportingSetsRequest {
          limit = PAGE_SIZE + 1
          filter =
            StreamReportingSetsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReportingSets returns without a next page token when there is a previous page token`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val request = listReportingSetsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageSize = PAGE_SIZE
      pageToken =
        listReportingSetsPageToken {
            pageSize = PAGE_SIZE
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            lastReportingSet = previousPageEnd {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              externalReportingSetId =
                INTERNAL_PRIMITIVE_REPORTING_SETS.first().externalReportingSetId
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportingSets(request) }
      }

    val expected = listReportingSetsResponse {
      reportingSets += (1 until PAGE_SIZE + 1).map { PRIMITIVE_REPORTING_SETS[it] }
    }

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::streamReportingSets,
      )
      .isEqualTo(
        streamReportingSetsRequest {
          limit = PAGE_SIZE + 1
          filter =
            StreamReportingSetsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              externalReportingSetIdAfter =
                INTERNAL_PRIMITIVE_REPORTING_SETS.first().externalReportingSetId
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReportingSets with page size replaced with a valid value and no previous page token`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val invalidPageSize = MAX_PAGE_SIZE * 2
    val request = listReportingSetsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageSize = invalidPageSize
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportingSets(request) }
      }

    val expected = listReportingSetsResponse { reportingSets += PRIMITIVE_REPORTING_SETS }

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::streamReportingSets,
      )
      .isEqualTo(
        streamReportingSetsRequest {
          limit = MAX_PAGE_SIZE + 1
          filter =
            StreamReportingSetsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReportingSets with invalid page size replaced with the one in previous page token`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val invalidPageSize = MAX_PAGE_SIZE * 2
    val previousPageSize = PAGE_SIZE
    val request = listReportingSetsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageSize = invalidPageSize
      pageToken =
        listReportingSetsPageToken {
            pageSize = previousPageSize
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            lastReportingSet = previousPageEnd {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              externalReportingSetId =
                INTERNAL_PRIMITIVE_REPORTING_SETS.first().externalReportingSetId
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportingSets(request) }
      }

    val expected = listReportingSetsResponse {
      reportingSets += (1 until previousPageSize + 1).map { PRIMITIVE_REPORTING_SETS[it] }
    }

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::streamReportingSets,
      )
      .isEqualTo(
        streamReportingSetsRequest {
          limit = previousPageSize + 1
          filter =
            StreamReportingSetsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              externalReportingSetIdAfter =
                INTERNAL_PRIMITIVE_REPORTING_SETS.first().externalReportingSetId
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReportingSets with page size replacing the one in previous page token`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val newPageSize = PAGE_SIZE
    val previousPageSize = 1
    val request = listReportingSetsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageSize = newPageSize
      pageToken =
        listReportingSetsPageToken {
            pageSize = previousPageSize
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            lastReportingSet = previousPageEnd {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              externalReportingSetId =
                INTERNAL_PRIMITIVE_REPORTING_SETS.first().externalReportingSetId
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listReportingSets(request) }
      }

    val expected = listReportingSetsResponse {
      reportingSets += (1 until newPageSize + 1).map { PRIMITIVE_REPORTING_SETS[it] }
    }

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::streamReportingSets,
      )
      .isEqualTo(
        streamReportingSetsRequest {
          limit = newPageSize + 1
          filter =
            StreamReportingSetsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              externalReportingSetIdAfter =
                INTERNAL_PRIMITIVE_REPORTING_SETS.first().externalReportingSetId
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReportingSets throws UNAUTHENTICATED when no principal is found`() {
    val request = listReportingSetsRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }
    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listReportingSets(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listReportingSets throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    val request = listReportingSetsRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "$name-wrong" }, SCOPES) {
          runBlocking { service.listReportingSets(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listReportingSets throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listReportingSetsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageSize = -1
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listReportingSets(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Page size cannot be less than 0")
  }

  @Test
  fun `listReportingSets throws INVALID_ARGUMENT when parent is unspecified`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listReportingSets(ListReportingSetsRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listReportingSets throws INVALID_ARGUMENT when mc id doesn't match one in page token`() {
    val request = listReportingSetsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageToken =
        listReportingSetsPageToken {
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.last().measurementConsumerId
            lastReportingSet = previousPageEnd {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.last().measurementConsumerId
              externalReportingSetId =
                INTERNAL_PRIMITIVE_REPORTING_SETS.first().externalReportingSetId
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listReportingSets(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  object PermissionName {
    const val GET = "permissions/${ReportingSetsService.Permission.GET}"
    const val LIST = "permissions/${ReportingSetsService.Permission.LIST}"
    const val CREATE_PRIMITIVE = "permissions/${ReportingSetsService.Permission.CREATE_PRIMITIVE}"
    const val CREATE_COMPOSITE = "permissions/${ReportingSetsService.Permission.CREATE_COMPOSITE}"
  }

  companion object {
    private val PRINCIPAL = principal { name = "principals/mc-user" }
    private val ALL_PERMISSIONS =
      setOf(
        ReportingSetsService.Permission.GET,
        ReportingSetsService.Permission.LIST,
        ReportingSetsService.Permission.CREATE_PRIMITIVE,
        ReportingSetsService.Permission.CREATE_COMPOSITE,
      )
    private val SCOPES = ALL_PERMISSIONS

    // Measurement consumers
    private val MEASUREMENT_CONSUMER_KEYS: List<MeasurementConsumerKey> =
      (1L..2L).map { MeasurementConsumerKey(ExternalId(it + 110L).apiId.value) }

    // Data providers
    private val DATA_PROVIDER_KEYS: List<DataProviderKey> =
      (1L..3L).map { DataProviderKey(ExternalId(it + 220L).apiId.value) }

    // Event group IDs and names
    private val CMMS_EVENT_GROUP_KEYS =
      DATA_PROVIDER_KEYS.mapIndexed { index, dataProviderKey ->
        CmmsEventGroupKey(dataProviderKey.dataProviderId, ExternalId(index + 330L).apiId.value)
      }

    // Internal reporting sets
    private val INTERNAL_PRIMITIVE_REPORTING_SETS: List<InternalReportingSet> =
      (0L..2L).map {
        internalReportingSet {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
          externalReportingSetId = (it + 440L).toString()
          filter = "AGE>18"
          displayName = "primitive_reporting_set_display_name$it"
          primitive =
            InternalReportingSetKt.primitive {
              eventGroupKeys += CMMS_EVENT_GROUP_KEYS[it.toInt()].toInternal()
            }
          weightedSubsetUnions +=
            InternalReportingSetKt.weightedSubsetUnion {
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = this@internalReportingSet.externalReportingSetId
                  filters += this@internalReportingSet.filter
                }
              weight = 1
              binaryRepresentation = 1
            }
          details =
            InternalReportingSetKt.details { tags.put("name", "PRIMITIVE_REPORTING_SETS$it") }
        }
      }

    private val INTERNAL_COMPOSITE_REPORTING_SET: InternalReportingSet = internalReportingSet {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
      externalReportingSetId = "450"
      filter = "GENDER==MALE"
      displayName = "composite_reporting_set_display_name"
      composite =
        InternalReportingSetKt.setExpression {
          operation = InternalReportingSet.SetExpression.Operation.UNION
          lhs =
            InternalReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
            }
          rhs =
            InternalReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
            }
        }
      weightedSubsetUnions +=
        InternalReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            InternalReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
              filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
            }
          primitiveReportingSetBases +=
            InternalReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
              filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
            }
          weight = 1
          binaryRepresentation = 3
        }
      details = InternalReportingSetKt.details { tags.put("name", "COMPOSITE_REPORTING_SET") }
    }

    private val INTERNAL_COMPOSITE_REPORTING_SET2: InternalReportingSet =
      INTERNAL_COMPOSITE_REPORTING_SET.copy {
        externalReportingSetId += "2"
        displayName = "composite_reporting_set_display_name2"
        details = InternalReportingSetKt.details { tags.put("name", "COMPOSITE_REPORTING_SET2") }
      }

    private val INTERNAL_ROOT_COMPOSITE_REPORTING_SET: InternalReportingSet = internalReportingSet {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
      externalReportingSetId = "451"
      displayName = "root_composite_reporting_set_display_name"
      composite =
        InternalReportingSetKt.setExpression {
          operation = InternalReportingSet.SetExpression.Operation.DIFFERENCE
          lhs =
            InternalReportingSetKt.SetExpressionKt.operand {
              expression =
                InternalReportingSetKt.setExpression {
                  operation = InternalReportingSet.SetExpression.Operation.UNION
                  lhs =
                    InternalReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId =
                        INTERNAL_PRIMITIVE_REPORTING_SETS[2].externalReportingSetId
                    }
                  rhs =
                    InternalReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId =
                        INTERNAL_COMPOSITE_REPORTING_SET2.externalReportingSetId
                    }
                }
            }
          rhs =
            InternalReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = INTERNAL_COMPOSITE_REPORTING_SET.externalReportingSetId
            }
        }
      weightedSubsetUnions +=
        InternalReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            InternalReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
              filters += INTERNAL_COMPOSITE_REPORTING_SET2.filter
              filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
            }
          primitiveReportingSetBases +=
            InternalReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
              filters += INTERNAL_COMPOSITE_REPORTING_SET2.filter
              filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
            }
          primitiveReportingSetBases +=
            InternalReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[2].externalReportingSetId
              filters += INTERNAL_PRIMITIVE_REPORTING_SETS[2].filter
            }
          weight = 1
          binaryRepresentation = 7
        }
      weightedSubsetUnions +=
        InternalReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            InternalReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
              filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
              filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
            }
          primitiveReportingSetBases +=
            InternalReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
              filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
              filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
            }
          weight = -1
          binaryRepresentation = 6
        }
      details = InternalReportingSetKt.details { tags.put("name", "ROOT_COMPOSITE_REPORTING_SET") }
    }

    // Reporting sets
    private val PRIMITIVE_REPORTING_SETS: List<ReportingSet> =
      INTERNAL_PRIMITIVE_REPORTING_SETS.map { internalReportingSet ->
        reportingSet {
          name = internalReportingSet.resourceName
          filter = internalReportingSet.filter
          displayName = internalReportingSet.displayName
          tags.putAll(internalReportingSet.details.tagsMap)
          primitive =
            ReportingSetKt.primitive {
              cmmsEventGroups +=
                internalReportingSet.primitive.eventGroupKeysList.map {
                  CmmsEventGroupKey(it.cmmsDataProviderId, it.cmmsEventGroupId).toName()
                }
            }
        }
      }

    private val ROOT_COMPOSITE_REPORTING_SET: ReportingSet = reportingSet {
      name = INTERNAL_ROOT_COMPOSITE_REPORTING_SET.resourceName
      filter = INTERNAL_ROOT_COMPOSITE_REPORTING_SET.filter
      displayName = INTERNAL_ROOT_COMPOSITE_REPORTING_SET.displayName
      tags.putAll(INTERNAL_ROOT_COMPOSITE_REPORTING_SET.details.tagsMap)
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.DIFFERENCE
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  expression =
                    ReportingSetKt.setExpression {
                      operation = ReportingSet.SetExpression.Operation.UNION
                      lhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = INTERNAL_PRIMITIVE_REPORTING_SETS[2].resourceName
                        }
                      rhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = INTERNAL_COMPOSITE_REPORTING_SET2.resourceName
                        }
                    }
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = INTERNAL_COMPOSITE_REPORTING_SET.resourceName
                }
            }
        }
    }
  }
}

private fun CmmsEventGroupKey.toInternal(): InternalReportingSet.Primitive.EventGroupKey {
  val source = this
  return InternalReportingSetKt.PrimitiveKt.eventGroupKey {
    cmmsDataProviderId = source.dataProviderId
    cmmsEventGroupId = source.eventGroupId
  }
}

private val InternalReportingSet.resourceKey: ReportingSetKey
  get() = ReportingSetKey(cmmsMeasurementConsumerId, externalReportingSetId)
private val InternalReportingSet.resourceName: String
  get() = resourceKey.toName()
