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
import com.google.rpc.errorInfo
import com.google.type.DayOfWeek
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Paths
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.and
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.reset
import org.mockito.kotlin.stub
import org.mockito.kotlin.whenever
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.client.v1alpha.testing.PermissionMatcher.Companion.hasPermissionId
import org.wfanet.measurement.access.client.v1alpha.testing.PrincipalMatcher.Companion.hasPrincipal
import org.wfanet.measurement.access.client.v1alpha.testing.ProtectedResourceMatcher.Companion.hasProtectedResource
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec as InternalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt as InternalMetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricCalculationSpecRequest as internalCreateMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.getMetricCalculationSpecRequest as internalGetMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsRequest as internalListMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsResponse as internalListMetricCalculationSpecsResponse
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec as internalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.reporting.service.api.Errors
import org.wfanet.measurement.reporting.v2alpha.ListMetricCalculationSpecsPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.getMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsPageToken
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsResponse
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec

private const val RANDOM_OUTPUT_INT = 0
private const val RANDOM_OUTPUT_LONG = 0L

@RunWith(JUnit4::class)
class MetricCalculationSpecsServiceTest {
  private val internalMetricCalculationSpecsMock: MetricCalculationSpecsCoroutineImplBase =
    mockService {
      onBlocking { createMetricCalculationSpec(any()) }.thenReturn(INTERNAL_METRIC_CALCULATION_SPEC)
      onBlocking {
          getMetricCalculationSpec(
            eq(
              internalGetMetricCalculationSpecRequest {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
              }
            )
          )
        }
        .thenReturn(INTERNAL_METRIC_CALCULATION_SPEC)

      onBlocking { listMetricCalculationSpecs(any()) }
        .thenReturn(
          internalListMetricCalculationSpecsResponse {
            metricCalculationSpecs += INTERNAL_METRIC_CALCULATION_SPEC
            metricCalculationSpecs += INTERNAL_METRIC_CALCULATION_SPEC_WITH_GREATER_ID
          }
        )
    }

  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    onBlocking { checkPermissions(any()) } doReturn CheckPermissionsResponse.getDefaultInstance()

    // Grant all permissions to PRINCIPAL.
    onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doAnswer
      { invocation ->
        val request: CheckPermissionsRequest = invocation.getArgument(0)
        checkPermissionsResponse { permissions += request.permissionsList }
      }
  }

  private val modelLinesServiceMock: ModelLinesCoroutineImplBase = mockService {
    onBlocking { getModelLine(any()) }.thenReturn(modelLine {})
  }

  private val randomMock: Random = mock()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalMetricCalculationSpecsMock)
    addService(permissionsServiceMock)
    addService(modelLinesServiceMock)
  }

  private lateinit var service: MetricCalculationSpecsService

  @Before
  fun initService() {
    randomMock.stub {
      on { nextInt(any()) } doReturn RANDOM_OUTPUT_INT
      on { nextLong() } doReturn RANDOM_OUTPUT_LONG
    }

    service =
      MetricCalculationSpecsService(
        MetricCalculationSpecsCoroutineStub(grpcTestServerRule.channel),
        ModelLinesCoroutineStub(grpcTestServerRule.channel),
        METRIC_SPEC_CONFIG,
        Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel)),
        randomMock,
        MEASUREMENT_CONSUMER_CONFIGS,
      )
  }

  @Test
  fun `createMetricCalculationSpec returns metric calculation spec`() = runBlocking {
    val reachMetricSpec = metricSpec {
      reach =
        MetricSpecKt.reachParams {
          multipleDataProviderParams =
            MetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon
                  delta =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .start
                  width =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .width
                }
            }
          singleDataProviderParams =
            MetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.epsilon
                  delta =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.delta
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .start
                  width =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .width
                }
            }
        }
    }

    val metricCalculationSpec = metricCalculationSpec {
      name = METRIC_CALCULATION_SPEC_NAME
      displayName = "displayName"
      metricSpecs += reachMetricSpec
      groupings +=
        listOf(
          MetricCalculationSpecKt.grouping {
            predicates += listOf("gender == MALE", "gender == FEMALE")
          },
          MetricCalculationSpecKt.grouping {
            predicates += listOf("age == 18_34", "age == 55_PLUS")
          },
        )
      tags["year"] = "2024"
    }

    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.metricCalculationSpec = metricCalculationSpec
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val createdMetricCalculationSpec =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.createMetricCalculationSpec(request) }
      }

    val expected = METRIC_CALCULATION_SPEC

    assertThat(createdMetricCalculationSpec).isEqualTo(expected)

    val internalReachMetricSpec = internalMetricSpec {
      reach =
        InternalMetricSpecKt.reachParams {
          multipleDataProviderParams =
            InternalMetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                InternalMetricSpecKt.differentialPrivacyParams {
                  epsilon =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon
                  delta =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta
                }
              vidSamplingInterval =
                InternalMetricSpecKt.vidSamplingInterval {
                  start =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .start
                  width =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .width
                }
            }
          singleDataProviderParams =
            InternalMetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                InternalMetricSpecKt.differentialPrivacyParams {
                  epsilon =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.epsilon
                  delta =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.delta
                }
              vidSamplingInterval =
                InternalMetricSpecKt.vidSamplingInterval {
                  start =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .start
                  width =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .width
                }
            }
        }
    }

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::createMetricCalculationSpec,
      )
      .isEqualTo(
        internalCreateMetricCalculationSpecRequest {
          this.metricCalculationSpec = internalMetricCalculationSpec {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            details =
              InternalMetricCalculationSpecKt.details {
                displayName = metricCalculationSpec.displayName
                metricSpecs += internalReachMetricSpec
                groupings +=
                  metricCalculationSpec.groupingsList.map {
                    InternalMetricCalculationSpecKt.grouping { predicates += it.predicatesList }
                  }
                tags.putAll(metricCalculationSpec.tagsMap)
              }
          }
          externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `createMetricCalculationSpec returns metric calculation spec when model_line set`() =
    runBlocking {
      val modelLineName = ModelLineKey("123", "1234", "125").toName()
      val internalMetricCalculationSpec =
        INTERNAL_METRIC_CALCULATION_SPEC.copy { cmmsModelLine = modelLineName }

      whenever(internalMetricCalculationSpecsMock.createMetricCalculationSpec(any()))
        .thenReturn(internalMetricCalculationSpec)

      val metricCalculationSpec = METRIC_CALCULATION_SPEC.copy { modelLine = modelLineName }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val createdMetricCalculationSpec =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }

      assertThat(createdMetricCalculationSpec).isEqualTo(metricCalculationSpec)

      verifyProtoArgument(
          internalMetricCalculationSpecsMock,
          MetricCalculationSpecsCoroutineImplBase::createMetricCalculationSpec,
        )
        .isEqualTo(
          internalCreateMetricCalculationSpecRequest {
            this.metricCalculationSpec =
              internalMetricCalculationSpec.copy { clearExternalMetricCalculationSpecId() }
            externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
          }
        )
    }

  @Test
  fun `createMetricCalculationSpec returns metric calculation spec when both no freq and window`() =
    runBlocking {
      val internalMetricCalculationSpec =
        INTERNAL_METRIC_CALCULATION_SPEC.copy {
          details =
            details.copy {
              clearMetricFrequencySpec()
              clearTrailingWindow()
            }
        }

      whenever(internalMetricCalculationSpecsMock.createMetricCalculationSpec(any()))
        .thenReturn(internalMetricCalculationSpec)

      val metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          clearMetricFrequencySpec()
          clearTrailingWindow()
        }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val createdMetricCalculationSpec =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }

      assertThat(createdMetricCalculationSpec).isEqualTo(metricCalculationSpec)

      verifyProtoArgument(
          internalMetricCalculationSpecsMock,
          MetricCalculationSpecsCoroutineImplBase::createMetricCalculationSpec,
        )
        .isEqualTo(
          internalCreateMetricCalculationSpecRequest {
            this.metricCalculationSpec =
              internalMetricCalculationSpec.copy { clearExternalMetricCalculationSpecId() }
            externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
          }
        )
    }

  @Test
  fun `createMetricCalculationSpec returns metric calculation spec when frequency weekly`() =
    runBlocking {
      val monday = DayOfWeek.MONDAY
      val internalMetricCalculationSpec =
        INTERNAL_METRIC_CALCULATION_SPEC.copy {
          details =
            details.copy {
              metricFrequencySpec =
                InternalMetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    InternalMetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = monday
                    }
                }
            }
        }

      whenever(internalMetricCalculationSpecsMock.createMetricCalculationSpec(any()))
        .thenReturn(internalMetricCalculationSpec)

      val metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          metricFrequencySpec =
            MetricCalculationSpecKt.metricFrequencySpec {
              weekly = MetricCalculationSpecKt.MetricFrequencySpecKt.weekly { dayOfWeek = monday }
            }
        }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val createdMetricCalculationSpec =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }

      assertThat(createdMetricCalculationSpec).isEqualTo(metricCalculationSpec)

      verifyProtoArgument(
          internalMetricCalculationSpecsMock,
          MetricCalculationSpecsCoroutineImplBase::createMetricCalculationSpec,
        )
        .isEqualTo(
          internalCreateMetricCalculationSpecRequest {
            this.metricCalculationSpec =
              internalMetricCalculationSpec.copy { clearExternalMetricCalculationSpecId() }
            externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
          }
        )
    }

  @Test
  fun `createMetricCalculationSpec returns metric calculation spec when frequency monthly`() =
    runBlocking {
      val first = 1
      val internalMetricCalculationSpec =
        INTERNAL_METRIC_CALCULATION_SPEC.copy {
          details =
            details.copy {
              metricFrequencySpec =
                InternalMetricCalculationSpecKt.metricFrequencySpec {
                  monthly =
                    InternalMetricCalculationSpecKt.MetricFrequencySpecKt.monthly {
                      dayOfMonth = first
                    }
                }
            }
        }

      whenever(internalMetricCalculationSpecsMock.createMetricCalculationSpec(any()))
        .thenReturn(internalMetricCalculationSpec)

      val metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          metricFrequencySpec =
            MetricCalculationSpecKt.metricFrequencySpec {
              monthly = MetricCalculationSpecKt.MetricFrequencySpecKt.monthly { dayOfMonth = first }
            }
        }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val createdMetricCalculationSpec =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }

      assertThat(createdMetricCalculationSpec).isEqualTo(metricCalculationSpec)

      verifyProtoArgument(
          internalMetricCalculationSpecsMock,
          MetricCalculationSpecsCoroutineImplBase::createMetricCalculationSpec,
        )
        .isEqualTo(
          internalCreateMetricCalculationSpecRequest {
            this.metricCalculationSpec =
              internalMetricCalculationSpec.copy { clearExternalMetricCalculationSpecId() }
            externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
          }
        )
    }

  @Test
  fun `createMetricCalculationSpec returns metric calculation spec when window trailing`() =
    runBlocking {
      val internalMetricCalculationSpec =
        INTERNAL_METRIC_CALCULATION_SPEC.copy {
          details =
            details.copy {
              trailingWindow =
                InternalMetricCalculationSpecKt.trailingWindow {
                  count = 2
                  increment = InternalMetricCalculationSpec.TrailingWindow.Increment.DAY
                }
            }
        }

      whenever(internalMetricCalculationSpecsMock.createMetricCalculationSpec(any()))
        .thenReturn(internalMetricCalculationSpec)

      val metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          trailingWindow =
            MetricCalculationSpecKt.trailingWindow {
              count = 2
              increment = MetricCalculationSpec.TrailingWindow.Increment.DAY
            }
        }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val createdMetricCalculationSpec =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }

      assertThat(createdMetricCalculationSpec).isEqualTo(metricCalculationSpec)

      verifyProtoArgument(
          internalMetricCalculationSpecsMock,
          MetricCalculationSpecsCoroutineImplBase::createMetricCalculationSpec,
        )
        .isEqualTo(
          internalCreateMetricCalculationSpecRequest {
            this.metricCalculationSpec =
              internalMetricCalculationSpec.copy { clearExternalMetricCalculationSpecId() }
            externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
          }
        )
    }

  @Test
  fun `createMetricCalculationSpec returns metric calculation spec when window report start`() =
    runBlocking {
      val internalMetricCalculationSpec =
        INTERNAL_METRIC_CALCULATION_SPEC.copy { details = details.copy { clearTrailingWindow() } }

      whenever(internalMetricCalculationSpecsMock.createMetricCalculationSpec(any()))
        .thenReturn(internalMetricCalculationSpec)

      val metricCalculationSpec = METRIC_CALCULATION_SPEC.copy { clearTrailingWindow() }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val createdMetricCalculationSpec =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }

      assertThat(createdMetricCalculationSpec).isEqualTo(metricCalculationSpec)

      verifyProtoArgument(
          internalMetricCalculationSpecsMock,
          MetricCalculationSpecsCoroutineImplBase::createMetricCalculationSpec,
        )
        .isEqualTo(
          internalCreateMetricCalculationSpecRequest {
            this.metricCalculationSpec =
              internalMetricCalculationSpec.copy { clearExternalMetricCalculationSpecId() }
            externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
          }
        )
    }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when parent name is invalid`() {
    val request = createMetricCalculationSpecRequest {
      parent = "name"
      metricCalculationSpec = METRIC_CALCULATION_SPEC
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Parent")
  }

  @Test
  fun `createMetricCalculationSpec throws PERMISSION_DENIED when MC's identity does not match`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains(MetricCalculationSpecsService.Permission.CREATE)
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when display name missing`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC.copy { clearDisplayName() }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("display_name")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when model_line invalid`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC.copy { modelLine = "invalid" }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.INVALID_ARGUMENT.code)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "request.metric_calculation_spec.model_line"
        }
      )
  }

  @Test
  fun `createMetricCalculationSpec throws NOT_FOUND when model_line not found`() {
    wheneverBlocking { modelLinesServiceMock.getModelLine(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val modelLineKey = ModelLineKey("123", "124", "125")

    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC.copy { modelLine = modelLineKey.toName() }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.NOT_FOUND.code)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "request.metric_calculation_spec.model_line"
        }
      )
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when no metric specs`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC.copy { metricSpecs.clear() }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("metric_spec")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when no predicate in grouping`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          groupings.clear()
          groupings += MetricCalculationSpec.Grouping.getDefaultInstance()
        }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("predicates in Grouping")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when predicates duplicated`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          groupings.clear()
          groupings += MetricCalculationSpecKt.grouping { predicates += "age = 1" }
          groupings += MetricCalculationSpecKt.grouping { predicates += "age = 1" }
        }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("duplicate predicates")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when metric_calculation_spec_id bad`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC
      metricCalculationSpecId = "!!!!!!!"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("metric_calculation_spec_id")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when metric_spec vid width too big`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          metricSpecs.clear()
          metricSpecs +=
            REACH_METRIC_SPEC.copy {
              reach =
                reach.copy {
                  multipleDataProviderParams =
                    multipleDataProviderParams.copy {
                      vidSamplingInterval = MetricSpecKt.vidSamplingInterval { width = 2.0f }
                    }
                }
            }
        }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("metric_spec")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when no frequency but window set`() =
    runBlocking {
      val metricCalculationSpec = METRIC_CALCULATION_SPEC.copy { clearMetricFrequencySpec() }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.createMetricCalculationSpec(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when weekly frequency missing day`() =
    runBlocking {
      val metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          metricFrequencySpec =
            MetricCalculationSpecKt.metricFrequencySpec {
              weekly = MetricCalculationSpec.MetricFrequencySpec.Weekly.getDefaultInstance()
            }
        }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.createMetricCalculationSpec(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("day_of_week")
    }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when monthly frequency missing day`() =
    runBlocking {
      val metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          metricFrequencySpec =
            MetricCalculationSpecKt.metricFrequencySpec {
              monthly = MetricCalculationSpec.MetricFrequencySpec.Monthly.getDefaultInstance()
            }
        }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.createMetricCalculationSpec(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("day_of_month")
    }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when monthly frequency day 0`() =
    runBlocking {
      val metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          metricFrequencySpec =
            MetricCalculationSpecKt.metricFrequencySpec {
              monthly = MetricCalculationSpecKt.MetricFrequencySpecKt.monthly { dayOfMonth = 0 }
            }
        }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.createMetricCalculationSpec(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("day_of_month")
    }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when trailing window count 0`() =
    runBlocking {
      val metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          trailingWindow =
            MetricCalculationSpecKt.trailingWindow {
              count = 0
              increment = MetricCalculationSpec.TrailingWindow.Increment.DAY
            }
        }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.createMetricCalculationSpec(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("count")
    }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when no trailing window increment`() =
    runBlocking {
      val metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          trailingWindow = MetricCalculationSpecKt.trailingWindow { count = 1 }
        }
      val request = createMetricCalculationSpecRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        this.metricCalculationSpec = metricCalculationSpec
        metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.createMetricCalculationSpec(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("increment")
    }

  @Test
  fun `getMetricCalculationSpec returns MetricCalculationSpec`() = runBlocking {
    val request = getMetricCalculationSpecRequest { name = METRIC_CALCULATION_SPEC_NAME }

    val metricCalculationSpec =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.getMetricCalculationSpec(request) }
      }

    val expected = METRIC_CALCULATION_SPEC

    assertThat(metricCalculationSpec).isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::getMetricCalculationSpec,
      )
      .isEqualTo(
        internalGetMetricCalculationSpecRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `getMetricCalculationSpec returns MetricCalculationSpec when principal has permission on resource`() {
    val request = getMetricCalculationSpecRequest { name = METRIC_CALCULATION_SPEC_NAME }
    reset(permissionsServiceMock)
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(
        and(
          and(hasPrincipal(PRINCIPAL.name), hasProtectedResource(request.name)),
          hasPermissionId(MetricCalculationSpecsService.Permission.GET),
        )
      )
    } doReturn
      checkPermissionsResponse {
        permissions += PermissionKey(MetricCalculationSpecsService.Permission.GET).toName()
      }

    val metricCalculationSpec =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.getMetricCalculationSpec(request) }
      }

    val expected = METRIC_CALCULATION_SPEC

    assertThat(metricCalculationSpec).isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::getMetricCalculationSpec,
      )
      .isEqualTo(
        internalGetMetricCalculationSpecRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `getMetricCalculationSpec throws NOT_FOUND when spec not found`() = runBlocking {
    whenever(
        internalMetricCalculationSpecsMock.getMetricCalculationSpec(
          eq(
            internalGetMetricCalculationSpecRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
            }
          )
        )
      )
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val request = getMetricCalculationSpecRequest { name = METRIC_CALCULATION_SPEC_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.getMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains(request.name)
  }

  @Test
  fun `getMetricCalculationSpec throws INVALID_ARGUMENT when name is invalid`() {
    val invalidMetricCalculationSpecName = "$METRIC_CALCULATION_SPEC_NAME/test/1234"
    val request = getMetricCalculationSpecRequest { name = invalidMetricCalculationSpecName }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.getMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("name")
  }

  @Test
  fun `getMetricCalculationSpec throws PERMISSION_DENIED when MC's identity does not match`() {
    val request = getMetricCalculationSpecRequest { name = METRIC_CALCULATION_SPEC_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
          runBlocking { service.getMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains(MetricCalculationSpecsService.Permission.GET)
  }

  @Test
  fun `listMetricCalculationSpecs returns with next page token when there is another page`() {
    runBlocking {
      whenever(internalMetricCalculationSpecsMock.listMetricCalculationSpecs(any()))
        .thenReturn(
          internalListMetricCalculationSpecsResponse {
            metricCalculationSpecs += INTERNAL_METRIC_CALCULATION_SPEC
            limited = true
          }
        )
    }

    val pageSize = 1
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      nextPageToken =
        listMetricCalculationSpecsPageToken {
            this.pageSize = pageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastMetricCalculationSpec =
              ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs,
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = pageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs returns with no next page token when no other page`() {
    val pageSize = 2
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs,
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = pageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs succeeds when there is a previous page token`() {
    val pageSize = 2
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      pageToken =
        listMetricCalculationSpecsPageToken {
            this.pageSize = pageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastMetricCalculationSpec =
              ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs,
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = pageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricCalculationSpecIdAfter = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs with page size too large replaced by max page size`() {
    val pageSize = MAX_PAGE_SIZE + 1
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs,
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = MAX_PAGE_SIZE
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs with page size too large replaced by one in prev page token`() {
    val pageSize = MAX_PAGE_SIZE + 1
    val oldPageSize = 5
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      pageToken =
        listMetricCalculationSpecsPageToken {
            this.pageSize = oldPageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastMetricCalculationSpec =
              ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs,
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = oldPageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricCalculationSpecIdAfter = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs with no page size replaced by default size`() {
    val request = listMetricCalculationSpecsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs,
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = DEFAULT_PAGE_SIZE
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs with page size replaces size in page token`() {
    val pageSize = 6
    val oldPageSize = 3
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      pageToken =
        listMetricCalculationSpecsPageToken {
            this.pageSize = oldPageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastMetricCalculationSpec =
              ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs,
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = pageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricCalculationSpecIdAfter = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs with a filter returns filtered results`() {
    val pageSize = 2
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      filter = "name != '$METRIC_CALCULATION_SPEC_NAME'"
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs,
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = pageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs throws INTERNAL when backend errors`() = runBlocking {
    whenever(internalMetricCalculationSpecsMock.listMetricCalculationSpecs(any()))
      .thenThrow(StatusRuntimeException(Status.CANCELLED))

    val request = listMetricCalculationSpecsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INTERNAL)
  }

  @Test
  fun `listMetricCalculationSpecs throws UNAUTHENTICATED when no principal is found`() {
    val request = listMetricCalculationSpecsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listMetricCalculationSpecs throws PERMISSION_DENIED when MC caller doesn't match`() {
    val request = listMetricCalculationSpecsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains(MetricCalculationSpecsService.Permission.LIST)
  }

  @Test
  fun `listMetricCalculationSpecs throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = -1
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("less than 0")
  }

  @Test
  fun `listMetricCalculationSpecs throws INVALID_ARGUMENT when parent is unspecified`() {
    val request = listMetricCalculationSpecsRequest { pageSize = 1 }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("unspecified")
  }

  @Test
  fun `listMetricCalculationSpecs throws INVALID_ARGUMENT when mc id doesn't match page token`() {
    val pageToken =
      listMetricCalculationSpecsPageToken {
          pageSize = 2
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          lastMetricCalculationSpec =
            ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
            }
        }
        .toByteString()
        .base64UrlEncode()

    val request = listMetricCalculationSpecsRequest {
      pageSize = 2
      parent = "measurementConsumers/${CMMS_MEASUREMENT_CONSUMER_ID + 1}"
      this.pageToken = pageToken
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("kept the same")
  }

  @Test
  fun `listMetricCalculationSpecs throws INVALID_ARGUMENT when filter is not valid CEL`() {
    val request = listMetricCalculationSpecsRequest {
      pageSize = 2
      parent = MEASUREMENT_CONSUMER_NAME
      filter = "name >>> 5"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a valid CEL")
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000

    private val PRINCIPAL = principal { name = "principals/mc-user" }
    private val SCOPES = setOf("*")

    private const val CMMS_MEASUREMENT_CONSUMER_ID = "A123"
    private const val MEASUREMENT_CONSUMER_NAME =
      "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID"

    private const val METRIC_CALCULATION_SPEC_ID = "b123"
    private const val METRIC_CALCULATION_SPEC_NAME =
      "$MEASUREMENT_CONSUMER_NAME/metricCalculationSpecs/$METRIC_CALCULATION_SPEC_ID"

    private val SECRETS_DIR =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()

    private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"

    private val CONFIG = measurementConsumerConfig {
      apiKey = API_AUTHENTICATION_KEY
      signingCertificateName = "$MEASUREMENT_CONSUMER_NAME/certificates/123"
      signingPrivateKeyPath = "mc_cs_private.der"
    }

    private val MEASUREMENT_CONSUMER_CONFIGS = measurementConsumerConfigs {
      configs[MEASUREMENT_CONSUMER_NAME] = CONFIG
    }

    private val METRIC_SPEC_CONFIG: MetricSpecConfig =
      parseTextProto(
        SECRETS_DIR.resolve("metric_spec_config.textproto"),
        MetricSpecConfig.getDefaultInstance(),
      )

    private val REACH_METRIC_SPEC: MetricSpec = metricSpec {
      reach =
        MetricSpecKt.reachParams {
          multipleDataProviderParams =
            MetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon
                  delta =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .start
                  width =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .width
                }
            }
          singleDataProviderParams =
            MetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.epsilon
                  delta =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.delta
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .start
                  width =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .width
                }
            }
        }
    }

    private val METRIC_CALCULATION_SPEC = metricCalculationSpec {
      name = METRIC_CALCULATION_SPEC_NAME
      displayName = "displayName"
      metricSpecs += REACH_METRIC_SPEC
      groupings +=
        listOf(
          MetricCalculationSpecKt.grouping {
            predicates += listOf("gender == MALE", "gender == FEMALE")
          },
          MetricCalculationSpecKt.grouping {
            predicates += listOf("age == 18_34", "age == 55_PLUS")
          },
        )
      metricFrequencySpec =
        MetricCalculationSpecKt.metricFrequencySpec {
          daily = MetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
        }
      trailingWindow =
        MetricCalculationSpecKt.trailingWindow {
          count = 5
          increment = MetricCalculationSpec.TrailingWindow.Increment.DAY
        }
      tags["year"] = "2024"
    }

    private val METRIC_CALCULATION_SPEC_WITH_GREATER_ID =
      METRIC_CALCULATION_SPEC.copy { name += "2" }

    private val INTERNAL_REACH_METRIC_SPEC: InternalMetricSpec = internalMetricSpec {
      reach =
        InternalMetricSpecKt.reachParams {
          multipleDataProviderParams =
            InternalMetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                InternalMetricSpecKt.differentialPrivacyParams {
                  epsilon =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon
                  delta =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta
                }
              vidSamplingInterval =
                InternalMetricSpecKt.vidSamplingInterval {
                  start =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .start
                  width =
                    METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .width
                }
            }
          singleDataProviderParams =
            InternalMetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                InternalMetricSpecKt.differentialPrivacyParams {
                  epsilon =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.epsilon
                  delta =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.delta
                }
              vidSamplingInterval =
                InternalMetricSpecKt.vidSamplingInterval {
                  start =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .start
                  width =
                    METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                      .fixedStart
                      .width
                }
            }
        }
    }

    private val INTERNAL_METRIC_CALCULATION_SPEC: InternalMetricCalculationSpec =
      internalMetricCalculationSpec {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
        details =
          InternalMetricCalculationSpecKt.details {
            displayName = METRIC_CALCULATION_SPEC.displayName
            metricSpecs += INTERNAL_REACH_METRIC_SPEC
            groupings +=
              METRIC_CALCULATION_SPEC.groupingsList.map {
                InternalMetricCalculationSpecKt.grouping { predicates += it.predicatesList }
              }
            metricFrequencySpec =
              InternalMetricCalculationSpecKt.metricFrequencySpec {
                daily = InternalMetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
              }
            trailingWindow =
              InternalMetricCalculationSpecKt.trailingWindow {
                count = 5
                increment = InternalMetricCalculationSpec.TrailingWindow.Increment.DAY
              }
            tags.putAll(METRIC_CALCULATION_SPEC.tagsMap)
          }
      }

    private val INTERNAL_METRIC_CALCULATION_SPEC_WITH_GREATER_ID =
      INTERNAL_METRIC_CALCULATION_SPEC.copy { externalMetricCalculationSpecId += "2" }
  }
}
