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
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.ApiKeyConstants
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase as KingdomModelLinesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub as KingdomModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.enumerateValidModelLinesRequest
import org.wfanet.measurement.api.v2alpha.enumerateValidModelLinesResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.common.testing.HeaderCapturingInterceptor
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.reporting.service.api.Errors

class ModelLinesServiceTest {
  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    onBlocking { checkPermissions(any()) } doReturn
      checkPermissionsResponse {
        permissions += ModelLinesService.LIST_MODEL_LINES_PERMISSIONS.map { "permissions/$it" }
      }
  }

  private val modelLinesServiceMock: KingdomModelLinesCoroutineImplBase = mockService {
    onBlocking { enumerateValidModelLines(any()) }
      .thenReturn(enumerateValidModelLinesResponse { modelLines += MODEL_LINE })
  }

  private val headerCapturingInterceptor = HeaderCapturingInterceptor()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(permissionsServiceMock)
    addService(modelLinesServiceMock.withInterceptor(headerCapturingInterceptor))
  }

  private lateinit var authorization: Authorization
  private lateinit var service: ModelLinesService

  @Before
  fun initService() {
    authorization =
      Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel))

    service =
      ModelLinesService(
        KingdomModelLinesCoroutineStub(grpcTestServerRule.channel),
        authorization,
        API_AUTHENTICATION_KEY,
      )
  }

  @Test
  fun `enumerateValidModelLines returns modelLines`() = runBlocking {
    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.enumerateValidModelLines(
            enumerateValidModelLinesRequest {
              parent = MODEL_SUITE_NAME
              timeInterval = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
              dataProviders += DATA_PROVIDER_NAME
            }
          )
        }
      }

    assertThat(response).isEqualTo(enumerateValidModelLinesResponse { modelLines += MODEL_LINE })

    assertThat(
        headerCapturingInterceptor
          .captured(ModelLinesGrpcKt.enumerateValidModelLinesMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(API_AUTHENTICATION_KEY)

    verifyProtoArgument(
        modelLinesServiceMock,
        KingdomModelLinesCoroutineImplBase::enumerateValidModelLines,
      )
      .isEqualTo(
        enumerateValidModelLinesRequest {
          parent = MODEL_SUITE_NAME
          timeInterval = interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          }
          dataProviders += DATA_PROVIDER_NAME
        }
      )
  }

  @Test
  fun `enumerateValidModelLines throws UNAUTHENTICATED when principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.enumerateValidModelLines(
            enumerateValidModelLinesRequest {
              parent = MODEL_SUITE_NAME
              timeInterval = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
              dataProviders += DATA_PROVIDER_NAME
            }
          )
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `enumerateValidModelLines throws PERMISSION_DENIED when required scopes are missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, emptySet()) {
          runBlocking {
            service.enumerateValidModelLines(
              enumerateValidModelLinesRequest {
                parent = MODEL_SUITE_NAME
                timeInterval = interval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 200 }
                }
                dataProviders += DATA_PROVIDER_NAME
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when time_interval is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              withPrincipalAndScopes(PRINCIPAL, SCOPES) {
                runBlocking {
                  service.enumerateValidModelLines(
                    enumerateValidModelLinesRequest {
                      parent = MODEL_SUITE_NAME
                      dataProviders += DATA_PROVIDER_NAME
                    }
                  )
                }
              }
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
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when time_interval start_time is invalid`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              withPrincipalAndScopes(PRINCIPAL, SCOPES) {
                runBlocking {
                  service.enumerateValidModelLines(
                    enumerateValidModelLinesRequest {
                      parent = MODEL_SUITE_NAME
                      timeInterval = interval {
                        startTime = timestamp { seconds = Long.MAX_VALUE }
                        endTime = timestamp { seconds = 200 }
                      }
                      dataProviders += DATA_PROVIDER_NAME
                    }
                  )
                }
              }
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
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when time_interval end_time is invalid`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              withPrincipalAndScopes(PRINCIPAL, SCOPES) {
                runBlocking {
                  service.enumerateValidModelLines(
                    enumerateValidModelLinesRequest {
                      parent = MODEL_SUITE_NAME
                      timeInterval = interval {
                        startTime = timestamp { seconds = 100 }
                        endTime = timestamp { seconds = Long.MAX_VALUE }
                      }
                      dataProviders += DATA_PROVIDER_NAME
                    }
                  )
                }
              }
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
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when time_interval start equals end`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              withPrincipalAndScopes(PRINCIPAL, SCOPES) {
                runBlocking {
                  service.enumerateValidModelLines(
                    enumerateValidModelLinesRequest {
                      parent = MODEL_SUITE_NAME
                      timeInterval = interval {
                        startTime = timestamp { seconds = 100 }
                        endTime = timestamp { seconds = 100 }
                      }
                      dataProviders += DATA_PROVIDER_NAME
                    }
                  )
                }
              }
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
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when time_interval start greater than end`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              withPrincipalAndScopes(PRINCIPAL, SCOPES) {
                runBlocking {
                  service.enumerateValidModelLines(
                    enumerateValidModelLinesRequest {
                      parent = MODEL_SUITE_NAME
                      timeInterval = interval {
                        startTime = timestamp { seconds = 200 }
                        endTime = timestamp { seconds = 100 }
                      }
                      dataProviders += DATA_PROVIDER_NAME
                    }
                  )
                }
              }
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
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when data_providers is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              withPrincipalAndScopes(PRINCIPAL, SCOPES) {
                runBlocking {
                  service.enumerateValidModelLines(
                    enumerateValidModelLinesRequest {
                      parent = MODEL_SUITE_NAME
                      timeInterval = interval {
                        startTime = timestamp { seconds = 100 }
                        endTime = timestamp { seconds = 200 }
                      }
                    }
                  )
                }
              }
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "data_providers"
          }
        )
    }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when data_providers has invalid name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking {
              withPrincipalAndScopes(PRINCIPAL, SCOPES) {
                runBlocking {
                  service.enumerateValidModelLines(
                    enumerateValidModelLinesRequest {
                      parent = MODEL_SUITE_NAME
                      timeInterval = interval {
                        startTime = timestamp { seconds = 100 }
                        endTime = timestamp { seconds = 200 }
                      }
                      dataProviders += "invalid_name"
                    }
                  )
                }
              }
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "data_providers"
          }
        )
    }

  companion object {
    private val SCOPES = ModelLinesService.LIST_MODEL_LINES_PERMISSIONS
    private val PRINCIPAL = principal { name = "principals/mc-user" }

    private const val API_AUTHENTICATION_KEY = "key"

    private val MODEL_LINE = modelLine { displayName = "test" }
    private val DATA_PROVIDER_NAME = DataProviderKey("1234").toName()
    private val MODEL_SUITE_NAME = ModelSuiteKey("1234", "1234").toName()
  }
}
