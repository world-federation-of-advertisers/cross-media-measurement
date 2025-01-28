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
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.whenever
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.ApiKeyConstants
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.common.testing.HeaderCapturingInterceptor
import org.wfanet.measurement.common.testing.verifyProtoArgument

@RunWith(JUnit4::class)
class DataProvidersServiceTest {
  private val publicKingdomDataProvidersMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
  }
  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    onBlocking { checkPermissions(any()) } doReturn
      checkPermissionsResponse {
        permissions += DataProvidersService.GET_DATA_PROVIDER_PERMISSIONS.map { "permissions/$it" }
      }
  }

  private val headerCapturingInterceptor = HeaderCapturingInterceptor()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(publicKingdomDataProvidersMock.withInterceptor(headerCapturingInterceptor))
    addService(permissionsServiceMock)
  }

  private lateinit var service: DataProvidersService

  @Before
  fun initService() {
    val authorization =
      Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel))
    service =
      DataProvidersService(
        DataProvidersCoroutineStub(grpcTestServerRule.channel),
        authorization,
        API_AUTHENTICATION_KEY,
      )
  }

  @Test
  fun `getDataProvider returns dataProvider`() = runBlocking {
    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }

    assertThat(response).isEqualTo(DATA_PROVIDER)

    assertThat(
        headerCapturingInterceptor
          .captured(DataProvidersGrpcKt.getDataProviderMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(API_AUTHENTICATION_KEY)

    verifyProtoArgument(
        publicKingdomDataProvidersMock,
        DataProvidersCoroutineImplBase::getDataProvider,
      )
      .isEqualTo(getDataProviderRequest { name = DATA_PROVIDER_NAME })
  }

  @Test
  fun `getDataProvider throws UNAUTHENTICATED when principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getDataProvider throws PERMISSION_DENIED when required scopes are missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, emptySet()) {
          runBlocking {
            service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getDataProvider throws NOT_FOUND when kingdom returns not found`() {
    runBlocking {
      whenever(publicKingdomDataProvidersMock.getDataProvider(any()))
        .thenThrow(Status.NOT_FOUND.asRuntimeException())
    }

    val exception =
      assertFailsWith<StatusException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  companion object {
    private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"
    private val PRINCIPAL = principal { name = "principals/mc-user" }
    private val SCOPES = DataProvidersService.GET_DATA_PROVIDER_PERMISSIONS

    private const val DATA_PROVIDER_NAME = "dataProviders/foo"
    private val DATA_PROVIDER = dataProvider { name = DATA_PROVIDER_NAME }
  }
}
