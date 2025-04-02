/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.client.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.util.JsonFormat
import io.grpc.Context
import io.grpc.ServerInterceptors
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.health.v1.HealthCheckRequest
import io.grpc.health.v1.HealthCheckResponse
import io.grpc.health.v1.HealthGrpcKt
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.wfanet.measurement.access.v1alpha.PrincipalKt.oAuthUser
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.OpenIdProvider
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.access.OpenIdProvidersConfig

@RunWith(JUnit4::class)
class PrincipalAuthInterceptorTest {
  private val principalsServiceMock = mockService<PrincipalsGrpcKt.PrincipalsCoroutineImplBase>()

  private lateinit var capturedContext: Context
  private val healthService =
    object : HealthGrpcKt.HealthCoroutineImplBase() {
      override suspend fun check(request: HealthCheckRequest): HealthCheckResponse {
        capturedContext = Context.current()
        return HealthCheckResponse.getDefaultInstance()
      }
    }

  private val accessServer = GrpcTestServerRule { addService(principalsServiceMock) }
  private val server = GrpcTestServerRule {
    val principalsStub = PrincipalsGrpcKt.PrincipalsCoroutineStub(accessServer.channel)
    addService(
      ServerInterceptors.interceptForward(
        healthService,
        PrincipalAuthInterceptor(
          PROVIDERS_CONFIG,
          principalsStub,
          // TODO(@SanjayVas): Figure out how to exercise TLS credentials path.
          false,
        ),
      )
    )
  }
  @get:Rule val ruleChain = chainRulesSequentially(accessServer, server)

  private lateinit var serviceStub: HealthGrpcKt.HealthCoroutineStub

  @Before
  fun initServiceStub() {
    serviceStub = HealthGrpcKt.HealthCoroutineStub(server.channel)
  }

  @Test
  fun `sets verified token and scopes`() = runBlocking {
    val principal = principal {
      name = "principals/user-1"
      user = oAuthUser {
        issuer = "example.com"
        subject = "user1@example.com"
      }
    }
    principalsServiceMock.stub { onBlocking { lookupPrincipal(any()) } doReturn principal }
    val scopes = setOf("foo.bar", "foo.baz")

    serviceStub
      .withCallCredentials(
        openIdProvider.generateCredentials(AUDIENCE, principal.user.subject, scopes)
      )
      .check(HealthCheckRequest.getDefaultInstance())

    assertThat(ContextKeys.PRINCIPAL.get(capturedContext)).isEqualTo(principal)
    assertThat(ContextKeys.SCOPES.get(capturedContext)).isEqualTo(scopes)
  }

  @Test
  fun `throws UNAUTHENTICATED when principal not found`() = runBlocking {
    principalsServiceMock.stub {
      onBlocking { lookupPrincipal(any()) } doThrow Status.NOT_FOUND.asRuntimeException()
    }

    val exception =
      assertFailsWith<StatusException> {
        serviceStub
          .withCallCredentials(
            openIdProvider.generateCredentials(
              AUDIENCE,
              "user1@example.com",
              setOf("foo.bar", "foo.baz"),
            )
          )
          .check(HealthCheckRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception).hasMessageThat().ignoringCase().contains("principal")
  }

  @Test
  fun `throws UNAUTHENTICATED when token is invalid`() = runBlocking {
    val providerWithWrongJwks = OpenIdProvider(ISSUER)
    val exception =
      assertFailsWith<StatusException> {
        serviceStub
          .withCallCredentials(
            providerWithWrongJwks.generateCredentials(
              AUDIENCE,
              "user1@example.com",
              setOf("foo.bar", "foo.baz"),
            )
          )
          .check(HealthCheckRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception).hasMessageThat().ignoringCase().contains("JWT")
  }

  @Test
  fun `throws UNAUTHENTICATED when credentials are missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusException> {
        serviceStub.check(HealthCheckRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  companion object {
    private const val ISSUER = "example.com"
    private const val AUDIENCE = "dns:///api.example.com:8443"
    private val openIdProvider = OpenIdProvider(ISSUER)

    private val PROVIDERS_CONFIG_JSON =
      """
      {
        "audience": "$AUDIENCE",
        "providerConfigByIssuer": {
          "$ISSUER": {
            "jwks": ${openIdProvider.providerConfig.jwks}
          }
        }
      }
      """
        .trimIndent()

    private val PROVIDERS_CONFIG =
      OpenIdProvidersConfig.newBuilder()
        .apply { JsonFormat.parser().merge(PROVIDERS_CONFIG_JSON, this) }
        .build()
  }
}
