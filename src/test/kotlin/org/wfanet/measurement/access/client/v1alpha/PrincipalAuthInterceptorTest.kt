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
import io.grpc.CallCredentials
import io.grpc.Context
import io.grpc.Metadata
import io.grpc.Metadata.AsciiMarshaller
import io.grpc.ServerInterceptors
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.health.v1.HealthCheckRequest
import io.grpc.health.v1.HealthCheckResponse
import io.grpc.health.v1.HealthGrpcKt
import java.io.StringReader
import java.util.concurrent.Executor
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.mock
import org.mockito.kotlin.stub
import org.wfanet.measurement.access.v1alpha.PrincipalKt.oAuthUser
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.common.grpc.OpenIdConnectAuthentication
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.chainRulesSequentially

@RunWith(JUnit4::class)
class PrincipalAuthInterceptorTest {
  private val principalsServiceMock = mockService<PrincipalsGrpcKt.PrincipalsCoroutineImplBase>()
  private val openIdConnectAuthMock =
    mock<OpenIdConnectAuthentication>().stub {
      onBlocking { verifyAndDecodeBearerToken(any()) } doAnswer
        { invocation ->
          val headers = invocation.getArgument<Metadata>(0)
          val creds =
            FakeTokenCallCredentials.fromHeaders(headers)
              ?: throw Status.UNAUTHENTICATED.withDescription("Token not found").asException()
          creds.token
        }
    }

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
          openIdConnectAuthMock,
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
    val token =
      OpenIdConnectAuthentication.VerifiedToken(
        principal.user.issuer,
        principal.user.subject,
        setOf("foo.bar", "foo.baz"),
      )

    serviceStub
      .withCallCredentials(FakeTokenCallCredentials(token))
      .check(HealthCheckRequest.getDefaultInstance())

    assertThat(PrincipalAuthInterceptor.PRINCIPAL_CONTEXT_KEY.get(capturedContext))
      .isEqualTo(principal)
    assertThat(PrincipalAuthInterceptor.SCOPES_CONTEXT_KEY.get(capturedContext))
      .isEqualTo(token.scopes)
  }

  @Test
  fun `throws UNAUTHENTICATED when principal not found`() = runBlocking {
    principalsServiceMock.stub {
      onBlocking { lookupPrincipal(any()) } doThrow Status.NOT_FOUND.asRuntimeException()
    }
    val token =
      OpenIdConnectAuthentication.VerifiedToken(
        "example.com",
        "user1@example.com",
        setOf("foo.bar", "foo.baz"),
      )

    val exception =
      assertFailsWith<StatusException> {
        serviceStub
          .withCallCredentials(FakeTokenCallCredentials(token))
          .check(HealthCheckRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  private class FakeTokenCallCredentials(val token: OpenIdConnectAuthentication.VerifiedToken) :
    CallCredentials() {
    override fun applyRequestMetadata(
      requestInfo: RequestInfo,
      appExecutor: Executor,
      applier: MetadataApplier,
    ) {
      val headers = Metadata()
      headers.put(TOKEN_METADATA_KEY, token)

      applier.apply(headers)
    }

    companion object {
      private val TOKEN_METADATA_KEY = Metadata.Key.of("x-test-token", TokenMarshaller())

      fun fromHeaders(headers: Metadata): FakeTokenCallCredentials? {
        val token = headers.get(TOKEN_METADATA_KEY) ?: return null
        return FakeTokenCallCredentials(token)
      }
    }

    private class TokenMarshaller : AsciiMarshaller<OpenIdConnectAuthentication.VerifiedToken> {
      override fun toAsciiString(value: OpenIdConnectAuthentication.VerifiedToken): String {
        return buildString {
          appendLine(value.issuer)
          appendLine(value.subject)
          appendLine(value.scopes.joinToString(" "))
        }
      }

      override fun parseAsciiString(serialized: String): OpenIdConnectAuthentication.VerifiedToken {
        val lines = StringReader(serialized).readLines()
        return OpenIdConnectAuthentication.VerifiedToken(
          lines[0],
          lines[1],
          lines[2].split(" ").toSet(),
        )
      }
    }
  }
}
