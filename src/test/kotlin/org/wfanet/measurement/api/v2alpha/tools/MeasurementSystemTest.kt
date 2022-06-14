/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.api.v2alpha.tools

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Metadata
import io.grpc.Server
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.netty.NettyServerBuilder
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.nio.file.Paths
import java.security.Permission
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.AccountConstants
import org.wfanet.measurement.api.v2alpha.Account
import org.wfanet.measurement.api.v2alpha.AccountKt
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.account
import org.wfanet.measurement.api.v2alpha.activateAccountRequest
import org.wfanet.measurement.api.v2alpha.authenticateRequest
import org.wfanet.measurement.api.v2alpha.authenticateResponse
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.openid.createRequestUri
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.verifyProtoArgument

@RunWith(JUnit4::class)
class MeasurementSystemTest {
  private val accountsServiceMock: AccountsCoroutineImplBase = mockService()
  private val headerInterceptor = HeaderCapturingInterceptor()

  private val publicApiServer: Server =
    NettyServerBuilder.forPort(0)
      .sslContext(kingdomSigningCerts.toServerTlsContext())
      .addService(ServerInterceptors.intercept(accountsServiceMock, headerInterceptor))
      .build()

  @Before
  fun startServer() {
    publicApiServer.start()
  }

  @After
  fun shutdownServer() {
    publicApiServer.shutdown()
    publicApiServer.awaitTermination()
  }

  private val commonArgs: List<String>
    get() =
      listOf(
        "--tls-cert-file=$CLIENT_TLS_CERT",
        "--tls-key-file=$CLIENT_TLS_KEY",
        "--cert-collection-file=$CLIENT_TRUSTED_CERTS",
        "--kingdom-public-api-target=localhost:${publicApiServer.port}"
      )

  private fun callCli(args: Array<String>): String {
    return capturingSystemOut { assertExitsWith(0) { MeasurementSystem.main(args) } }
  }

  @Test
  fun `accounts authenticate prints ID token`() {
    val args =
      commonArgs + listOf("accounts", "authenticate", "--self-issued-openid-provider-key=$SIOP_KEY")
    accountsServiceMock.stub {
      onBlocking { authenticate(any()) }
        .thenReturn(
          authenticateResponse {
            authenticationRequestUri = createRequestUri(1234L, 5678L, "https://example.com", true)
          }
        )
    }

    val output: String = callCli(args.toTypedArray())

    verifyProtoArgument(accountsServiceMock, AccountsCoroutineImplBase::authenticate)
      .isEqualTo(authenticateRequest { issuer = MeasurementSystem.SELF_ISSUED_ISSUER })
    assertThat(output).matches("ID Token: [-_a-zA-Z0-9.]+\\s*")
  }

  @Test
  fun `accounts activate prints response`() {
    val accountName = "accounts/KcuXSjfBx9E"
    val idToken = "fake-id-token"
    val activationToken = "vzmtXavLdk4"
    val args =
      commonArgs +
        listOf(
          "accounts",
          "activate",
          accountName,
          "--id-token=$idToken",
          "--activation-token=$activationToken"
        )
    val account = account {
      name = accountName
      activationState = Account.ActivationState.ACTIVATED
      openId =
        AccountKt.openIdConnectIdentity {
          issuer = MeasurementSystem.SELF_ISSUED_ISSUER
          subject = "fake-oid-subject"
        }
    }
    accountsServiceMock.stub { onBlocking { activateAccount(any()) }.thenReturn(account) }

    val output: String = callCli(args.toTypedArray())

    assertThat(
        headerInterceptor.capturedHeaders.single().get(AccountConstants.ID_TOKEN_METADATA_KEY)
      )
      .isEqualTo(idToken)
    verifyProtoArgument(accountsServiceMock, AccountsCoroutineImplBase::activateAccount)
      .isEqualTo(
        activateAccountRequest {
          name = accountName
          this.activationToken = activationToken
        }
      )
    assertThat(parseTextProto(output.reader(), Account.getDefaultInstance())).isEqualTo(account)
  }

  companion object {
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get(
            "wfa_measurement_system",
            "src",
            "main",
            "k8s",
            "testing",
            "secretfiles",
          )
        )!!
        .toFile()

    private val KINGDOM_TLS_CERT: File = SECRETS_DIR.resolve("kingdom_tls.pem")
    private val KINGDOM_TLS_KEY: File = SECRETS_DIR.resolve("kingdom_tls.key")
    private val KINGDOM_TRUSTED_CERTS: File = SECRETS_DIR.resolve("all_root_certs.pem")

    private val CLIENT_TLS_CERT: File = SECRETS_DIR.resolve("mc_tls.pem")
    private val CLIENT_TLS_KEY: File = SECRETS_DIR.resolve("mc_tls.key")
    private val CLIENT_TRUSTED_CERTS: File = SECRETS_DIR.resolve("kingdom_root.pem")

    private val SIOP_KEY: File = SECRETS_DIR.resolve("account1_siop_private.tink")

    private val kingdomSigningCerts =
      SigningCerts.fromPemFiles(KINGDOM_TLS_CERT, KINGDOM_TLS_KEY, KINGDOM_TRUSTED_CERTS)
  }
}

private inline fun capturingSystemOut(block: () -> Unit): String {
  val originalOut = System.out
  val outputStream = ByteArrayOutputStream()

  System.setOut(PrintStream(outputStream, true))
  try {
    block()
  } finally {
    System.setOut(originalOut)
  }

  return outputStream.toString()
}

private inline fun assertExitsWith(status: Int, block: () -> Unit) {
  val exception: ExitException = assertFailsWith {
    val originalSecurityManager: SecurityManager? = System.getSecurityManager()
    System.setSecurityManager(
      object : SecurityManager() {
        override fun checkPermission(perm: Permission?) {
          // Allow everything.
        }

        override fun checkExit(status: Int) {
          super.checkExit(status)
          throw ExitException(status)
        }
      }
    )

    try {
      block()
    } finally {
      System.setSecurityManager(originalSecurityManager)
    }
  }
  assertThat(exception.status).isEqualTo(status)
}

private class ExitException(val status: Int) : RuntimeException()

private class HeaderCapturingInterceptor : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    _capturedHeaders.add(headers)
    return next.startCall(call, headers)
  }

  private val _capturedHeaders = mutableListOf<Metadata>()
  val capturedHeaders: List<Metadata>
    get() = _capturedHeaders
}
