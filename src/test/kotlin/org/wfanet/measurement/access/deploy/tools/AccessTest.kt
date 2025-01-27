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

package org.wfanet.measurement.access.deploy.tools

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit.SECONDS
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.wfanet.measurement.access.v1alpha.CreatePrincipalRequest
import org.wfanet.measurement.access.v1alpha.DeletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.GetPrincipalRequest
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.PrincipalKt.oAuthUser
import org.wfanet.measurement.access.v1alpha.PrincipalKt.tlsClient
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt.PrincipalsCoroutineImplBase
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.deletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.getPrincipalRequest
import org.wfanet.measurement.access.v1alpha.lookupPrincipalRequest
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.captureFirst

@RunWith(JUnit4::class)
class AccessTest {
  private val principalsServiceMock: PrincipalsCoroutineImplBase = mockService {
    onBlocking { getPrincipal(any()) }.thenReturn(USER_PRINCIPAL)
    onBlocking { createPrincipal(any()) }.thenReturn(USER_PRINCIPAL)
    onBlocking { lookupPrincipal(LOOKUP_USER_PRINCIPAL_REQUEST) }.thenReturn(USER_PRINCIPAL)
    onBlocking { lookupPrincipal(LOOKUP_TLS_PRINCIPAL_REQUEST) }.thenReturn(TLS_PRINCIPAL)
  }

  private val serverCerts =
    SigningCerts.fromPemFiles(
      certificateFile = SECRETS_DIR.resolve("reporting_tls.pem").toFile(),
      privateKeyFile = SECRETS_DIR.resolve("reporting_tls.key").toFile(),
      trustedCertCollectionFile = SECRETS_DIR.resolve("reporting_root.pem").toFile(),
    )

  private val services: List<ServerServiceDefinition> = listOf(principalsServiceMock.bindService())

  private val server: Server =
    NettyServerBuilder.forPort(0)
      .sslContext(serverCerts.toServerTlsContext())
      .addServices(services)
      .build()

  @Before
  fun initServer() {
    server.start()
  }

  @After
  fun shutdownServer() {
    server.shutdown()
    server.awaitTermination(1, SECONDS)
  }

  @Test
  fun `principals get calls GetPrincipal with valid request `() {
    val args = commonArgs + arrayOf("principals", "get", "principals/user-1")

    val output = callCli(args)

    val request: GetPrincipalRequest = captureFirst {
      runBlocking { verify(principalsServiceMock).getPrincipal(capture()) }
    }

    assertThat(request).isEqualTo(getPrincipalRequest { name = "principals/user-1" })
    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(USER_PRINCIPAL)
  }

  @Test
  fun `principals create calls CreatePrincipal with valid request with oauth user`() {
    val args =
      commonArgs +
        arrayOf(
          "principals",
          "create",
          "--name=principals/user-1",
          "--issuer=example.com",
          "--subject=user1@example.com",
          "--principal-id=user-1",
        )
    val output = callCli(args)

    val request: CreatePrincipalRequest = captureFirst {
      runBlocking { verify(principalsServiceMock).createPrincipal(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        createPrincipalRequest {
          principal = principal {
            name = "principals/user-1"
            user = oAuthUser {
              issuer = "example.com"
              subject = "user1@example.com"
            }
          }
          principalId = "user-1"
        }
      )
    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(USER_PRINCIPAL)
  }

  @Test
  fun `principals delete calls DeletePrincipal with valid request`() {
    val args = commonArgs + arrayOf("principals", "delete", "--name=principals/user-1")

    callCli(args)

    val request: DeletePrincipalRequest = captureFirst {
      runBlocking { verify(principalsServiceMock).deletePrincipal(capture()) }
    }

    assertThat(request).isEqualTo(deletePrincipalRequest { name = "principals/user-1" })
  }

  @Test
  fun `principals lookup calls LookupPrincipal with valid request with oauth user`() {
    val args =
      commonArgs +
        arrayOf("principals", "lookup", "--issuer=example.com", "--subject=user1@example.com")

    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(USER_PRINCIPAL)
  }

  @Test
  fun `principals lookup calls LookupPrincipal with valid request with tls client`() {
    val args =
      commonArgs + arrayOf("principals", "lookup", "--principal-tls-client-cert-file=$MC_CERT_PATH")

    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(TLS_PRINCIPAL)
  }

  private val commonArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/reporting_tls.pem",
        "--tls-key-file=$SECRETS_DIR/reporting_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--access-public-api-target=$HOST:${server.port}",
      )

  private fun callCli(args: Array<String>): String {
    val capturedOutput = CommandLineTesting.capturingOutput(args, Access::main)
    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    return capturedOutput.out
  }

  companion object {
    private const val HOST = "localhost"
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!
    private val MC_CERT_PATH: Path = SECRETS_DIR.resolve("mc_tls.pem")
    private val MC_CERT_FILE: File = MC_CERT_PATH.toFile()
    private val TLS_CLIENT_AKID: ByteString = readCertificate(MC_CERT_FILE).authorityKeyIdentifier!!

    private val USER_PRINCIPAL = principal {
      name = "principals/user-1"
      user = oAuthUser {
        issuer = "example.com"
        subject = "user1@example.com"
      }
    }

    private val TLS_PRINCIPAL = principal {
      name = "principals/user-1"
      tlsClient = tlsClient { authorityKeyIdentifier = TLS_CLIENT_AKID }
    }

    private val LOOKUP_USER_PRINCIPAL_REQUEST = lookupPrincipalRequest {
      user = oAuthUser {
        issuer = "example.com"
        subject = "user1@example.com"
      }
    }

    private val LOOKUP_TLS_PRINCIPAL_REQUEST = lookupPrincipalRequest {
      tlsClient = tlsClient { authorityKeyIdentifier = TLS_CLIENT_AKID }
    }
  }
}
