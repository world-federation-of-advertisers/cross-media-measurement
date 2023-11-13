/*
 * Copyright 2023 The Cross-Media Measurement Authors
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
import com.google.protobuf.any
import com.google.protobuf.kotlin.toByteString
import io.grpc.Server
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.io.File
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.CreateCertificateRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.publicKey
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.CommandLineTesting.assertThat
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.common.testing.HeaderCapturingInterceptor
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey

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

private val CLIENT_TLS_CERT: File = SECRETS_DIR.resolve("edp1_tls.pem")
private val CLIENT_TLS_KEY: File = SECRETS_DIR.resolve("edp1_tls.key")
private val CLIENT_TRUSTED_CERTS: File = SECRETS_DIR.resolve("edp_trusted_certs.pem")

private val CLIENT_CERTIFICATE_DER = SECRETS_DIR.resolve("edp1_cs_cert.der").readByteString()
private val CLIENT_CERTIFICATE = certificate {
  name = DATA_PROVIDER_CERTIFICATE_NAME
  x509Der = CLIENT_CERTIFICATE_DER
}

private val kingdomSigningCerts =
  SigningCerts.fromPemFiles(KINGDOM_TLS_CERT, KINGDOM_TLS_KEY, KINGDOM_TRUSTED_CERTS)

private const val DATA_PROVIDER_NAME = "dataProviders/1"
private const val DATA_PROVIDER_CERTIFICATE_NAME = "dataProviders/1/certificates/1"
private val DATA_PROVIDER_PUBLIC_KEY =
  loadPublicKey(SECRETS_DIR.resolve("edp1_enc_public.tink")).toEncryptionPublicKey()

private val DATA_PROVIDER = dataProvider {
  name = DATA_PROVIDER_NAME
  certificate = DATA_PROVIDER_CERTIFICATE_NAME
  publicKey = signedMessage { setMessage(DATA_PROVIDER_PUBLIC_KEY.pack()) }
}

@RunWith(JUnit4::class)
class CertificatesTest {
  private val headerInterceptor = HeaderCapturingInterceptor()

  private val dataProvidersServiceMock: DataProvidersGrpcKt.DataProvidersCoroutineImplBase =
    mockService {
      onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
    }
  private val certificatesServiceMock: CertificatesGrpcKt.CertificatesCoroutineImplBase =
    mockService {
      onBlocking { getCertificate(any()) }.thenReturn(CLIENT_CERTIFICATE)
    }

  val services: List<ServerServiceDefinition> =
    listOf(
      dataProvidersServiceMock.bindService(),
      ServerInterceptors.intercept(certificatesServiceMock, headerInterceptor),
    )

  private val publicApiServer: Server =
    NettyServerBuilder.forPort(0)
      .sslContext(kingdomSigningCerts.toServerTlsContext())
      .addServices(services)
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

  private val commonArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$CLIENT_TLS_CERT",
        "--tls-key-file=$CLIENT_TLS_KEY",
        "--cert-collection-file=$CLIENT_TRUSTED_CERTS",
        "--kingdom-public-api-target=localhost:${publicApiServer.port}"
      )

  private fun callCli(args: Array<String>): String {
    val capturedOutput = CommandLineTesting.capturingOutput(args, Certificates::main)
    assertThat(capturedOutput).status().isEqualTo(0)
    return capturedOutput.out
  }

  @Test
  fun `certificates create calls CreateCertificate with correct params`() {
    val args =
      arrayOf(
        "create",
        "--parent-resource=dataProviders/777",
        "--certificate-der-file=$SECRETS_DIR/edp1_result_cs_cert.der",
      ) + commonArgs
    callCli(args)

    val request: CreateCertificateRequest = captureFirst {
      runBlocking { verify(certificatesServiceMock).createCertificate(capture()) }
    }

    val certificate = request.certificate
    val x509 = readCertificate(File("$SECRETS_DIR/edp1_result_cs_cert.der"))
    assertThat(certificate)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        certificate {
          x509Der = x509.encoded.toByteString()
          subjectKeyIdentifier = x509.subjectKeyIdentifier!!
        }
      )
  }

  companion object {
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }
  }
}
