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
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.absolutePathString
import kotlin.io.path.createTempFile
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.CommandLineTesting.assertThat
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
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

