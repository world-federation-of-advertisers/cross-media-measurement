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

package org.wfanet.measurement.integration.common

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import io.grpc.Server
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.io.File
import java.nio.file.Paths
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.ListModelSuitesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.listModelSuitesPageToken
import org.wfanet.measurement.api.v2alpha.listModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.HeaderCapturingInterceptor
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.kingdom.deploy.tools.ModelRepository

abstract class InProcessModelRepositoryCliIntegrationTest(verboseGrpcLogging: Boolean = true) {
  private val headerInterceptor = HeaderCapturingInterceptor()

  private val modelSuitesServiceMock: ModelSuitesCoroutineImplBase = mockService {
    onBlocking { getModelSuite(any()) }.thenReturn(MODEL_SUITE)
    onBlocking { createModelSuite(any()) }.thenReturn(MODEL_SUITE)
    onBlocking { listModelSuites(any()) }
      .thenReturn(
        listModelSuitesResponse {
          modelSuites += MODEL_SUITE
          modelSuites += MODEL_SUITE_2
          nextPageToken = LIST_MODEL_SUITES_PAGE_TOKEN_2.toByteString().base64UrlEncode()
        }
      )
  }

  val services: List<ServerServiceDefinition> =
    listOf(ServerInterceptors.intercept(modelSuitesServiceMock, headerInterceptor))

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

  @Test
  fun `model-suites get prints ModelSuite`() = runBlocking {
    var args = commonArgs + arrayOf("model-suites", "get", MODEL_SUITE_NAME)
    var output = callCli(args)
    assertThat(parseTextProto(output.reader(), ModelSuite.getDefaultInstance()))
      .isEqualTo(MODEL_SUITE)
  }

  @Test
  fun `model-suites create prints ModelSuite`() = runBlocking {
    val args =
      commonArgs +
        arrayOf(
          "model-suites",
          "create",
          "--parent=$MODEL_PROVIDER_NAME",
          "--display-name=$DISPLAY_NAME",
          "--description=$DESCRIPTION",
        )

    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), ModelSuite.getDefaultInstance()))
      .isEqualTo(MODEL_SUITE)
  }

  @Test
  fun `model-suites list prints ModelSuites`() = runBlocking {
    val args =
      commonArgs +
        arrayOf(
          "model-suites",
          "list",
          "--page-size=50",
          "--page-token=${LIST_MODEL_SUITES_PAGE_TOKEN.toByteArray().base64UrlEncode()}",
        )
    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), ListModelSuitesResponse.getDefaultInstance()))
      .isEqualTo(
        listModelSuitesResponse {
          modelSuites += MODEL_SUITE
          modelSuites += MODEL_SUITE_2
          nextPageToken = LIST_MODEL_SUITES_PAGE_TOKEN_2.toByteString().base64UrlEncode()
        }
      )
  }

  private fun callCli(args: Array<String>): String {
    val capturedOutput = CommandLineTesting.capturingOutput(args, ModelRepository::main)
    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    return capturedOutput.out
  }

  private val commonArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$KINGDOM_TLS_CERT_FILE",
        "--tls-key-file=$KINGDOM_TLS_KEY_FILE",
        "--cert-collection-file=$KINGDOM_CERT_COLLECTION_FILE",
        "--kingdom-public-api-target=$HOST:${publicApiServer.port}",
      )

  companion object {
    private const val HOST = "localhost"
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()
    private val KINGDOM_TLS_CERT_FILE: File = SECRETS_DIR.resolve("kingdom_tls.pem")
    private val KINGDOM_TLS_KEY_FILE: File = SECRETS_DIR.resolve("kingdom_tls.key")
    private val KINGDOM_CERT_COLLECTION_FILE: File = SECRETS_DIR.resolve("kingdom_root.pem")
    private val kingdomSigningCerts =
      SigningCerts.fromPemFiles(
        KINGDOM_TLS_CERT_FILE,
        KINGDOM_TLS_KEY_FILE,
        KINGDOM_CERT_COLLECTION_FILE,
      )

    private const val DISPLAY_NAME = "Display name"
    private const val DESCRIPTION = "Description"
    private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

    private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
    private val EXTERNAL_MODEL_PROVIDER_ID =
      apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)

    private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
    private const val MODEL_SUITE_NAME_2 = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAJs"
    private val EXTERNAL_MODEL_SUITE_ID =
      apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME)!!.modelSuiteId)
    private val EXTERNAL_MODEL_SUITE_ID_2 =
      apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME_2)!!.modelSuiteId)
    private val MODEL_SUITE: ModelSuite = modelSuite {
      name = MODEL_SUITE_NAME
      displayName = DISPLAY_NAME
      description = DESCRIPTION
      createTime = CREATE_TIME
    }
    private val MODEL_SUITE_2: ModelSuite = modelSuite {
      name = MODEL_SUITE_NAME_2
      displayName = DISPLAY_NAME
      description = DESCRIPTION
      createTime = CREATE_TIME
    }

    private const val PAGE_SIZE = 50
    private val LIST_MODEL_SUITES_PAGE_TOKEN = listModelSuitesPageToken {
      pageSize = PAGE_SIZE
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      lastModelSuite = previousPageEnd {
        createTime = CREATE_TIME
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID - 1
      }
    }

    private val LIST_MODEL_SUITES_PAGE_TOKEN_2 = listModelSuitesPageToken {
      pageSize = PAGE_SIZE
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      lastModelSuite = previousPageEnd {
        createTime = CREATE_TIME
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID_2
      }
    }
  }
}
