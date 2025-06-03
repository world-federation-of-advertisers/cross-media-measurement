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
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v2alpha.AkidPrincipalLookup
import org.wfanet.measurement.api.v2alpha.ListModelSuitesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpc
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpc.ModelSuitesBlockingStub
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.listModelSuitesPageToken
import org.wfanet.measurement.api.v2alpha.listModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.api.v2alpha.withPrincipalsFromX509AuthorityKeyIdentifiers
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMapKt
import org.wfanet.measurement.config.authorityKeyToPrincipalMap
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.kingdom.ModelProvider as InternalModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt as InternalModelProvidersGrpc
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt as InternalModelSuitesGrpc
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.deploy.common.service.toList
import org.wfanet.measurement.kingdom.deploy.tools.ModelRepository
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelSuitesService

abstract class InProcessModelRepositoryCliIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  verboseGrpcLogging: Boolean = true,
) {
  private val internalApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      val services =
        kingdomDataServicesRule.value.buildDataServices().toList().map { it.bindService() }
      for (service in services) {
        addService(service)
      }
    }

  @get:Rule
  val ruleChain: TestRule = chainRulesSequentially(kingdomDataServicesRule, internalApiServer)

  private lateinit var publicModelSuitesClient: ModelSuitesBlockingStub

  private lateinit var server: CommonServer

  private lateinit var internalModelProvider: InternalModelProvider
  private lateinit var modelProviderName: String
  private lateinit var modelSuite: ModelSuite

  @Before
  fun startServer() {
    val internalChannel: Channel = internalApiServer.channel
    val internalModelProvidersService =
      InternalModelProvidersGrpc.ModelProvidersCoroutineStub(internalChannel)
    internalModelProvider = runBlocking {
      internalModelProvidersService.createModelProvider(
        modelProvider { externalModelProviderId = FIXED_GENERATED_EXTERNAL_ID }
      )
    }

    val modelProviderApiId = externalIdToApiId(internalModelProvider.externalModelProviderId)
    modelProviderName = ModelProviderKey(modelProviderApiId).toName()

    val principalLookup =
      AkidPrincipalLookup(
        config =
          authorityKeyToPrincipalMap {
            entries +=
              AuthorityKeyToPrincipalMapKt.entry {
                authorityKeyIdentifier =
                  readCertificate(MODEL_PROVIDER_TLS_CERT_FILE).authorityKeyIdentifier!!
                principalResourceName = modelProviderName
              }
          }
      )

    val internalModelSuitesClient =
      InternalModelSuitesGrpc.ModelSuitesCoroutineStub(internalChannel)

    val publicModelSuitesServices =
      ModelSuitesService(internalModelSuitesClient)
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
    val services = listOf(publicModelSuitesServices)

    val serverCerts =
      SigningCerts.fromPemFiles(
        MODEL_PROVIDER_TLS_CERT_FILE,
        MODEL_PROVIDER_TLS_KEY_FILE,
        MODEL_PROVIDER_CERT_COLLECTION_FILE,
      )

    server =
      CommonServer.fromParameters(
        verboseGrpcLogging = true,
        certs = serverCerts,
        clientAuth = ClientAuth.REQUIRE,
        nameForLogging = "model-repository-cli-integration-test",
        services = services,
      )
    server.start()

    val publicChannel: ManagedChannel =
      buildMutualTlsChannel("localhost:${server.port}", serverCerts)
    publicModelSuitesClient = ModelSuitesGrpc.newBlockingStub(publicChannel)

    modelSuite =
      publicModelSuitesClient.createModelSuite(
        createModelSuiteRequest {
          parent = modelProviderName
          modelSuite = modelSuite {
            displayName = DISPLAY_NAME
            description = DESCRIPTION
          }
        }
      )
  }

  @After
  fun shutdownServer() {
    server.close()
  }

  @Test
  fun `model-suites get prints ModelSuite`() = runBlocking {
    var args = commonArgs + arrayOf("model-suites", "get", modelSuite.name)
    var output = callCli(args)

    assertThat(parseTextProto(output.reader(), ModelSuite.getDefaultInstance()))
      .isEqualTo(modelSuite)
  }

  @Test
  fun `model-suites create prints ModelSuite`() = runBlocking {
    val args =
      commonArgs +
        arrayOf(
          "model-suites",
          "create",
          "--parent=$modelProviderName",
          "--display-name=$DISPLAY_NAME",
          "--description=$DESCRIPTION",
        )
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), ModelSuite.getDefaultInstance()))
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        modelSuite {
          displayName = DISPLAY_NAME
          description = DESCRIPTION
        }
      )
  }

  @Test
  fun `model-suites list prints ModelSuites`() = runBlocking {
    val createdModelSuite2 =
      publicModelSuitesClient.createModelSuite(
        createModelSuiteRequest {
          parent = modelProviderName
          modelSuite = modelSuite {
            displayName = DISPLAY_NAME
            description = DESCRIPTION
          }
        }
      )

    val pageToken = listModelSuitesPageToken {
      pageSize = PAGE_SIZE
      externalModelProviderId = internalModelProvider.externalModelProviderId
      lastModelSuite = previousPageEnd {
        externalModelProviderId = internalModelProvider.externalModelProviderId
        externalModelSuiteId =
          apiIdToExternalId(ModelSuiteKey.fromName(modelSuite.name)!!.modelSuiteId)
        createTime = modelSuite.createTime
      }
    }

    val args =
      commonArgs +
        arrayOf(
          "model-suites",
          "list",
          "--parent=$modelProviderName",
          "--page-size=$PAGE_SIZE",
          "--page-token=${pageToken.toByteArray().base64UrlEncode()}",
        )
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), ListModelSuitesResponse.getDefaultInstance()))
      .isEqualTo(listModelSuitesResponse { modelSuites += createdModelSuite2 })
  }

  private fun callCli(args: Array<String>): String {
    val capturedOutput = CommandLineTesting.capturingOutput(args, ModelRepository::main)
    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    return capturedOutput.out
  }

  private val commonArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$MODEL_PROVIDER_TLS_CERT_FILE",
        "--tls-key-file=$MODEL_PROVIDER_TLS_KEY_FILE",
        "--cert-collection-file=$MODEL_PROVIDER_CERT_COLLECTION_FILE",
        "--kingdom-public-api-target=$HOST:${server.port}",
      )

  companion object {
    private const val HOST = "localhost"
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()

    private val MODEL_PROVIDER_TLS_CERT_FILE: File = SECRETS_DIR.resolve("mp1_tls.pem")
    private val MODEL_PROVIDER_TLS_KEY_FILE: File = SECRETS_DIR.resolve("mp1_tls.key")
    private val MODEL_PROVIDER_CERT_COLLECTION_FILE: File = SECRETS_DIR.resolve("mp1_root.pem")

    private const val FIXED_GENERATED_EXTERNAL_ID = 6789L

    private const val DISPLAY_NAME = "Display name"
    private const val DESCRIPTION = "Description"

    private const val PAGE_SIZE = 50

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
