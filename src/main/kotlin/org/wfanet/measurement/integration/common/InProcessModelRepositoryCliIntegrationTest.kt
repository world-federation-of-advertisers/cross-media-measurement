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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.protobuf.util.Timestamps
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
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v2alpha.AkidPrincipalLookup
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ListModelLinesResponse
import org.wfanet.measurement.api.v2alpha.ListModelProvidersResponse
import org.wfanet.measurement.api.v2alpha.ListModelSuitesPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.ListPopulationsPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListPopulationsResponse
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpc
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpc.ModelLinesBlockingStub
import org.wfanet.measurement.api.v2alpha.ModelProvider
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProvidersGrpc
import org.wfanet.measurement.api.v2alpha.ModelProvidersGrpc.ModelProvidersBlockingStub
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpc
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpc.ModelSuitesBlockingStub
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationsGrpc
import org.wfanet.measurement.api.v2alpha.PopulationsGrpc.PopulationsBlockingStub
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.listModelProvidersRequest
import org.wfanet.measurement.api.v2alpha.listModelSuitesPageToken
import org.wfanet.measurement.api.v2alpha.listModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.listPopulationsPageToken
import org.wfanet.measurement.api.v2alpha.listPopulationsResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.withPrincipalsFromX509AuthorityKeyIdentifiers
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMapKt
import org.wfanet.measurement.config.authorityKeyToPrincipalMap
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt as InternalDataProvidersGrpc
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt as InternalModelLinesGrpc
import org.wfanet.measurement.internal.kingdom.ModelProvider as InternalModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt as InternalModelProvidersGrpc
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt as InternalModelReleasesGrpc
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt as InternalModelRolloutsGrpc
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt as InternalModelSuitesGrpc
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt as InternalPopulationsGrpc
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.certificateDetails
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.dataProviderDetails
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.deploy.common.service.toList
import org.wfanet.measurement.kingdom.deploy.tools.ModelRepository
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelLinesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelProvidersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelReleasesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelRolloutsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelSuitesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.PopulationsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.toModelProvider

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

  @get:Rule val tempDir = TemporaryFolder()

  private lateinit var publicModelProvidersClient: ModelProvidersBlockingStub
  private lateinit var publicModelSuitesClient: ModelSuitesBlockingStub
  private lateinit var publicPopulationsClient: PopulationsBlockingStub
  private lateinit var publicModelLinesClient: ModelLinesBlockingStub

  private lateinit var server: CommonServer

  private lateinit var internalDataProvider: InternalDataProvider
  private lateinit var dataProviderName: String

  private lateinit var internalModelProvider: InternalModelProvider
  private lateinit var modelProviderName: String
  private lateinit var internalModelProvider2: InternalModelProvider

  private lateinit var modelSuite: ModelSuite

  @Before
  fun startServer() {
    val internalChannel = internalApiServer.channel
    val internalModelProvidersService =
      InternalModelProvidersGrpc.ModelProvidersCoroutineStub(internalChannel)
    internalModelProvider = runBlocking {
      internalModelProvidersService.createModelProvider(InternalModelProvider.getDefaultInstance())
    }
    internalModelProvider2 = runBlocking {
      internalModelProvidersService.createModelProvider(InternalModelProvider.getDefaultInstance())
    }
    val modelProviderApiId = externalIdToApiId(internalModelProvider.externalModelProviderId)
    modelProviderName = ModelProviderKey(modelProviderApiId).toName()

    val internalDataProvidersService =
      InternalDataProvidersGrpc.DataProvidersCoroutineStub(internalChannel)
    internalDataProvider = runBlocking {
      internalDataProvidersService.createDataProvider(
        dataProvider {
          certificate {
            notValidBefore = timestamp { seconds = 12345 }
            notValidAfter = timestamp { seconds = 23456 }
            details = certificateDetails {
              x509Der = ByteString.copyFromUtf8("This is a certificate der.")
            }
          }
          details = dataProviderDetails {
            apiVersion = "v2alpha"
            publicKey = ByteString.copyFromUtf8("This is a public key.")
            publicKeySignature = ByteString.copyFromUtf8("This is a public key signature.")
          }
        }
      )
    }
    val dataProviderApiId = externalIdToApiId(internalDataProvider.externalDataProviderId)
    dataProviderName = DataProviderKey(dataProviderApiId).toName()

    val principalLookup =
      AkidPrincipalLookup(
        config =
          authorityKeyToPrincipalMap {
            entries +=
              AuthorityKeyToPrincipalMapKt.entry {
                authorityKeyIdentifier =
                  readCertificate(DATA_PROVIDER_TLS_CERT_FILE).authorityKeyIdentifier!!
                principalResourceName = dataProviderName
              }
            entries +=
              AuthorityKeyToPrincipalMapKt.entry {
                authorityKeyIdentifier =
                  readCertificate(MODEL_PROVIDER_TLS_CERT_FILE).authorityKeyIdentifier!!
                principalResourceName = modelProviderName
              }
          }
      )

    val internalModelProvidersClient =
      InternalModelProvidersGrpc.ModelProvidersCoroutineStub(internalChannel)
    val internalModelSuitesClient =
      InternalModelSuitesGrpc.ModelSuitesCoroutineStub(internalChannel)
    val internalPopulationsClient =
      InternalPopulationsGrpc.PopulationsCoroutineStub(internalChannel)
    val internalModelLinesClient = InternalModelLinesGrpc.ModelLinesCoroutineStub(internalChannel)
    val internalModelReleasesClient =
      InternalModelReleasesGrpc.ModelReleasesCoroutineStub(internalChannel)
    val internalModelRolloutsClient =
      InternalModelRolloutsGrpc.ModelRolloutsCoroutineStub(internalChannel)

    val publicModelProvidersService =
      ModelProvidersService(internalModelProvidersClient)
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
    val publicModelSuitesService =
      ModelSuitesService(internalModelSuitesClient)
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
    val publicPopulationsService =
      PopulationsService(internalPopulationsClient)
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
    val publicModelLinesService =
      ModelLinesService(internalModelLinesClient)
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
    val publicModelReleasesService =
      ModelReleasesService(internalModelReleasesClient)
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
    val publicModelRolloutsService =
      ModelRolloutsService(internalModelRolloutsClient)
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
    val services =
      listOf(
        publicModelProvidersService,
        publicModelSuitesService,
        publicPopulationsService,
        publicModelLinesService,
        publicModelReleasesService,
        publicModelRolloutsService,
      )

    val serverCerts =
      SigningCerts.fromPemFiles(KINGDOM_TLS_CERT_FILE, KINGDOM_TLS_KEY_FILE, ALL_ROOT_CERT_FILE)

    server =
      CommonServer.fromParameters(
        verboseGrpcLogging = true,
        certs = serverCerts,
        clientAuth = ClientAuth.REQUIRE,
        nameForLogging = "model-repository-cli-integration-test",
        services = services,
      )
    server.start()

    val modelProviderCerts =
      SigningCerts.fromPemFiles(
        MODEL_PROVIDER_TLS_CERT_FILE,
        MODEL_PROVIDER_TLS_KEY_FILE,
        KINGDOM_ROOT_CERT_FILE,
      )

    val publicModelProviderChannel: ManagedChannel =
      buildMutualTlsChannel("localhost:${server.port}", modelProviderCerts)
    publicModelProvidersClient = ModelProvidersGrpc.newBlockingStub(publicModelProviderChannel)
    publicModelSuitesClient = ModelSuitesGrpc.newBlockingStub(publicModelProviderChannel)
    publicModelLinesClient = ModelLinesGrpc.newBlockingStub(publicModelProviderChannel)

    modelSuite = createModelSuite()
    publicModelSuitesClient = ModelSuitesGrpc.newBlockingStub(publicModelProviderChannel)
    publicPopulationsClient = PopulationsGrpc.newBlockingStub(publicModelProviderChannel)

    val dataProviderCerts =
      SigningCerts.fromPemFiles(
        DATA_PROVIDER_TLS_CERT_FILE,
        DATA_PROVIDER_TLS_KEY_FILE,
        KINGDOM_ROOT_CERT_FILE,
      )

    val publicDataProviderChannel: ManagedChannel =
      buildMutualTlsChannel("localhost:${server.port}", dataProviderCerts)
    publicPopulationsClient = PopulationsGrpc.newBlockingStub(publicDataProviderChannel)
  }

  @After
  fun shutdownServer() {
    server.close()
  }

  @Test
  fun `model-providers get prints ModelProvider`() {
    val modelProvider = internalModelProvider.toModelProvider()

    val args = modelProviderArgs + arrayOf("model-providers", "get", modelProvider.name)
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), ModelProvider.getDefaultInstance()))
      .isEqualTo(modelProvider)
  }

  @Test
  fun `model-providers list prints ModelProviders`() {
    val args = modelProviderArgs + arrayOf("model-providers", "list", "--page-size=1")
    val output = callCli(args)

    val response: ListModelProvidersResponse =
      parseTextProto(output.reader(), ListModelProvidersResponse.getDefaultInstance())

    assertThat(response.modelProvidersList).hasSize(1)
    assertThat(response.nextPageToken).isNotEmpty()
  }

  @Test
  fun `model-providers list with page token prints ModelProviders`() {
    var response: ListModelProvidersResponse =
      publicModelProvidersClient.listModelProviders(listModelProvidersRequest { pageSize = 1 })

    val args =
      modelProviderArgs +
        arrayOf(
          "model-providers",
          "list",
          "--page-size=1",
          "--page-token=${response.nextPageToken}",
        )
    val output = callCli(args)

    response = parseTextProto(output.reader(), ListModelProvidersResponse.getDefaultInstance())

    assertThat(response.modelProvidersList).hasSize(1)
    assertThat(response.nextPageToken).isEmpty()
  }

  @Test
  fun `model-suites get prints ModelSuite`() {
    val modelSuite = createModelSuite()

    val args = modelProviderArgs + arrayOf("model-suites", "get", modelSuite.name)
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), ModelSuite.getDefaultInstance()))
      .isEqualTo(modelSuite)
  }

  @Test
  fun `model-suites create prints ModelSuite`() {
    val args =
      modelProviderArgs +
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
  fun `model-suites list prints ModelSuites`() {
    val modelSuite = createModelSuite()
    val modelSuite2 = createModelSuite()

    val pageToken = listModelSuitesPageToken {
      pageSize = PAGE_SIZE
      externalModelProviderId = internalModelProvider.externalModelProviderId
      lastModelSuite =
        ListModelSuitesPageTokenKt.previousPageEnd {
          externalModelProviderId = internalModelProvider.externalModelProviderId
          externalModelSuiteId =
            apiIdToExternalId(ModelSuiteKey.fromName(modelSuite.name)!!.modelSuiteId)
          createTime = modelSuite.createTime
        }
    }

    val args =
      modelProviderArgs +
        arrayOf(
          "model-suites",
          "list",
          "--parent=$modelProviderName",
          "--page-size=$PAGE_SIZE",
          "--page-token=${pageToken.toByteArray().base64UrlEncode()}",
        )
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), ListModelSuitesResponse.getDefaultInstance()))
      .isEqualTo(listModelSuitesResponse { modelSuites += modelSuite2 })
  }

  @Test
  fun `populations get prints Population`() {
    val population = createPopulation()

    val args = dataProviderArgs + arrayOf("populations", "get", population.name)
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), Population.getDefaultInstance()))
      .isEqualTo(population)
  }

  @Test
  fun `populations create prints Population`() {
    val populationSpecFile: File = tempDir.root.resolve("population-spec.binpb")
    populationSpecFile.writeBytes(POPULATION_SPEC.toByteArray())
    val args =
      dataProviderArgs +
        arrayOf(
          "populations",
          "create",
          "--parent=$dataProviderName",
          "--description=$DESCRIPTION",
          "--population-spec=${populationSpecFile.path}",
          "--event-message-descriptor-set=$EVENT_MESSAGE_DESCRIPTOR_SET_PATH",
          "--event-message-type-url=$EVENT_MESSAGE_TYPE_URL",
        )
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), Population.getDefaultInstance()))
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        population {
          description = DESCRIPTION
          populationSpec = POPULATION_SPEC
        }
      )
  }

  @Test
  fun `populations list prints Populations`() {
    val population = createPopulation()
    val population2 = createPopulation()

    val pageToken = listPopulationsPageToken {
      pageSize = PAGE_SIZE
      externalDataProviderId = internalDataProvider.externalDataProviderId
      lastPopulation =
        ListPopulationsPageTokenKt.previousPageEnd {
          externalDataProviderId = internalDataProvider.externalDataProviderId
          externalPopulationId =
            apiIdToExternalId(PopulationKey.fromName(population2.name)!!.populationId)
          createTime = population2.createTime
        }
    }

    val args =
      dataProviderArgs +
        arrayOf(
          "populations",
          "list",
          "--parent=$dataProviderName",
          "--page-size=$PAGE_SIZE",
          "--page-token=${pageToken.toByteArray().base64UrlEncode()}",
        )
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), ListPopulationsResponse.getDefaultInstance()))
      .isEqualTo(listPopulationsResponse { populations += population })
  }

  @Test
  fun `model-lines create prints ModelLine`() {
    val holdback = createModelLine(modelLineType = ModelLine.Type.HOLDBACK)
    val population = createPopulation()

    val args =
      modelProviderArgs +
        arrayOf(
          "model-lines",
          "create",
          "--parent=${modelSuite.name}",
          "--display-name=$DISPLAY_NAME",
          "--description=$DESCRIPTION",
          "--active-start-time=$START_TIME",
          "--active-end-time=$END_TIME",
          "--type=PROD",
          "--holdback-model-line=${holdback.name}",
          "--population=${population.name}",
        )

    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), ModelLine.getDefaultInstance()))
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        modelLine {
          displayName = DISPLAY_NAME
          description = DESCRIPTION
          activeStartTime = Timestamps.parse(START_TIME)
          activeEndTime = Timestamps.parse(END_TIME)
          type = ModelLine.Type.PROD
          holdbackModelLine = holdback.name
        }
      )
  }

  @Test
  fun `model-lines get prints ModelLine`() {
    val modelLine = createModelLine()

    val args = modelProviderArgs + arrayOf("model-lines", "get", modelLine.name)
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), ModelLine.getDefaultInstance())).isEqualTo(modelLine)
  }

  @Test
  fun `model-lines list prints ModelLines`() {
    val modelLine1 = createModelLine()
    val modelLine2 = createModelLine()

    val args =
      modelProviderArgs +
        arrayOf(
          "model-lines",
          "list",
          "--parent=${modelSuite.name}",
          "--page-size=1",
          "--type=PROD",
        )
    val output = callCli(args)

    val response = parseTextProto(output.reader(), ListModelLinesResponse.getDefaultInstance())
    assertThat(response.modelLinesList).containsExactly(modelLine1)
    assertThat(response.nextPageToken).isNotEmpty()

    val args2 =
      modelProviderArgs +
        arrayOf(
          "model-lines",
          "list",
          "--parent=${modelSuite.name}",
          "--page-size=1",
          "--page-token=${response.nextPageToken}",
          "--type=PROD",
        )
    val output2 = callCli(args2)
    val response2 = parseTextProto(output2.reader(), ListModelLinesResponse.getDefaultInstance())
    assertThat(response2.modelLinesList).containsExactly(modelLine2)
    assertThat(response2.nextPageToken).isEmpty()
  }

  @Test
  fun `model-lines set-active-end-time prints ModelLine`() {
    val modelLine = createModelLine()
    val newEndTime = "2099-04-15T10:00:00Z"
    val args =
      modelProviderArgs +
        arrayOf(
          "model-lines",
          "set-active-end-time",
          "--name=${modelLine.name}",
          "--active-end-time=$newEndTime",
        )

    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), ModelLine.getDefaultInstance()))
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        modelLine.copy {
          activeEndTime = Timestamps.parse(newEndTime)
          clearUpdateTime()
        }
      )
  }

  private fun callCli(args: Array<String>): String {
    val capturedOutput = CommandLineTesting.capturingOutput(args, ModelRepository::main)
    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    return capturedOutput.out
  }

  private fun createModelSuite(): ModelSuite {
    return publicModelSuitesClient.createModelSuite(
      createModelSuiteRequest {
        parent = modelProviderName
        modelSuite = modelSuite {
          displayName = DISPLAY_NAME
          description = DESCRIPTION
        }
      }
    )
  }

  private fun createPopulation(): Population {
    return publicPopulationsClient.createPopulation(
      createPopulationRequest {
        parent = dataProviderName
        population = population {
          description = DESCRIPTION
          populationSpec = POPULATION_SPEC
        }
      }
    )
  }

  private fun createModelLine(
    modelLineType: ModelLine.Type = ModelLine.Type.PROD,
    holdbackName: String = "",
  ): ModelLine {
    return publicModelLinesClient.createModelLine(
      createModelLineRequest {
        parent = modelSuite.name
        modelLine = modelLine {
          displayName = DISPLAY_NAME
          description = DESCRIPTION
          activeStartTime = Timestamps.parse(START_TIME)
          activeEndTime = Timestamps.parse(END_TIME)
          type = modelLineType
          if (holdbackName.isNotEmpty()) {
            holdbackModelLine = holdbackName
          }
        }
      }
    )
  }

  private val modelProviderArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$MODEL_PROVIDER_TLS_CERT_FILE",
        "--tls-key-file=$MODEL_PROVIDER_TLS_KEY_FILE",
        "--cert-collection-file=$KINGDOM_ROOT_CERT_FILE",
        "--kingdom-public-api-target=$HOST:${server.port}",
      )

  private val dataProviderArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$DATA_PROVIDER_TLS_CERT_FILE",
        "--tls-key-file=$DATA_PROVIDER_TLS_KEY_FILE",
        "--cert-collection-file=$KINGDOM_ROOT_CERT_FILE",
        "--kingdom-public-api-target=$HOST:${server.port}",
      )

  companion object {
    private const val HOST = "localhost"
    private const val MODULE_REPO_NAME = "wfa_measurement_system"
    private val SECRETS_DIR: File =
      getRuntimePath(Paths.get(MODULE_REPO_NAME, "src", "main", "k8s", "testing", "secretfiles"))!!
        .toFile()

    private val KINGDOM_TLS_CERT_FILE: File = SECRETS_DIR.resolve("kingdom_tls.pem")
    private val KINGDOM_TLS_KEY_FILE: File = SECRETS_DIR.resolve("kingdom_tls.key")
    private val KINGDOM_ROOT_CERT_FILE: File = SECRETS_DIR.resolve("kingdom_root.pem")

    private val MODEL_PROVIDER_TLS_CERT_FILE: File = SECRETS_DIR.resolve("mp1_tls.pem")
    private val MODEL_PROVIDER_TLS_KEY_FILE: File = SECRETS_DIR.resolve("mp1_tls.key")

    private val DATA_PROVIDER_TLS_CERT_FILE: File = SECRETS_DIR.resolve("edp1_tls.pem")
    private val DATA_PROVIDER_TLS_KEY_FILE: File = SECRETS_DIR.resolve("edp1_tls.key")

    private val ALL_ROOT_CERT_FILE: File = SECRETS_DIR.resolve("all_root_certs.pem")

    private const val DISPLAY_NAME = "Display name"
    private const val DESCRIPTION = "Description"

    private const val PAGE_SIZE = 50

    private const val START_TIME = "2099-01-15T10:00:00Z"
    private const val END_TIME = "2099-02-15T10:00:00Z"

    private const val EVENT_MESSAGE_TYPE_URL =
      "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
    private val EVENT_MESSAGE_DESCRIPTOR_SET_PATH =
      getRuntimePath(
        Paths.get(
          MODULE_REPO_NAME,
          "src",
          "main",
          "proto",
          "wfa",
          "measurement",
          "api",
          "v2alpha",
          "event_templates",
          "testing",
          "test_event_descriptor_set.pb",
        )
      )!!

    private val POPULATION_SPEC = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1
              endVidInclusive = 100
            }
          attributes +=
            ProtoAny.pack(
              person {
                gender = Person.Gender.FEMALE
                ageGroup = Person.AgeGroup.YEARS_18_TO_34
                socialGradeGroup = Person.SocialGradeGroup.A_B_C1
              }
            )
        }
    }

    init {
      DuchyInfo.setForTest(emptySet())
    }

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
