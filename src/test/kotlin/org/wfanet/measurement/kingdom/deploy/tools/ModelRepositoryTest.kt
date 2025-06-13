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

package org.wfanet.measurement.kingdom.deploy.tools

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit.SECONDS
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.CreateModelLineRequest
import org.wfanet.measurement.api.v2alpha.CreateModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.CreatePopulationRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.GetModelProviderRequest
import org.wfanet.measurement.api.v2alpha.GetModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.GetPopulationRequest
import org.wfanet.measurement.api.v2alpha.ListModelProvidersPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListModelProvidersRequest
import org.wfanet.measurement.api.v2alpha.ListModelProvidersResponse
import org.wfanet.measurement.api.v2alpha.ListModelSuitesPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.ListModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.ListPopulationsPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListPopulationsRequest
import org.wfanet.measurement.api.v2alpha.ListPopulationsResponse
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelProvider
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.PopulationKt.populationBlob
import org.wfanet.measurement.api.v2alpha.PopulationsGrpcKt
import org.wfanet.measurement.api.v2alpha.SetModelLineActiveEndTimeRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.eventTemplate
import org.wfanet.measurement.api.v2alpha.getModelProviderRequest
import org.wfanet.measurement.api.v2alpha.getModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.getPopulationRequest
import org.wfanet.measurement.api.v2alpha.listModelProvidersPageToken
import org.wfanet.measurement.api.v2alpha.listModelProvidersRequest
import org.wfanet.measurement.api.v2alpha.listModelProvidersResponse
import org.wfanet.measurement.api.v2alpha.listModelSuitesPageToken
import org.wfanet.measurement.api.v2alpha.listModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.listModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.listPopulationsPageToken
import org.wfanet.measurement.api.v2alpha.listPopulationsRequest
import org.wfanet.measurement.api.v2alpha.listPopulationsResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelProvider
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.api.v2alpha.setModelLineActiveEndTimeRequest
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime

@RunWith(JUnit4::class)
class ModelRepositoryTest {
  private val modelProvidersServiceMock: ModelProvidersCoroutineImplBase = mockService {
    onBlocking { getModelProvider(any()) }.thenReturn(MODEL_PROVIDER)
    onBlocking { listModelProviders(any()) }
      .thenReturn(
        listModelProvidersResponse {
          modelProviders += MODEL_PROVIDER
          modelProviders += MODEL_PROVIDER_2
          nextPageToken = LIST_MODEL_PROVIDERS_PAGE_TOKEN_2.toByteString().base64UrlEncode()
        }
      )
  }

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

  private val modelReleasesServiceMock: ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase =
    mockService {
      onBlocking { createModelRelease(any()) }.thenReturn(MODEL_RELEASE)
    }

  private val modelRolloutsServiceMock: ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase =
    mockService {
      onBlocking { createModelRollout(any()) }.thenReturn(MODEL_ROLLOUT)
    }

  private val populationsServiceMock: PopulationsGrpcKt.PopulationsCoroutineImplBase = mockService {
    onBlocking { getPopulation(any()) }.thenReturn(POPULATION)
    onBlocking { createPopulation(any()) }.thenReturn(POPULATION)
    onBlocking { listPopulations(any()) }
      .thenReturn(
        listPopulationsResponse {
          populations += POPULATION_2
          populations += POPULATION
          nextPageToken = LIST_POPULATIONS_PAGE_TOKEN_2.toByteString().base64UrlEncode()
        }
      )
  }

  private val modelLinesServiceMock: ModelLinesGrpcKt.ModelLinesCoroutineImplBase = mockService {
    onBlocking { createModelLine(any()) }.thenReturn(MODEL_LINE)
    onBlocking { setModelLineActiveEndTime(any()) }
      .thenReturn(MODEL_LINE.copy { activeEndTime = Timestamps.parse(ACTIVE_END_TIME_2) })
  }

  private val serverCerts =
    SigningCerts.fromPemFiles(
      certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
      privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
      trustedCertCollectionFile = SECRETS_DIR.resolve("kingdom_root.pem").toFile(),
    )

  private val services: List<ServerServiceDefinition> =
    listOf(
      modelProvidersServiceMock.bindService(),
      modelSuitesServiceMock.bindService(),
      populationsServiceMock.bindService(),
      modelLinesServiceMock.bindService(),
      modelReleasesServiceMock.bindService(),
      modelRolloutsServiceMock.bindService(),
    )

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
  fun `modelProviders get calls GetModelProvider with valid request`() {
    val args = commonArgs + arrayOf("model-providers", "get", MODEL_PROVIDER_NAME)

    val output = callCli(args)

    val request: GetModelProviderRequest = captureFirst {
      runBlocking { verify(modelProvidersServiceMock).getModelProvider(capture()) }
    }

    assertThat(request).isEqualTo(getModelProviderRequest { name = MODEL_PROVIDER_NAME })
    assertThat(parseTextProto(output.reader(), ModelProvider.getDefaultInstance()))
      .isEqualTo(MODEL_PROVIDER)
  }

  @Test
  fun `modelProviders list calls ListModelProvider with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "model-providers",
          "list",
          "--page-size=50",
          "--page-token=${LIST_MODEL_PROVIDERS_PAGE_TOKEN.toByteArray().base64UrlEncode()}",
        )
    val output = callCli(args)

    val request: ListModelProvidersRequest = captureFirst {
      runBlocking { verify(modelProvidersServiceMock).listModelProviders(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        listModelProvidersRequest {
          pageSize = 50
          pageToken = LIST_MODEL_PROVIDERS_PAGE_TOKEN.toByteArray().base64UrlEncode()
        }
      )
    assertThat(parseTextProto(output.reader(), ListModelProvidersResponse.getDefaultInstance()))
      .isEqualTo(
        listModelProvidersResponse {
          modelProviders += MODEL_PROVIDER
          modelProviders += MODEL_PROVIDER_2
          nextPageToken = LIST_MODEL_PROVIDERS_PAGE_TOKEN_2.toByteString().base64UrlEncode()
        }
      )
  }

  @Test
  fun `modelSuites get calls GetModelSuite with valid request`() {
    val args = commonArgs + arrayOf("model-suites", "get", MODEL_SUITE_NAME)

    val output = callCli(args)

    val request: GetModelSuiteRequest = captureFirst {
      runBlocking { verify(modelSuitesServiceMock).getModelSuite(capture()) }
    }

    assertThat(request).isEqualTo(getModelSuiteRequest { name = MODEL_SUITE_NAME })
    assertThat(parseTextProto(output.reader(), ModelSuite.getDefaultInstance()))
      .isEqualTo(MODEL_SUITE)
  }

  @Test
  fun `modelSuites create calls CreateModelSuite with valid request`() {
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

    val request: CreateModelSuiteRequest = captureFirst {
      runBlocking { verify(modelSuitesServiceMock).createModelSuite(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        createModelSuiteRequest {
          parent = MODEL_PROVIDER_NAME
          modelSuite = modelSuite {
            displayName = DISPLAY_NAME
            description = DESCRIPTION
          }
        }
      )
    assertThat(parseTextProto(output.reader(), ModelSuite.getDefaultInstance()))
      .isEqualTo(MODEL_SUITE)
  }

  @Test
  fun `modelSuites list calls ListModelSuite with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "model-suites",
          "list",
          "--parent=$MODEL_PROVIDER_NAME",
          "--page-size=50",
          "--page-token=${LIST_MODEL_SUITES_PAGE_TOKEN.toByteArray().base64UrlEncode()}",
        )
    val output = callCli(args)

    val request: ListModelSuitesRequest = captureFirst {
      runBlocking { verify(modelSuitesServiceMock).listModelSuites(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        listModelSuitesRequest {
          parent = MODEL_PROVIDER_NAME
          pageSize = 50
          pageToken = LIST_MODEL_SUITES_PAGE_TOKEN.toByteArray().base64UrlEncode()
        }
      )
    assertThat(parseTextProto(output.reader(), ListModelSuitesResponse.getDefaultInstance()))
      .isEqualTo(
        listModelSuitesResponse {
          modelSuites += MODEL_SUITE
          modelSuites += MODEL_SUITE_2
          nextPageToken = LIST_MODEL_SUITES_PAGE_TOKEN_2.toByteString().base64UrlEncode()
        }
      )
  }

  @Test
  fun `populations get calls GetPopulation with valid request`() {
    val args = commonArgs + arrayOf("populations", "get", POPULATION_NAME)

    val output = callCli(args)

    val request: GetPopulationRequest = captureFirst {
      runBlocking { verify(populationsServiceMock).getPopulation(capture()) }
    }

    assertThat(request).isEqualTo(getPopulationRequest { name = POPULATION_NAME })
    assertThat(parseTextProto(output.reader(), Population.getDefaultInstance()))
      .isEqualTo(POPULATION)
  }

  @Test
  fun `populations create calls CreatePopulation with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "populations",
          "create",
          "--parent=$DATA_PROVIDER_NAME",
          "--description=$DESCRIPTION",
          "--model-blob-uri=$MODEL_BLOB_URI",
          "--event-template-type=$EVENT_TEMPLATE_TYPE",
        )

    val output = callCli(args)

    val request: CreatePopulationRequest = captureFirst {
      runBlocking { verify(populationsServiceMock).createPopulation(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        createPopulationRequest {
          parent = DATA_PROVIDER_NAME
          population = population {
            description = DESCRIPTION
            eventTemplate = eventTemplate { type = EVENT_TEMPLATE_TYPE }
            populationBlob = populationBlob { modelBlobUri = MODEL_BLOB_URI }
          }
        }
      )
    assertThat(parseTextProto(output.reader(), Population.getDefaultInstance()))
      .isEqualTo(POPULATION)
  }

  @Test
  fun `populations list calls ListPopulations with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "populations",
          "list",
          "--parent=$DATA_PROVIDER_NAME",
          "--page-size=50",
          "--page-token=${LIST_POPULATIONS_PAGE_TOKEN.toByteArray().base64UrlEncode()}",
        )
    val output = callCli(args)

    val request: ListPopulationsRequest = captureFirst {
      runBlocking { verify(populationsServiceMock).listPopulations(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        listPopulationsRequest {
          parent = DATA_PROVIDER_NAME
          pageSize = 50
          pageToken = LIST_POPULATIONS_PAGE_TOKEN.toByteArray().base64UrlEncode()
        }
      )
    assertThat(parseTextProto(output.reader(), ListPopulationsResponse.getDefaultInstance()))
      .isEqualTo(
        listPopulationsResponse {
          populations += POPULATION_2
          populations += POPULATION
          nextPageToken = LIST_POPULATIONS_PAGE_TOKEN_2.toByteString().base64UrlEncode()
        }
      )
  }

  @Test
  fun `modelLines create calls CreateModelLine with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "model-lines",
          "create",
          "--parent=$MODEL_SUITE_NAME",
          "--display-name=$DISPLAY_NAME",
          "--description=$DESCRIPTION",
          "--active-start-time=$ACTIVE_START_TIME",
          "--active-end-time=$ACTIVE_END_TIME",
          "--type=$MODEL_LINE_TYPE_DEV",
          "--holdback-model-line=$HOLDBACK_MODEL_LINE_NAME",
          "--population=$POPULATION_NAME",
        )

    val output = callCli(args)

    val request: CreateModelLineRequest = captureFirst {
      runBlocking { verify(modelLinesServiceMock).createModelLine(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        createModelLineRequest {
          parent = MODEL_SUITE_NAME
          modelLine = MODEL_LINE.copy { clearName() }
        }
      )
    assertThat(parseTextProto(output.reader(), ModelLine.getDefaultInstance()))
      .comparingExpectedFieldsOnly()
      .isEqualTo(MODEL_LINE)
  }

  @Test
  fun `modelLines set-active-end-time calls SetModelLineActiveEndTime with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "model-lines",
          "set-active-end-time",
          "--name=$MODEL_LINE_NAME",
          "--active-end-time=$ACTIVE_END_TIME_2",
        )

    val output = callCli(args)

    val request: SetModelLineActiveEndTimeRequest = captureFirst {
      runBlocking { verify(modelLinesServiceMock).setModelLineActiveEndTime(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        setModelLineActiveEndTimeRequest {
          name = MODEL_LINE_NAME
          activeEndTime = Timestamps.parse(ACTIVE_END_TIME_2)
        }
      )
    assertThat(parseTextProto(output.reader(), ModelLine.getDefaultInstance()))
      .isEqualTo(MODEL_LINE.copy { activeEndTime = Timestamps.parse(ACTIVE_END_TIME_2) })
  }

  private val commonArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/kingdom_tls.pem",
        "--tls-key-file=$SECRETS_DIR/kingdom_tls.key",
        "--cert-collection-file=$SECRETS_DIR/kingdom_root.pem",
        "--kingdom-public-api-target=$HOST:${server.port}",
      )

  private fun callCli(args: Array<String>): String {
    val capturedOutput = CommandLineTesting.capturingOutput(args, ModelRepository::main)
    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    return capturedOutput.out
  }

  companion object {
    private const val HOST = "localhost"
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!

    private const val DISPLAY_NAME = "Display name"
    private const val DESCRIPTION = "Description"
    private const val EVENT_TEMPLATE_TYPE = "event_template_type"
    private const val MODEL_BLOB_URI = "model_blob_uri"
    private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private val EXTERNAL_DATA_PROVIDER_ID =
      apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)

    private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
    private const val MODEL_PROVIDER_NAME_2 = "modelProviders/AAAAAAAAAHs"
    private val EXTERNAL_MODEL_PROVIDER_ID =
      apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
    private val EXTERNAL_MODEL_PROVIDER_ID_2 =
      apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME_2)!!.modelProviderId)
    private val MODEL_PROVIDER = modelProvider {
      name = MODEL_PROVIDER_NAME
      displayName = "Default model provider"
    }
    private val MODEL_PROVIDER_2 = modelProvider {
      name = MODEL_PROVIDER_NAME_2
      displayName = "Alternative model provider"
    }
    private val LIST_MODEL_PROVIDERS_PAGE_TOKEN = listModelProvidersPageToken {
      pageSize = PAGE_SIZE
      lastModelProvider =
        ListModelProvidersPageTokenKt.previousPageEnd {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID - 1
        }
    }
    private val LIST_MODEL_PROVIDERS_PAGE_TOKEN_2 = listModelProvidersPageToken {
      pageSize = PAGE_SIZE
      lastModelProvider =
        ListModelProvidersPageTokenKt.previousPageEnd {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID_2
        }
    }

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
    private val LIST_MODEL_SUITES_PAGE_TOKEN = listModelSuitesPageToken {
      pageSize = PAGE_SIZE
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      lastModelSuite =
        ListModelSuitesPageTokenKt.previousPageEnd {
          createTime = CREATE_TIME
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID - 1
        }
    }

    private val LIST_MODEL_SUITES_PAGE_TOKEN_2 = listModelSuitesPageToken {
      pageSize = PAGE_SIZE
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      lastModelSuite =
        ListModelSuitesPageTokenKt.previousPageEnd {
          createTime = CREATE_TIME
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID_2
        }
    }

    private const val POPULATION_NAME = "$DATA_PROVIDER_NAME/populations/AAAAAAAAAHs"
    private const val POPULATION_NAME_2 = "$DATA_PROVIDER_NAME/populations/AAAAAAAAAJs"
    private val EXTERNAL_POPULATION_ID =
      apiIdToExternalId(PopulationKey.fromName(POPULATION_NAME)!!.populationId)
    private val EXTERNAL_POPULATION_ID_2 =
      apiIdToExternalId(PopulationKey.fromName(POPULATION_NAME_2)!!.populationId)
    private val POPULATION: Population = population {
      name = POPULATION_NAME
      description = DESCRIPTION
      createTime = CREATE_TIME
      eventTemplate = eventTemplate { type = EVENT_TEMPLATE_TYPE }
      populationBlob = populationBlob { modelBlobUri = MODEL_BLOB_URI }
    }
    private val POPULATION_2: Population = population {
      name = POPULATION_NAME_2
      description = DESCRIPTION
      createTime = CREATE_TIME
      eventTemplate = eventTemplate { type = EVENT_TEMPLATE_TYPE }
      populationBlob = populationBlob { modelBlobUri = MODEL_BLOB_URI }
    }

    private const val MODEL_LINE_NAME = "$MODEL_SUITE_NAME/modelLines/AAAAAAAAAHs"
    private val MODEL_LINE: ModelLine = modelLine {
      name = MODEL_LINE_NAME
      displayName = DISPLAY_NAME
      description = DESCRIPTION
      activeStartTime = Timestamps.parse(ACTIVE_START_TIME)
      activeEndTime = Timestamps.parse(ACTIVE_END_TIME)
      type = ModelLine.Type.DEV
      holdbackModelLine = HOLDBACK_MODEL_LINE_NAME
    }
    private const val ACTIVE_START_TIME = "2025-01-15T10:00:00Z"
    private const val ACTIVE_END_TIME = "2025-02-15T10:00:00Z"
    private const val ACTIVE_END_TIME_2 = "2025-03-15T10:00:00Z"
    private const val MODEL_LINE_TYPE_DEV = "DEV"
    private const val HOLDBACK_MODEL_LINE_NAME = "$MODEL_SUITE_NAME/modelLines/BBBBBBBBBHs"

    private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAHs"
    private val MODEL_RELEASE = modelRelease {
      name = MODEL_RELEASE_NAME
      createTime = CREATE_TIME
      population = POPULATION_NAME
    }

    private const val MODEL_ROLLOUT_NAME = "$MODEL_LINE_NAME/modelRollouts/AAAAAAAAAHs"
    private val MODEL_ROLLOUT = modelRollout {
      name = MODEL_ROLLOUT_NAME
      instantRolloutDate =
        MODEL_LINE.activeStartTime.toInstant().atZone(ZoneOffset.UTC).toLocalDate().toProtoDate()
      modelRelease = MODEL_RELEASE_NAME
      createTime = CREATE_TIME
      updateTime = CREATE_TIME
    }

    private const val PAGE_SIZE = 50

    private val LIST_POPULATIONS_PAGE_TOKEN = listPopulationsPageToken {
      pageSize = PAGE_SIZE
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      lastPopulation =
        ListPopulationsPageTokenKt.previousPageEnd {
          createTime = CREATE_TIME
          externalPopulationId = EXTERNAL_POPULATION_ID - 1
        }
    }

    private val LIST_POPULATIONS_PAGE_TOKEN_2 = listPopulationsPageToken {
      pageSize = PAGE_SIZE
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      lastPopulation =
        ListPopulationsPageTokenKt.previousPageEnd {
          createTime = CREATE_TIME
          externalPopulationId = EXTERNAL_POPULATION_ID_2
        }
    }
  }
}
