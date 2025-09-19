/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.getModelReleaseRequest
import org.wfanet.measurement.internal.kingdom.modelRelease
import org.wfanet.measurement.internal.kingdom.populationKey
import org.wfanet.measurement.internal.kingdom.streamModelReleasesRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ModelReleasesServiceTest<T : ModelReleasesCoroutineImplBase> {
  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)

  protected data class Services<T>(
    val modelReleasesService: T,
    val modelSuitesService: ModelSuitesCoroutineImplBase,
    val modelProvidersService: ModelProvidersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val populationsService: PopulationsCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var modelProvidersService: ModelProvidersCoroutineImplBase
    private set

  protected lateinit var modelSuitesService: ModelSuitesCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var populationsService: PopulationsCoroutineImplBase
    private set

  protected lateinit var modelReleasesService: T
    private set

  protected abstract fun newServices(clock: Clock, idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(clock, idGenerator)
    modelReleasesService = services.modelReleasesService
    modelSuitesService = services.modelSuitesService
    modelProvidersService = services.modelProvidersService
    populationsService = services.populationsService
    dataProvidersService = services.dataProvidersService
  }

  @Test
  fun `createModelRelease succeeds`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease = modelRelease {
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalDataProviderId = createdPopulation.externalDataProviderId
      externalPopulationId = createdPopulation.externalPopulationId
    }
    val createdModelRelease = modelReleasesService.createModelRelease(modelRelease)

    assertThat(createdModelRelease)
      .ignoringFields(
        ModelRelease.CREATE_TIME_FIELD_NUMBER,
        ModelRelease.EXTERNAL_MODEL_RELEASE_ID_FIELD_NUMBER,
      )
      .isEqualTo(modelRelease)
  }

  @Test
  fun `createModelRelease fails when Model Suite is not found`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelRelease = modelRelease {
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = 123L
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelReleasesService.createModelRelease(modelRelease)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelSuite not found")
  }

  @Test
  fun `getModelRelease returns created model release`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease = modelRelease {
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalDataProviderId = createdPopulation.externalDataProviderId
      externalPopulationId = createdPopulation.externalPopulationId
    }
    val createdModelRelease = modelReleasesService.createModelRelease(modelRelease)

    val returnedModelRelease =
      modelReleasesService.getModelRelease(
        getModelReleaseRequest {
          externalModelProviderId = createdModelRelease.externalModelProviderId
          externalModelSuiteId = createdModelRelease.externalModelSuiteId
          externalModelReleaseId = createdModelRelease.externalModelReleaseId
        }
      )

    assertThat(createdModelRelease).isEqualTo(returnedModelRelease)
  }

  @Test
  fun `getModelRelease fails for missing model release`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelReleasesService.getModelRelease(
          getModelReleaseRequest {
            externalModelProviderId = 123L
            externalModelSuiteId = 456L
            externalModelReleaseId = 789L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("NOT_FOUND: ModelRelease not found.")
  }

  @Test
  fun `streamModelReleases returns all model releases`(): Unit = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease1 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalDataProviderId = createdPopulation.externalDataProviderId
          externalPopulationId = createdPopulation.externalPopulationId
        }
      )

    val modelRelease2 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalDataProviderId = createdPopulation.externalDataProviderId
          externalPopulationId = createdPopulation.externalPopulationId
        }
      )

    val modelRelease3 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalDataProviderId = createdPopulation.externalDataProviderId
          externalPopulationId = createdPopulation.externalPopulationId
        }
      )

    val modelReleases: List<ModelRelease> =
      modelReleasesService
        .streamModelReleases(
          streamModelReleasesRequest {
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
            }
          }
        )
        .toList()

    assertThat(modelReleases)
      .comparingExpectedFieldsOnly()
      .containsExactly(modelRelease1, modelRelease2, modelRelease3)
      .inOrder()
  }

  @Test
  fun `streamModelReleases returns ModelReleases filtered by Population`(): Unit = runBlocking {
    val pdp = population.createDataProvider(dataProvidersService)
    val populationResource = population.createPopulation(pdp, populationsService)
    val populationResource2 = population.createPopulation(pdp, populationsService)
    val modelProvider: ModelProvider = population.createModelProvider(modelProvidersService)
    val modelSuite: ModelSuite = population.createModelSuite(modelSuitesService, modelProvider)
    val modelSuite2 = population.createModelSuite(modelSuitesService, modelProvider)
    val modelRelease1 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalDataProviderId = populationResource.externalDataProviderId
          externalPopulationId = populationResource.externalPopulationId
        }
      )
    modelReleasesService.createModelRelease(
      modelRelease {
        externalModelProviderId = modelSuite.externalModelProviderId
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalDataProviderId = populationResource2.externalDataProviderId
        externalPopulationId = populationResource2.externalPopulationId
      }
    )
    val modelRelease3 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite2.externalModelProviderId
          externalModelSuiteId = modelSuite2.externalModelSuiteId
          externalDataProviderId = populationResource.externalDataProviderId
          externalPopulationId = populationResource.externalPopulationId
        }
      )

    val response =
      modelReleasesService
        .streamModelReleases(
          streamModelReleasesRequest {
            filter = filter {
              externalModelProviderId = modelProvider.externalModelProviderId
              populationKeyIn += populationKey {
                externalDataProviderId = populationResource.externalDataProviderId
                externalPopulationId = populationResource.externalPopulationId
              }
            }
          }
        )
        .toList()

    assertThat(response).containsExactly(modelRelease1, modelRelease3)
  }

  @Test
  fun `streamModelReleases can get one page at a time`(): Unit = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease1 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalDataProviderId = createdPopulation.externalDataProviderId
          externalPopulationId = createdPopulation.externalPopulationId
        }
      )

    val modelRelease2 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalDataProviderId = createdPopulation.externalDataProviderId
          externalPopulationId = createdPopulation.externalPopulationId
        }
      )

    val modelReleases: List<ModelRelease> =
      modelReleasesService
        .streamModelReleases(
          streamModelReleasesRequest {
            limit = 1
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
            }
          }
        )
        .toList()

    assertThat(modelReleases).hasSize(1)
    assertThat(modelReleases).contains(modelRelease1)

    val modelReleases2: List<ModelRelease> =
      modelReleasesService
        .streamModelReleases(
          streamModelReleasesRequest {
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
              after = afterFilter {
                createTime = modelReleases[0].createTime
                externalModelReleaseId = modelReleases[0].externalModelReleaseId
                externalModelSuiteId = modelReleases[0].externalModelSuiteId
                externalModelProviderId = modelReleases[0].externalModelProviderId
              }
            }
          }
        )
        .toList()

    assertThat(modelReleases2).hasSize(1)
    assertThat(modelReleases2).contains(modelRelease2)
  }

  @Test
  fun `streamModelReleases fails for missing after filter fields`(): Unit = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    modelReleasesService.createModelRelease(
      modelRelease {
        externalModelProviderId = modelSuite.externalModelProviderId
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalDataProviderId = createdPopulation.externalDataProviderId
        externalPopulationId = createdPopulation.externalPopulationId
      }
    )

    modelReleasesService.createModelRelease(
      modelRelease {
        externalModelProviderId = modelSuite.externalModelProviderId
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalDataProviderId = createdPopulation.externalDataProviderId
        externalPopulationId = createdPopulation.externalPopulationId
      }
    )

    val modelReleases: List<ModelRelease> =
      modelReleasesService
        .streamModelReleases(
          streamModelReleasesRequest {
            limit = 1
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
            }
          }
        )
        .toList()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelReleasesService
          .streamModelReleases(
            streamModelReleasesRequest {
              filter = filter {
                externalModelProviderId = modelSuite.externalModelProviderId
                externalModelSuiteId = modelSuite.externalModelSuiteId
                after = afterFilter {
                  createTime = modelReleases[0].createTime
                  externalModelSuiteId = modelReleases[0].externalModelSuiteId
                  externalModelProviderId = modelReleases[0].externalModelProviderId
                }
              }
            }
          )
          .toList()
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Missing After filter fields")
  }

  @Test
  fun `streamModelReleases fails when limit is less than 0`(): Unit = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelReleasesService
          .streamModelReleases(
            streamModelReleasesRequest {
              limit = -1
              filter = filter {
                externalModelProviderId = modelSuite.externalModelProviderId
                externalModelSuiteId = modelSuite.externalModelSuiteId
              }
            }
          )
          .toList()
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Limit cannot be less than 0")
  }
}
