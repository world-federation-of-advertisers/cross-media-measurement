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
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelShard
import org.wfanet.measurement.internal.kingdom.ModelShardsGrpcKt.ModelShardsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.deleteModelShardRequest
import org.wfanet.measurement.internal.kingdom.modelShard
import org.wfanet.measurement.internal.kingdom.modelSuite
import org.wfanet.measurement.internal.kingdom.streamModelShardsRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ModelShardsServiceTest<T : ModelShardsCoroutineImplBase> {
  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)

  protected data class Services<T>(
    val modelShardsService: T,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val modelProvidersService: ModelProvidersCoroutineImplBase,
    val modelSuitesService: ModelSuitesCoroutineImplBase,
    val modelReleasesService: ModelReleasesCoroutineImplBase,
    val populationsService: PopulationsCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var modelShardsService: T
    private set

  protected lateinit var modelProvidersService: ModelProvidersCoroutineImplBase
    private set

  protected lateinit var modelSuitesService: ModelSuitesCoroutineImplBase
    private set

  protected lateinit var modelReleasesService: ModelReleasesCoroutineImplBase
    private set

  protected lateinit var populationsService: PopulationsCoroutineImplBase
    private set

  protected abstract fun newServices(clock: Clock, idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(clock, idGenerator)
    modelShardsService = services.modelShardsService
    dataProvidersService = services.dataProvidersService
    modelProvidersService = services.modelProvidersService
    modelSuitesService = services.modelSuitesService
    modelReleasesService = services.modelReleasesService
    populationsService = services.populationsService
  }

  @Test
  fun `createModelShard succeeds`() = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
        },
        createdPopulation,
        modelReleasesService,
      )

    val modelShard = modelShard {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalModelProviderId = modelRelease.externalModelProviderId
      externalModelSuiteId = modelRelease.externalModelSuiteId
      externalModelReleaseId = modelRelease.externalModelReleaseId
      modelBlobPath = "modelBlobPath"
    }

    val createdModelShard = modelShardsService.createModelShard(modelShard)

    assertThat(createdModelShard)
      .ignoringFields(ModelShard.CREATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        modelShard {
          externalDataProviderId = createdModelShard.externalDataProviderId
          externalModelShardId = createdModelShard.externalModelShardId
          externalModelProviderId = createdModelShard.externalModelProviderId
          externalModelSuiteId = createdModelShard.externalModelSuiteId
          externalModelReleaseId = modelRelease.externalModelReleaseId
          modelBlobPath = "modelBlobPath"
        }
      )
  }

  @Test
  fun `createModelShard fails when Data Provider is not found`() = runBlocking {
    val modelShard = modelShard {
      externalDataProviderId = 123L
      externalModelProviderId = 123L
      externalModelSuiteId = 123L
      externalModelReleaseId = 123L
      modelBlobPath = "modelBlobPath"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelShardsService.createModelShard(modelShard) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `createModelShard fails when externalDataProviderId is missing`() = runBlocking {
    val modelShard = modelShard {}

    val exception =
      assertFailsWith<StatusRuntimeException> { modelShardsService.createModelShard(modelShard) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("DataProviderId field of ModelShard is missing.")
  }

  @Test
  fun `createModelShard fails when modelBlobPath is missing`() = runBlocking {
    val modelShard = modelShard { externalDataProviderId = 123L }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelShardsService.createModelShard(modelShard) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ModelBlobPath field of ModelShard is missing.")
  }

  @Test
  fun `createModelShard fails when Model Suite is not found`() = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
        },
        createdPopulation,
        modelReleasesService,
      )

    val modelShard = modelShard {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalModelProviderId = 123L
      externalModelSuiteId = 456L
      externalModelReleaseId = modelRelease.externalModelReleaseId
      modelBlobPath = "modelBlobPath"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelShardsService.createModelShard(modelShard) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelSuite not found")
  }

  @Test
  fun `createModelShard fails when Model Release is not found`() = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val modelShard = modelShard {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelReleaseId = 123L
      modelBlobPath = "modelBlobPath"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelShardsService.createModelShard(modelShard) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelRelease not found")
  }

  @Test
  fun `deleteModelShard succeeds`() = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
        },
        createdPopulation,
        modelReleasesService,
      )

    val modelShard = modelShard {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelReleaseId = modelRelease.externalModelReleaseId
      modelBlobPath = "modelBlobPath"
    }

    val createdModelShard = modelShardsService.createModelShard(modelShard)
    modelShardsService.deleteModelShard(
      deleteModelShardRequest {
        externalDataProviderId = createdModelShard.externalDataProviderId
        externalModelShardId = createdModelShard.externalModelShardId
      }
    )
    assertThat(
        modelShardsService
          .streamModelShards(
            streamModelShardsRequest {
              filter = filter { externalDataProviderId = createdModelShard.externalDataProviderId }
            }
          )
          .toList()
          .size
      )
      .isEqualTo(0)
  }

  @Test
  fun `deleteModelShard fails when external model release is owned by another model provider`() =
    runBlocking {
      val dataProvider = population.createDataProvider(dataProvidersService)
      val modelSuite1 = population.createModelSuite(modelProvidersService, modelSuitesService)
      val modelSuite2 = population.createModelSuite(modelProvidersService, modelSuitesService)
      val populationDataProvider = population.createDataProvider(dataProvidersService)
      val createdPopulation =
        population.createPopulation(populationDataProvider, populationsService)

      val modelRelease =
        population.createModelRelease(
          modelSuite {
            externalModelProviderId = modelSuite1.externalModelProviderId
            externalModelSuiteId = modelSuite1.externalModelSuiteId
          },
          createdPopulation,
          modelReleasesService,
        )

      val modelShard = modelShard {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalModelProviderId = modelSuite1.externalModelProviderId
        externalModelSuiteId = modelSuite1.externalModelSuiteId
        externalModelReleaseId = modelRelease.externalModelReleaseId
        modelBlobPath = "modelBlobPath"
      }
      val createdModelShard = modelShardsService.createModelShard(modelShard)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          modelShardsService.deleteModelShard(
            deleteModelShardRequest {
              externalDataProviderId = createdModelShard.externalDataProviderId
              externalModelShardId = createdModelShard.externalModelShardId
              externalModelProviderId = modelSuite2.externalModelProviderId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("Cannot delete ModelShard having ModelRelease owned by another ModelProvider.")
    }

  @Test
  fun `deleteModelShard fails when external data provider is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelShardsService.deleteModelShard(deleteModelShardRequest { externalModelShardId = 123L })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalDataProviderId unspecified")
  }

  @Test
  fun `deleteModelShard fails when external model shard is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelShardsService.deleteModelShard(
          deleteModelShardRequest { externalDataProviderId = 123L }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalModelShardId unspecified")
  }

  @Test
  fun `deleteModelShard fails when Data Provider is not found`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelShardsService.deleteModelShard(
          deleteModelShardRequest {
            externalDataProviderId = 123L
            externalModelShardId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider not found.")
  }

  @Test
  fun `deleteModelShard fails when Model Shard is not found`() = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelShardsService.deleteModelShard(
          deleteModelShardRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalModelShardId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelShard not found")
  }

  @Test
  fun `streamModelShards returns all model shards`(): Unit = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
        },
        createdPopulation,
        modelReleasesService,
      )

    val modelShard = modelShard {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelReleaseId = modelRelease.externalModelReleaseId
      modelBlobPath = "modelBlobPath"
    }

    val modelShard1 = modelShardsService.createModelShard(modelShard)
    val modelShard2 = modelShardsService.createModelShard(modelShard)

    val modelShards: List<ModelShard> =
      modelShardsService
        .streamModelShards(
          streamModelShardsRequest {
            filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
          }
        )
        .toList()

    assertThat(modelShards)
      .comparingExpectedFieldsOnly()
      .containsExactly(modelShard1, modelShard2)
      .inOrder()
  }

  @Test
  fun `streamModelShards returns all model shards for a given ModelProvider`(): Unit = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)
    val modelSuite1 = population.createModelSuite(modelProvidersService, modelSuitesService)
    val modelSuite2 = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease1 =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelSuite1.externalModelProviderId
          externalModelSuiteId = modelSuite1.externalModelSuiteId
        },
        createdPopulation,
        modelReleasesService,
      )

    val modelRelease2 =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelSuite2.externalModelProviderId
          externalModelSuiteId = modelSuite2.externalModelSuiteId
        },
        createdPopulation,
        modelReleasesService,
      )

    val modelShardProto1 = modelShard {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalModelProviderId = modelSuite1.externalModelProviderId
      externalModelSuiteId = modelSuite1.externalModelSuiteId
      externalModelReleaseId = modelRelease1.externalModelReleaseId
      modelBlobPath = "modelBlobPath"
    }

    val modelShardProto2 = modelShard {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalModelProviderId = modelSuite2.externalModelProviderId
      externalModelSuiteId = modelSuite2.externalModelSuiteId
      externalModelReleaseId = modelRelease2.externalModelReleaseId
      modelBlobPath = "modelBlobPath"
    }

    val modelShard1 = modelShardsService.createModelShard(modelShardProto1)
    modelShardsService.createModelShard(modelShardProto2)

    val modelShards: List<ModelShard> =
      modelShardsService
        .streamModelShards(
          streamModelShardsRequest {
            filter = filter {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalModelProviderId = modelSuite1.externalModelProviderId
            }
          }
        )
        .toList()

    assertThat(modelShards).comparingExpectedFieldsOnly().containsExactly(modelShard1)
  }

  @Test
  fun `streamModelShards can get one page at a time`(): Unit = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
        },
        createdPopulation,
        modelReleasesService,
      )

    val modelShard = modelShard {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelReleaseId = modelRelease.externalModelReleaseId
      modelBlobPath = "modelBlobPath"
    }

    val modelShard1 = modelShardsService.createModelShard(modelShard)
    val modelShard2 = modelShardsService.createModelShard(modelShard)

    val modelShards: List<ModelShard> =
      modelShardsService
        .streamModelShards(
          streamModelShardsRequest {
            limit = 1
            filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
          }
        )
        .toList()

    assertThat(modelShards).hasSize(1)
    assertThat(modelShards).contains(modelShard1)

    val modelShards2: List<ModelShard> =
      modelShardsService
        .streamModelShards(
          streamModelShardsRequest {
            limit = 1
            filter = filter {
              externalDataProviderId = dataProvider.externalDataProviderId
              after = afterFilter {
                createTime = modelShards[0].createTime
                externalDataProviderId = modelShards[0].externalDataProviderId
                externalModelShardId = modelShards[0].externalModelShardId
              }
            }
          }
        )
        .toList()

    assertThat(modelShards2).hasSize(1)
    assertThat(modelShards2).contains(modelShard2)
  }

  @Test
  fun `streamModelShards fails for missing after filter fields`(): Unit = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val populationDataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(populationDataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
        },
        createdPopulation,
        modelReleasesService,
      )

    val modelShard = modelShard {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelReleaseId = modelRelease.externalModelReleaseId
      modelBlobPath = "modelBlobPath"
    }

    modelShardsService.createModelShard(modelShard)
    modelShardsService.createModelShard(modelShard)

    val modelShards: List<ModelShard> =
      modelShardsService
        .streamModelShards(
          streamModelShardsRequest {
            limit = 1
            filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
          }
        )
        .toList()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelShardsService.streamModelShards(
          streamModelShardsRequest {
            limit = 1
            filter = filter {
              externalDataProviderId = dataProvider.externalDataProviderId
              after = afterFilter {
                createTime = modelShards[0].createTime
                externalModelShardId = modelShards[0].externalModelShardId
              }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Missing After filter fields")
  }

  @Test
  fun `streamModelShards fails when limit is less than 0`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelShardsService.streamModelShards(
          streamModelShardsRequest {
            limit = -1
            filter = filter { externalDataProviderId = 123L }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Limit cannot be less than 0")
  }
}
