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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.getModelSuiteRequest
import org.wfanet.measurement.internal.kingdom.modelSuite
import org.wfanet.measurement.internal.kingdom.streamModelSuitesRequest

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ModelSuitesServiceTest<T : ModelSuitesCoroutineImplBase> {

  protected data class Services<T>(
    val modelSuitesService: T,
    val modelProvidersService: ModelProvidersCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var modelProvidersService: ModelProvidersCoroutineImplBase
    private set

  protected lateinit var modelSuitesService: T
    private set

  protected abstract fun newServices(clock: Clock, idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(clock, idGenerator)
    modelSuitesService = services.modelSuitesService
    modelProvidersService = services.modelProvidersService
  }

  @Test
  fun `createModelSuite fails for missing 'DisplayName'`() = runBlocking {
    val modelSuite = modelSuite { description = "description" }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelSuitesService.createModelSuite(modelSuite) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("DisplayName field")
  }

  @Test
  fun `createModelSuite fails when Model Provider is not found`() = runBlocking {
    val modelSuite = modelSuite {
      externalModelProviderId = 123L
      displayName = "displayName"
      description = "description"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelSuitesService.createModelSuite(modelSuite) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelProvider not found")
  }

  @Test
  fun `createModelSuite succeeds`() = runBlocking {
    val modelProvider = population.createModelProvider(modelProvidersService)

    val modelSuite = modelSuite {
      externalModelProviderId = modelProvider.externalModelProviderId
      displayName = "displayName"
      description = "description"
    }
    val createdModelSuite = modelSuitesService.createModelSuite(modelSuite)

    assertThat(createdModelSuite)
      .ignoringFields(
        ModelSuite.CREATE_TIME_FIELD_NUMBER,
        ModelSuite.EXTERNAL_MODEL_SUITE_ID_FIELD_NUMBER,
      )
      .isEqualTo(
        modelSuite {
          externalModelProviderId = modelProvider.externalModelProviderId
          displayName = "displayName"
          description = "description"
        }
      )
  }

  @Test
  fun `getModelSuite returns created model suite`() = runBlocking {
    val modelProvider = population.createModelProvider(modelProvidersService)
    val modelSuite = modelSuite {
      externalModelProviderId = modelProvider.externalModelProviderId
      displayName = "displayName"
      description = "description"
    }
    val createdModelSuite = modelSuitesService.createModelSuite(modelSuite)

    val returnedModelSuite =
      modelSuitesService.getModelSuite(
        getModelSuiteRequest {
          externalModelProviderId = createdModelSuite.externalModelProviderId
          externalModelSuiteId = createdModelSuite.externalModelSuiteId
        }
      )

    assertThat(createdModelSuite)
      .ignoringFields(ModelSuite.CREATE_TIME_FIELD_NUMBER)
      .isEqualTo(returnedModelSuite)
  }

  @Test
  fun `getModelSuite fails for missing model suite`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelSuitesService.getModelSuite(
          getModelSuiteRequest {
            externalModelProviderId = 123L
            externalModelSuiteId = 456L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("NOT_FOUND: ModelSuite not found")
  }

  @Test
  fun `streamModelSuites returns all model suites`(): Unit = runBlocking {
    val modelProvider = population.createModelProvider(modelProvidersService)

    val modelSuite1 =
      modelSuitesService.createModelSuite(
        modelSuite {
          externalModelProviderId = modelProvider.externalModelProviderId
          displayName = "displayName1"
          description = "description1"
        }
      )

    val modelSuite2 =
      modelSuitesService.createModelSuite(
        modelSuite {
          externalModelProviderId = modelProvider.externalModelProviderId
          displayName = "displayName2"
          description = "description2"
        }
      )

    val modelSuite3 =
      modelSuitesService.createModelSuite(
        modelSuite {
          externalModelProviderId = modelProvider.externalModelProviderId
          displayName = "displayName3"
          description = "description3"
        }
      )

    val modelSuites: List<ModelSuite> =
      modelSuitesService
        .streamModelSuites(
          streamModelSuitesRequest {
            filter = filter { externalModelProviderId = modelProvider.externalModelProviderId }
          }
        )
        .toList()

    assertThat(modelSuites)
      .comparingExpectedFieldsOnly()
      .containsExactly(modelSuite1, modelSuite2, modelSuite3)
      .inOrder()
  }

  @Test
  fun `streamModelSuites can get one page at a time`(): Unit = runBlocking {
    val modelProvider = population.createModelProvider(modelProvidersService)

    val modelSuite1 =
      modelSuitesService.createModelSuite(
        modelSuite {
          externalModelProviderId = modelProvider.externalModelProviderId
          displayName = "displayName1"
          description = "description1"
        }
      )

    val modelSuite2 =
      modelSuitesService.createModelSuite(
        modelSuite {
          externalModelProviderId = modelProvider.externalModelProviderId
          displayName = "displayName2"
          description = "description2"
        }
      )

    val modelSuites: List<ModelSuite> =
      modelSuitesService
        .streamModelSuites(
          streamModelSuitesRequest {
            limit = 1
            filter = filter { externalModelProviderId = modelProvider.externalModelProviderId }
          }
        )
        .toList()

    assertThat(modelSuites).hasSize(1)
    assertThat(modelSuites).contains(modelSuite1)

    val modelSuites2: List<ModelSuite> =
      modelSuitesService
        .streamModelSuites(
          streamModelSuitesRequest {
            filter = filter {
              externalModelProviderId = modelProvider.externalModelProviderId
              after = afterFilter {
                createTime = modelSuites[0].createTime
                externalModelSuiteId = modelSuites[0].externalModelSuiteId
                externalModelProviderId = modelSuites[0].externalModelProviderId
              }
            }
          }
        )
        .toList()

    assertThat(modelSuites2).hasSize(1)
    assertThat(modelSuites2).contains(modelSuite2)
  }

  @Test
  fun `streamModelSuites fails for missing after filter fields`(): Unit = runBlocking {
    val modelProvider = population.createModelProvider(modelProvidersService)

    modelSuitesService.createModelSuite(
      modelSuite {
        externalModelProviderId = modelProvider.externalModelProviderId
        displayName = "displayName1"
        description = "description1"
      }
    )

    modelSuitesService.createModelSuite(
      modelSuite {
        externalModelProviderId = modelProvider.externalModelProviderId
        displayName = "displayName2"
        description = "description2"
      }
    )

    val modelSuites: List<ModelSuite> =
      modelSuitesService
        .streamModelSuites(
          streamModelSuitesRequest {
            limit = 1
            filter = filter { externalModelProviderId = modelProvider.externalModelProviderId }
          }
        )
        .toList()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelSuitesService.streamModelSuites(
          streamModelSuitesRequest {
            filter = filter {
              externalModelProviderId = modelProvider.externalModelProviderId
              after = afterFilter {
                createTime = modelSuites[0].createTime
                externalModelProviderId = modelSuites[0].externalModelProviderId
              }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Missing After filter fields")
  }
}
