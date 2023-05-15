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

import com.google.common.truth.Truth
import com.google.common.truth.extensions.proto.ProtoTruth
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
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.getModelReleaseRequest
import org.wfanet.measurement.internal.kingdom.modelRelease
import org.wfanet.measurement.internal.kingdom.modelSuite
import org.wfanet.measurement.internal.kingdom.streamModelReleasesRequest

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ModelReleasesServiceTest<T : ModelReleasesCoroutineImplBase> {

  protected data class Services<T>(
    val modelReleasesService: T,
    val modelSuitesService: ModelSuitesCoroutineImplBase,
    val modelProvidersService: ModelProvidersCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var modelProvidersService: ModelProvidersCoroutineImplBase
    private set

  protected lateinit var modelSuitesService: ModelSuitesCoroutineImplBase
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
  }

  @Test
  fun `createModelRelease succeeds`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelRelease = modelRelease {
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
    }
    val createdModelRelease = modelReleasesService.createModelRelease(modelRelease)

    ProtoTruth.assertThat(createdModelRelease)
      .ignoringFields(
        ModelRelease.CREATE_TIME_FIELD_NUMBER,
        ModelRelease.EXTERNAL_MODEL_RELEASE_ID_FIELD_NUMBER
      )
      .isEqualTo(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
        }
      )
  }

  @Test
  fun `getModelRelease returns created model release`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val modelRelease = modelRelease {
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
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

    ProtoTruth.assertThat(createdModelRelease)
      .ignoringFields(ModelRelease.CREATE_TIME_FIELD_NUMBER)
      .isEqualTo(returnedModelRelease)
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

    Truth.assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    Truth.assertThat(exception).hasMessageThat().contains("NOT_FOUND: ModelRelease not found.")
  }

  @Test
  fun `streamModelReleases returns all model releases`(): Unit = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelRelease1 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
        }
      )

    val modelRelease2 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
        }
      )

    val modelRelease3 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
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

    ProtoTruth.assertThat(modelReleases)
      .comparingExpectedFieldsOnly()
      .containsExactly(modelRelease1, modelRelease2, modelRelease3)
      .inOrder()
  }

  @Test
  fun `streamModelReleases can get one page at a time`(): Unit = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelRelease1 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
        }
      )

    val modelRelease2 =
      modelReleasesService.createModelRelease(
        modelRelease {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
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

    ProtoTruth.assertThat(modelReleases).hasSize(1)
    ProtoTruth.assertThat(modelReleases).contains(modelRelease1)

    val modelReleases2: List<ModelRelease> =
      modelReleasesService
        .streamModelReleases(
          streamModelReleasesRequest {
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
              createdAfter = modelReleases[0].createTime
            }
          }
        )
        .toList()

    ProtoTruth.assertThat(modelReleases2).hasSize(1)
    ProtoTruth.assertThat(modelReleases2).contains(modelRelease2)
  }
}
