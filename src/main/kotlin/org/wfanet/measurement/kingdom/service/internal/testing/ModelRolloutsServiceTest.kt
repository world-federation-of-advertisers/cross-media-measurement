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
import java.time.Instant
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
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.rolloutPeriod
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.deleteModelRolloutRequest
import org.wfanet.measurement.internal.kingdom.modelRollout
import org.wfanet.measurement.internal.kingdom.modelSuite
import org.wfanet.measurement.internal.kingdom.scheduleModelRolloutFreezeRequest
import org.wfanet.measurement.internal.kingdom.streamModelRolloutsRequest

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ModelRolloutsServiceTest<T : ModelRolloutsCoroutineImplBase> {
  protected data class Services<T>(
    val modelRolloutsService: T,
    val modelProvidersService: ModelProvidersCoroutineImplBase,
    val modelSuitesService: ModelSuitesCoroutineImplBase,
    val modelLinesService: ModelLinesCoroutineImplBase,
    val modelReleasesService: ModelReleasesCoroutineImplBase,
  )

  protected val testClock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(testClock, Random(RANDOM_SEED))
  private val population = Population(testClock, idGenerator)

  protected lateinit var modelProvidersService: ModelProvidersCoroutineImplBase
    private set

  protected lateinit var modelSuitesService: ModelSuitesCoroutineImplBase
    private set

  protected lateinit var modelLinesService: ModelLinesCoroutineImplBase
    private set

  protected lateinit var modelReleasesService: ModelReleasesCoroutineImplBase
    private set

  protected lateinit var modelRolloutsService: T
    private set

  protected abstract fun newServices(testClock: Clock, idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(testClock, idGenerator)
    modelRolloutsService = services.modelRolloutsService
    modelProvidersService = services.modelProvidersService
    modelSuitesService = services.modelSuitesService
    modelLinesService = services.modelLinesService
    modelReleasesService = services.modelReleasesService
  }

  @Test
  fun `createModelRollout succeeds`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodStartTime = Instant.now().plusSeconds(100L).toProtoTime()
      rolloutPeriodEndTime = Instant.now().plusSeconds(200L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    val createdModelRollout = modelRolloutsService.createModelRollout(modelRollout)

    assertThat(createdModelRollout)
      .ignoringFields(
        ModelRollout.CREATE_TIME_FIELD_NUMBER,
        ModelRollout.UPDATE_TIME_FIELD_NUMBER,
        ModelRollout.ROLLOUT_FREEZE_TIME_FIELD_NUMBER,
        ModelRollout.EXTERNAL_MODEL_ROLLOUT_ID_FIELD_NUMBER,
      )
      .isEqualTo(modelRollout.copy { externalPreviousModelRolloutId = 0L })
  }

  @Test
  fun `createModelRollout succeeds when rollout start time is equal to rollout end time`() =
    runBlocking {
      val modelLine =
        population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
      val modelRelease =
        population.createModelRelease(
          modelSuite {
            externalModelProviderId = modelLine.externalModelProviderId
            externalModelSuiteId = modelLine.externalModelSuiteId
          },
          modelReleasesService
        )

      val modelRollout = modelRollout {
        externalModelProviderId = modelLine.externalModelProviderId
        externalModelSuiteId = modelLine.externalModelSuiteId
        externalModelLineId = modelLine.externalModelLineId
        rolloutPeriodStartTime = Instant.now().plusSeconds(100L).toProtoTime()
        rolloutPeriodEndTime = Instant.now().plusSeconds(100L).toProtoTime()
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
      val createdModelRollout = modelRolloutsService.createModelRollout(modelRollout)

      assertThat(createdModelRollout)
        .ignoringFields(
          ModelRollout.CREATE_TIME_FIELD_NUMBER,
          ModelRollout.UPDATE_TIME_FIELD_NUMBER,
          ModelRollout.ROLLOUT_FREEZE_TIME_FIELD_NUMBER,
          ModelRollout.EXTERNAL_MODEL_ROLLOUT_ID_FIELD_NUMBER,
        )
        .isEqualTo(modelRollout.copy { externalPreviousModelRolloutId = 0L })
    }

  @Test
  fun `createModelRollout correctly sets previous model rollout`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodStartTime = Instant.now().plusSeconds(100L).toProtoTime()
      rolloutPeriodEndTime = Instant.now().plusSeconds(100L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    val createdModelRollout = modelRolloutsService.createModelRollout(modelRollout)

    val modelRollout2 = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodStartTime = Instant.now().plusSeconds(200L).toProtoTime()
      rolloutPeriodEndTime = Instant.now().plusSeconds(300L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }

    val createdModelRollout2 = modelRolloutsService.createModelRollout(modelRollout2)

    assertThat(createdModelRollout2.externalPreviousModelRolloutId)
      .isEqualTo(createdModelRollout.externalModelRolloutId)
  }

  @Test
  fun `createModelRollout fails when rollout period start time is missing`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodEndTime = Instant.now().plusSeconds(200L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.createModelRollout(modelRollout)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("RolloutPeriodStartTime field of ModelRollout is missing.")
  }

  @Test
  fun `createModelRollout fails when rollout period end time is missing`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodStartTime = Instant.now().plusSeconds(200L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.createModelRollout(modelRollout)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("RolloutPeriodEndTime field of ModelRollout is missing.")
  }

  @Test
  fun `createModelRollout fails when external model release id is missing`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodStartTime = Instant.now().plusSeconds(200L).toProtoTime()
      rolloutPeriodEndTime = Instant.now().plusSeconds(300L).toProtoTime()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.createModelRollout(modelRollout)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("ExternalModelReleaseId field of ModelRollout is missing.")
  }

  @Test
  fun `createModelRollout fails when Model Line is not found`() = runBlocking {
    val modelRollout = modelRollout {
      externalModelProviderId = 123L
      externalModelSuiteId = 123L
      externalModelLineId = 123L
      rolloutPeriodStartTime = Instant.now().plusSeconds(100L).toProtoTime()
      rolloutPeriodEndTime = Instant.now().plusSeconds(200L).toProtoTime()
      externalModelReleaseId = 123L
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.createModelRollout(modelRollout)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelLine not found")
  }

  @Test
  fun `createModelRollout fails when rollout period start time is in the past`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodStartTime = Instant.now().minusSeconds(300L).toProtoTime()
      rolloutPeriodEndTime = Instant.now().minusSeconds(200L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.createModelRollout(modelRollout)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("RolloutPeriodStartTime must be in the future.")
  }

  @Test
  fun `createModelRollout fails when rollout period end time precedes rollout period start time`() =
    runBlocking {
      val modelLine =
        population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
      val modelRelease =
        population.createModelRelease(
          modelSuite {
            externalModelProviderId = modelLine.externalModelProviderId
            externalModelSuiteId = modelLine.externalModelSuiteId
          },
          modelReleasesService
        )

      val modelRollout = modelRollout {
        externalModelProviderId = modelLine.externalModelProviderId
        externalModelSuiteId = modelLine.externalModelSuiteId
        externalModelLineId = modelLine.externalModelLineId
        rolloutPeriodStartTime = Instant.now().plusSeconds(300L).toProtoTime()
        rolloutPeriodEndTime = Instant.now().plusSeconds(200L).toProtoTime()
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          modelRolloutsService.createModelRollout(modelRollout)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("RolloutPeriodEndTime cannot precede RolloutPeriodStartTime.")
    }

  @Test
  fun `createModelRollout fails when Model Release is not found`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodStartTime = Instant.now().plusSeconds(100L).toProtoTime()
      rolloutPeriodEndTime = Instant.now().plusSeconds(200L).toProtoTime()
      externalModelReleaseId = 123L
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.createModelRollout(modelRollout)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelRelease not found.")
  }

  @Test
  fun `scheduleModelRolloutFreeze succeeds`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodStartTime = Instant.now().plusSeconds(100L).toProtoTime()
      rolloutPeriodEndTime = Instant.now().plusSeconds(200L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    val createdModelRollout = modelRolloutsService.createModelRollout(modelRollout)
    val updatedModelRollout =
      modelRolloutsService.scheduleModelRolloutFreeze(
        scheduleModelRolloutFreezeRequest {
          externalModelProviderId = createdModelRollout.externalModelProviderId
          externalModelSuiteId = createdModelRollout.externalModelSuiteId
          externalModelLineId = createdModelRollout.externalModelLineId
          externalModelRolloutId = createdModelRollout.externalModelRolloutId
          rolloutFreezeTime = Instant.now().plusSeconds(150L).toProtoTime()
        }
      )
    assertThat(updatedModelRollout)
      .ignoringFields(ModelLine.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        modelRolloutsService
          .streamModelRollouts(
            streamModelRolloutsRequest {
              filter = filter {
                externalModelProviderId = updatedModelRollout.externalModelProviderId
                externalModelSuiteId = updatedModelRollout.externalModelSuiteId
                externalModelLineId = updatedModelRollout.externalModelLineId
              }
            }
          )
          .toList()
          .get(0)
      )
  }

  @Test
  fun `scheduleModelRolloutFreeze fails when freeze time is in the past`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodStartTime = Instant.now().plusSeconds(100L).toProtoTime()
      rolloutPeriodEndTime = Instant.now().plusSeconds(200L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    val createdModelRollout = modelRolloutsService.createModelRollout(modelRollout)
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.scheduleModelRolloutFreeze(
          scheduleModelRolloutFreezeRequest {
            externalModelProviderId = createdModelRollout.externalModelProviderId
            externalModelSuiteId = createdModelRollout.externalModelSuiteId
            externalModelLineId = createdModelRollout.externalModelLineId
            externalModelRolloutId = createdModelRollout.externalModelRolloutId
            rolloutFreezeTime = Instant.now().minusSeconds(100L).toProtoTime()
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("RolloutFreezeTime must be in the future.")
  }

  @Test
  fun `scheduleModelRolloutFreeze fails when freeze time precedes rollout start time`() =
    runBlocking {
      val modelLine =
        population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
      val modelRelease =
        population.createModelRelease(
          modelSuite {
            externalModelProviderId = modelLine.externalModelProviderId
            externalModelSuiteId = modelLine.externalModelSuiteId
          },
          modelReleasesService
        )

      val modelRollout = modelRollout {
        externalModelProviderId = modelLine.externalModelProviderId
        externalModelSuiteId = modelLine.externalModelSuiteId
        externalModelLineId = modelLine.externalModelLineId
        rolloutPeriodStartTime = Instant.now().plusSeconds(500L).toProtoTime()
        rolloutPeriodEndTime = Instant.now().plusSeconds(700L).toProtoTime()
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
      val createdModelRollout = modelRolloutsService.createModelRollout(modelRollout)
      val exception =
        assertFailsWith<StatusRuntimeException> {
          modelRolloutsService.scheduleModelRolloutFreeze(
            scheduleModelRolloutFreezeRequest {
              externalModelProviderId = createdModelRollout.externalModelProviderId
              externalModelSuiteId = createdModelRollout.externalModelSuiteId
              externalModelLineId = createdModelRollout.externalModelLineId
              externalModelRolloutId = createdModelRollout.externalModelRolloutId
              rolloutFreezeTime = Instant.now().plusSeconds(200L).toProtoTime()
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("RolloutFreezeTime cannot precede RolloutPeriodStartTime.")
    }

  @Test
  fun `scheduleModelRolloutFreeze fails when freeze time is later than rollout end time`() =
    runBlocking {
      val modelLine =
        population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
      val modelRelease =
        population.createModelRelease(
          modelSuite {
            externalModelProviderId = modelLine.externalModelProviderId
            externalModelSuiteId = modelLine.externalModelSuiteId
          },
          modelReleasesService
        )

      val modelRollout = modelRollout {
        externalModelProviderId = modelLine.externalModelProviderId
        externalModelSuiteId = modelLine.externalModelSuiteId
        externalModelLineId = modelLine.externalModelLineId
        rolloutPeriodStartTime = Instant.now().plusSeconds(100L).toProtoTime()
        rolloutPeriodEndTime = Instant.now().plusSeconds(200L).toProtoTime()
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
      val createdModelRollout = modelRolloutsService.createModelRollout(modelRollout)
      val exception =
        assertFailsWith<StatusRuntimeException> {
          modelRolloutsService.scheduleModelRolloutFreeze(
            scheduleModelRolloutFreezeRequest {
              externalModelProviderId = createdModelRollout.externalModelProviderId
              externalModelSuiteId = createdModelRollout.externalModelSuiteId
              externalModelLineId = createdModelRollout.externalModelLineId
              externalModelRolloutId = createdModelRollout.externalModelRolloutId
              rolloutFreezeTime = Instant.now().plusSeconds(400L).toProtoTime()
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("RolloutFreezeTime cannot be equal or later than RolloutPeriodEndTime.")
    }

  @Test
  fun `scheduleModelRolloutFreeze fails when Model Rollout is not found`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.scheduleModelRolloutFreeze(
          scheduleModelRolloutFreezeRequest {
            externalModelProviderId = 123L
            externalModelSuiteId = 123L
            externalModelLineId = 123L
            externalModelRolloutId = 123L
            rolloutFreezeTime = Instant.now().plusSeconds(400L).toProtoTime()
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelRollout not found")
  }

  @Test
  fun `deleteModelRollout succeeds`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodStartTime = Instant.now().plusSeconds(100L).toProtoTime()
      rolloutPeriodEndTime = Instant.now().plusSeconds(200L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    val createdModelRollout = modelRolloutsService.createModelRollout(modelRollout)
    modelRolloutsService.deleteModelRollout(
      deleteModelRolloutRequest {
        externalModelProviderId = createdModelRollout.externalModelProviderId
        externalModelSuiteId = createdModelRollout.externalModelSuiteId
        externalModelLineId = createdModelRollout.externalModelLineId
        externalModelRolloutId = createdModelRollout.externalModelRolloutId
      }
    )
    assertThat(
        modelRolloutsService
          .streamModelRollouts(
            streamModelRolloutsRequest {
              filter = filter {
                externalModelProviderId = createdModelRollout.externalModelProviderId
                externalModelSuiteId = createdModelRollout.externalModelSuiteId
                externalModelLineId = createdModelRollout.externalModelLineId
              }
            }
          )
          .toList()
          .size
      )
      .isEqualTo(0)
  }

  @Test
  fun `deleteModelRollout fails when external model provider id is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.deleteModelRollout(
          deleteModelRolloutRequest {
            externalModelSuiteId = 123L
            externalModelLineId = 123L
            externalModelRolloutId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalModelProviderId unspecified")
  }

  @Test
  fun `deleteModelRollout fails when external model suite id is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.deleteModelRollout(
          deleteModelRolloutRequest {
            externalModelProviderId = 123L
            externalModelLineId = 123L
            externalModelRolloutId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalModelSuiteId unspecified")
  }

  @Test
  fun `deleteModelRollout fails when external model line id is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.deleteModelRollout(
          deleteModelRolloutRequest {
            externalModelProviderId = 123L
            externalModelSuiteId = 123L
            externalModelRolloutId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalModelLineId unspecified")
  }

  @Test
  fun `deleteModelRollout fails when external model rollout id is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.deleteModelRollout(
          deleteModelRolloutRequest {
            externalModelProviderId = 123L
            externalModelSuiteId = 123L
            externalModelLineId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalModelRolloutId unspecified")
  }

  @Test
  fun `deleteModelRollout fails when Model Rollout is not found`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.deleteModelRollout(
          deleteModelRolloutRequest {
            externalModelProviderId = 123L
            externalModelSuiteId = 123L
            externalModelLineId = 123L
            externalModelRolloutId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelRollout not found")
  }

  @Test
  fun `streamModelRollouts returns all model rollouts`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodEndTime = Instant.now().plusSeconds(400L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    val modelRollout1 =
      modelRolloutsService.createModelRollout(
        modelRollout.copy { rolloutPeriodStartTime = Instant.now().plusSeconds(50L).toProtoTime() }
      )
    val modelRollout2 =
      modelRolloutsService.createModelRollout(
        modelRollout.copy { rolloutPeriodStartTime = Instant.now().plusSeconds(150L).toProtoTime() }
      )

    val modelRollouts: List<ModelRollout> =
      modelRolloutsService
        .streamModelRollouts(
          streamModelRolloutsRequest {
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
            }
          }
        )
        .toList()

    assertThat(modelRollouts)
      .comparingExpectedFieldsOnly()
      .containsExactly(modelRollout1, modelRollout2)
      .inOrder()
  }

  @Test
  fun `streamModelRollouts can get one page at a time`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodEndTime = Instant.now().plusSeconds(400L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    val modelRollout1 =
      modelRolloutsService.createModelRollout(
        modelRollout.copy { rolloutPeriodStartTime = Instant.now().plusSeconds(150L).toProtoTime() }
      )
    val modelRollout2 =
      modelRolloutsService.createModelRollout(
        modelRollout.copy { rolloutPeriodStartTime = Instant.now().plusSeconds(50L).toProtoTime() }
      )

    val modelRollouts: List<ModelRollout> =
      modelRolloutsService
        .streamModelRollouts(
          streamModelRolloutsRequest {
            limit = 1
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
            }
          }
        )
        .toList()

    assertThat(modelRollouts).hasSize(1)
    assertThat(modelRollouts).contains(modelRollout2)

    val modelRollouts2: List<ModelRollout> =
      modelRolloutsService
        .streamModelRollouts(
          streamModelRolloutsRequest {
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
              after = afterFilter {
                rolloutPeriodStartTime = modelRollouts[0].rolloutPeriodStartTime
                externalModelProviderId = modelRollouts[0].externalModelProviderId
                externalModelSuiteId = modelRollouts[0].externalModelSuiteId
                externalModelLineId = modelRollouts[0].externalModelLineId
                externalModelRolloutId = modelRollouts[0].externalModelRolloutId
              }
            }
          }
        )
        .toList()

    assertThat(modelRollouts2).hasSize(1)
    assertThat(modelRollouts2).contains(modelRollout1)
  }

  @Test
  fun `streamModelRollouts fails for missing after filter fields`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      rolloutPeriodEndTime = Instant.now().plusSeconds(400L).toProtoTime()
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    modelRolloutsService.createModelRollout(
      modelRollout.copy { rolloutPeriodStartTime = Instant.now().plusSeconds(50L).toProtoTime() }
    )
    modelRolloutsService.createModelRollout(
      modelRollout.copy { rolloutPeriodStartTime = Instant.now().plusSeconds(150L).toProtoTime() }
    )

    val modelRollouts: List<ModelRollout> =
      modelRolloutsService
        .streamModelRollouts(
          streamModelRolloutsRequest {
            limit = 1
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
            }
          }
        )
        .toList()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService
          .streamModelRollouts(
            streamModelRolloutsRequest {
              filter = filter {
                externalModelProviderId = modelLine.externalModelProviderId
                externalModelSuiteId = modelLine.externalModelSuiteId
                externalModelLineId = modelLine.externalModelLineId
                after = afterFilter {
                  rolloutPeriodStartTime = modelRollouts[0].rolloutPeriodStartTime
                  externalModelLineId = modelRollouts[0].externalModelLineId
                  externalModelRolloutId = modelRollouts[0].externalModelRolloutId
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
  fun `streamModelRollouts fails when limit is less than 0`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.streamModelRollouts(
          streamModelRolloutsRequest {
            limit = -1
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Limit cannot be less than 0")
  }

  @Test
  fun `streamModelRollouts filter by rollout period interval`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)
    val modelRelease =
      population.createModelRelease(
        modelSuite {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
        },
        modelReleasesService
      )

    val START_ROLLOUT_LIMIT_1 = Instant.now().plusSeconds(1000L).toProtoTime()
    val END_ROLLOUT_LIMIT_1 = Instant.now().plusSeconds(2000L).toProtoTime()
    val START_ROLLOUT_LIMIT_2 = Instant.now().plusSeconds(2000L).toProtoTime()
    val END_ROLLOUT_LIMIT_2 = Instant.now().plusSeconds(3000L).toProtoTime()
    val END_ROLLOUT_QUERY_LIMIT = Instant.now().plusSeconds(4000L).toProtoTime()

    val modelRollout = modelRollout {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }

    modelRolloutsService.createModelRollout(
      modelRollout.copy {
        rolloutPeriodStartTime = START_ROLLOUT_LIMIT_1
        rolloutPeriodEndTime = END_ROLLOUT_LIMIT_1
      }
    )
    modelRolloutsService.createModelRollout(
      modelRollout.copy {
        rolloutPeriodStartTime = START_ROLLOUT_LIMIT_1
        rolloutPeriodEndTime = END_ROLLOUT_LIMIT_1
      }
    )
    val modelRollout3 =
      modelRolloutsService.createModelRollout(
        modelRollout.copy {
          rolloutPeriodStartTime = START_ROLLOUT_LIMIT_2
          rolloutPeriodEndTime = END_ROLLOUT_LIMIT_2
        }
      )

    val modelRollouts: List<ModelRollout> =
      modelRolloutsService
        .streamModelRollouts(
          streamModelRolloutsRequest {
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
              rolloutPeriod = rolloutPeriod {
                rolloutPeriodStartTime = START_ROLLOUT_LIMIT_2
                rolloutPeriodEndTime = END_ROLLOUT_QUERY_LIMIT
              }
            }
          }
        )
        .toList()

    assertThat(modelRollouts).containsExactly(modelRollout3)
  }

  @Test
  fun `streamModelRollouts fails for missing rollout interval filter fields`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelRolloutsService.streamModelRollouts(
          streamModelRolloutsRequest {
            filter = filter {
              rolloutPeriod = rolloutPeriod {
                rolloutPeriodStartTime = Instant.now().minusSeconds(100L).toProtoTime()
              }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Missing RolloutPeriod fields")
  }
}
