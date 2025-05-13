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
import com.google.protobuf.copy
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Instant
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
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.enumerateValidModelLinesRequest
import org.wfanet.measurement.internal.kingdom.enumerateValidModelLinesResponse
import org.wfanet.measurement.internal.kingdom.modelLine
import org.wfanet.measurement.internal.kingdom.modelLineKey
import org.wfanet.measurement.internal.kingdom.modelRollout
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.internal.kingdom.setActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.setModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.streamModelLinesRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ModelLinesServiceTest<T : ModelLinesCoroutineImplBase> {

  protected data class Services<T>(
    val modelLinesService: T,
    val modelSuitesService: ModelSuitesCoroutineImplBase,
    val modelProvidersService: ModelProvidersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val modelRolloutsService: ModelRolloutsCoroutineImplBase,
    val modelReleasesService: ModelReleasesCoroutineImplBase,
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

  protected lateinit var modelRolloutsService: ModelRolloutsCoroutineImplBase
    private set

  protected lateinit var modelReleasesService: ModelReleasesCoroutineImplBase
    private set

  protected lateinit var populationsService: PopulationsCoroutineImplBase
    private set

  protected lateinit var modelLinesService: T
    private set

  protected abstract fun newServices(clock: Clock, idGenerator: IdGenerator): Services<T>

  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)

  @Before
  fun initServices() {
    val services = newServices(clock, idGenerator)
    modelLinesService = services.modelLinesService
    modelSuitesService = services.modelSuitesService
    modelProvidersService = services.modelProvidersService
    dataProvidersService = services.dataProvidersService
    modelRolloutsService = services.modelRolloutsService
    modelReleasesService = services.modelReleasesService
    populationsService = services.populationsService
  }

  @Test
  fun `createModelLine fails for missing activeStartTime`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelLinesService.createModelLine(modelLine) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ActiveStartTime is missing.")
  }

  @Test
  fun `createModelLine fails when activeStartTime is in the past`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = Instant.now().minusSeconds(100L).toProtoTime()
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelLinesService.createModelLine(modelLine) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ActiveStartTime must be in the future.")
  }

  @Test
  fun `createModelLine fails when activeEndTime is before activeStartTime`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
      activeEndTime = Instant.now().plusSeconds(1000L).toProtoTime()
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelLinesService.createModelLine(modelLine) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ActiveEndTime cannot precede ActiveStartTime.")
  }

  @Test
  fun `createModelLine fails when 'type' is 'TYPE_UNSPECIFIED'`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
      type = ModelLine.Type.TYPE_UNSPECIFIED
      displayName = "display name"
      description = "description"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelLinesService.createModelLine(modelLine) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Unrecognized ModelLine's type")
  }

  @Test
  fun `createModelLine fails if HoldbackModelLine is set but ModelLine type != 'PROD'`() =
    runBlocking {
      val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

      val ast = Instant.now().plusSeconds(2000L).toProtoTime()

      val holdbackModelLine = modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = ast
        type = ModelLine.Type.HOLDBACK
        displayName = "display name1"
        description = "description1"
      }

      val createdHoldbackModelLine = modelLinesService.createModelLine(holdbackModelLine)

      val devModelLine = modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = ast
        type = ModelLine.Type.DEV
        externalHoldbackModelLineId = createdHoldbackModelLine.externalModelLineId
        displayName = "display name2"
        description = "description2"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { modelLinesService.createModelLine(devModelLine) }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("Only ModelLine with type == PROD can have a Holdback ModelLine.")
    }

  @Test
  fun `createModelLine fails if HoldbackModelLine is set but has type != 'HOLDBACK'`() =
    runBlocking {
      val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

      val ast = Instant.now().plusSeconds(2000L).toProtoTime()

      val devModelLine = modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = ast
        type = ModelLine.Type.DEV
        displayName = "display name1"
        description = "description1"
      }

      val createdDevModelLine = modelLinesService.createModelLine(devModelLine)

      val prodModelLine = modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = ast
        type = ModelLine.Type.PROD
        externalHoldbackModelLineId = createdDevModelLine.externalModelLineId
        displayName = "display name2"
        description = "description2"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { modelLinesService.createModelLine(prodModelLine) }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("Only ModelLine with type == HOLDBACK can be set as Holdback ModelLine.")
    }

  @Test
  fun `createModelLine fails when Model Suite is not found`() = runBlocking {
    val ast = Instant.now().plusSeconds(2000L).toProtoTime()

    val modelLine = modelLine {
      externalModelSuiteId = 123L
      externalModelProviderId = 123L
      activeStartTime = ast
      type = ModelLine.Type.PROD
      displayName = "display name1"
      description = "description1"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelLinesService.createModelLine(modelLine) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelSuite not found")
  }

  @Test
  fun `createModelLine succeeds`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val ast = Instant.now().plusSeconds(2000L).toProtoTime()

    val holdbackModelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = ast
      type = ModelLine.Type.HOLDBACK
      displayName = "display name1"
      description = "description1"
    }

    val createdHoldbackModelLine = modelLinesService.createModelLine(holdbackModelLine)

    assertThat(createdHoldbackModelLine)
      .ignoringFields(
        ModelLine.CREATE_TIME_FIELD_NUMBER,
        ModelLine.UPDATE_TIME_FIELD_NUMBER,
        ModelLine.EXTERNAL_MODEL_LINE_ID_FIELD_NUMBER,
      )
      .isEqualTo(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = ast
          type = ModelLine.Type.HOLDBACK
          displayName = "display name1"
          description = "description1"
        }
      )

    val prodModelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = ast
      type = ModelLine.Type.PROD
      externalHoldbackModelLineId = createdHoldbackModelLine.externalModelLineId
      displayName = "display name2"
      description = "description2"
    }

    val createdProdModelLine = modelLinesService.createModelLine(prodModelLine)

    assertThat(createdProdModelLine)
      .ignoringFields(
        ModelLine.CREATE_TIME_FIELD_NUMBER,
        ModelLine.UPDATE_TIME_FIELD_NUMBER,
        ModelLine.EXTERNAL_MODEL_LINE_ID_FIELD_NUMBER,
      )
      .isEqualTo(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = ast
          type = ModelLine.Type.PROD
          externalHoldbackModelLineId = createdHoldbackModelLine.externalModelLineId
          displayName = "display name2"
          description = "description2"
        }
      )
  }

  @Test
  fun `setActiveEndTime fails if ModelLine is not found`() = runBlocking {
    val setActiveEndTimeRequest = setActiveEndTimeRequest {
      externalModelLineId = 123L
      externalModelSuiteId = 456L
      externalModelProviderId = 789L
      activeEndTime = Instant.now().plusSeconds(2000L).toProtoTime()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelLinesService.setActiveEndTime(setActiveEndTimeRequest)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelLine not found.")
  }

  @Test
  fun `setActiveEndTime fails if ActiveEndTime is before ActiveStartTime`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val createdModelLine = modelLinesService.createModelLine(modelLine)

    val setActiveEndTimeRequest = setActiveEndTimeRequest {
      externalModelLineId = createdModelLine.externalModelLineId
      externalModelSuiteId = createdModelLine.externalModelSuiteId
      externalModelProviderId = createdModelLine.externalModelProviderId
      activeEndTime = Instant.now().plusSeconds(500L).toProtoTime()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelLinesService.setActiveEndTime(setActiveEndTimeRequest)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("ActiveEndTime must be later than ActiveStartTime.")
  }

  @Test
  fun `setActiveEndTime fails if ActiveEndTime is in the past`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val createdModelLine = modelLinesService.createModelLine(modelLine)

    val setActiveEndTimeRequest = setActiveEndTimeRequest {
      externalModelLineId = createdModelLine.externalModelLineId
      externalModelSuiteId = createdModelLine.externalModelSuiteId
      externalModelProviderId = createdModelLine.externalModelProviderId
      activeEndTime = Instant.now().minusSeconds(500L).toProtoTime()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelLinesService.setActiveEndTime(setActiveEndTimeRequest)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ActiveEndTime must be in the future.")
  }

  @Test
  fun `setActiveEndTime succeeds`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val ast = Instant.now().plusSeconds(2000L).toProtoTime()
    val aet = Instant.now().plusSeconds(3000L).toProtoTime()

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = ast
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val createdModelLine = modelLinesService.createModelLine(modelLine)

    val setActiveEndTimeRequest = setActiveEndTimeRequest {
      externalModelLineId = createdModelLine.externalModelLineId
      externalModelSuiteId = createdModelLine.externalModelSuiteId
      externalModelProviderId = createdModelLine.externalModelProviderId
      activeEndTime = aet
    }

    val updatedModelLine = modelLinesService.setActiveEndTime(setActiveEndTimeRequest)

    assertThat(updatedModelLine)
      .ignoringFields(ModelLine.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        modelLinesService
          .streamModelLines(
            streamModelLinesRequest {
              filter = filter {
                externalModelProviderId = createdModelLine.externalModelProviderId
                externalModelSuiteId = createdModelLine.externalModelSuiteId
              }
            }
          )
          .toList()
          .get(0)
      )
  }

  @Test
  fun `streamModelLines returns all model lines`(): Unit = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine1 =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          type = ModelLine.Type.PROD
          displayName = "display name1"
          description = "description1"
        }
      )

    val modelLine2 =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          type = ModelLine.Type.PROD
          displayName = "display name2"
          description = "description2"
        }
      )

    val modelLines: List<ModelLine> =
      modelLinesService
        .streamModelLines(
          streamModelLinesRequest {
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
            }
          }
        )
        .toList()

    assertThat(modelLines)
      .comparingExpectedFieldsOnly()
      .containsExactly(modelLine1, modelLine2)
      .inOrder()
  }

  @Test
  fun `streamModelLines can get one page at a time`(): Unit = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine1 =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          type = ModelLine.Type.PROD
          displayName = "display name1"
          description = "description1"
        }
      )

    val modelLine2 =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          type = ModelLine.Type.PROD
          displayName = "display name2"
          description = "description2"
        }
      )

    val modelLines: List<ModelLine> =
      modelLinesService
        .streamModelLines(
          streamModelLinesRequest {
            limit = 1
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
            }
          }
        )
        .toList()

    assertThat(modelLines).hasSize(1)
    assertThat(modelLines).contains(modelLine1)

    val modelLines2: List<ModelLine> =
      modelLinesService
        .streamModelLines(
          streamModelLinesRequest {
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
              after = afterFilter {
                createTime = modelLines[0].createTime
                externalModelLineId = modelLines[0].externalModelLineId
                externalModelSuiteId = modelLines[0].externalModelSuiteId
                externalModelProviderId = modelLines[0].externalModelProviderId
              }
            }
          }
        )
        .toList()

    assertThat(modelLines2).hasSize(1)
    assertThat(modelLines2).contains(modelLine2)
  }

  @Test
  fun `streamModelLines can return model lines with a given type`(): Unit = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine1 =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          type = ModelLine.Type.PROD
          displayName = "display name1"
          description = "description1"
        }
      )

    modelLinesService.createModelLine(
      modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
        type = ModelLine.Type.DEV
        displayName = "display name2"
        description = "description2"
      }
    )

    val modelLines: List<ModelLine> =
      modelLinesService
        .streamModelLines(
          streamModelLinesRequest {
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
              type += ModelLine.Type.PROD
            }
          }
        )
        .toList()

    assertThat(modelLines).hasSize(1)
    assertThat(modelLines).contains(modelLine1)
  }

  @Test
  fun `streamModelLines fails for missing after filter fields`(): Unit = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    modelLinesService.createModelLine(
      modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
        type = ModelLine.Type.PROD
        displayName = "display name1"
        description = "description1"
      }
    )

    modelLinesService.createModelLine(
      modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
        type = ModelLine.Type.PROD
        displayName = "display name2"
        description = "description2"
      }
    )

    val modelLines: List<ModelLine> =
      modelLinesService
        .streamModelLines(
          streamModelLinesRequest {
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
            }
          }
        )
        .toList()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelLinesService.streamModelLines(
          streamModelLinesRequest {
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
              after = afterFilter {
                createTime = modelLines[0].createTime
                externalModelSuiteId = modelLines[0].externalModelSuiteId
                externalModelProviderId = modelLines[0].externalModelProviderId
              }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Missing After filter fields")
  }

  @Test
  fun `setModelLineHoldbackModelLine fails if ModelLine has type != 'PROD'`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine1 =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          type = ModelLine.Type.DEV
          displayName = "display name1"
          description = "description1"
        }
      )

    val modelLine2 =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          type = ModelLine.Type.PROD
          displayName = "display name2"
          description = "description2"
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelLinesService.setModelLineHoldbackModelLine(
          setModelLineHoldbackModelLineRequest {
            externalModelLineId = modelLine1.externalModelLineId
            externalModelSuiteId = modelSuite.externalModelSuiteId
            externalModelProviderId = modelSuite.externalModelProviderId
            externalHoldbackModelLineId = modelLine2.externalModelLineId
            externalHoldbackModelSuiteId = modelSuite.externalModelSuiteId
            externalHoldbackModelProviderId = modelSuite.externalModelProviderId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("Only ModelLine with type == PROD can have a Holdback ModelLine.")
  }

  @Test
  fun `setModelLineHoldbackModelLine fails if HoldbackModelLine has type != 'HOLDBACK'`() =
    runBlocking {
      val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

      val modelLine1 =
        modelLinesService.createModelLine(
          modelLine {
            externalModelSuiteId = modelSuite.externalModelSuiteId
            externalModelProviderId = modelSuite.externalModelProviderId
            activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
            type = ModelLine.Type.PROD
            displayName = "display name1"
            description = "description1"
          }
        )

      val modelLine2 =
        modelLinesService.createModelLine(
          modelLine {
            externalModelSuiteId = modelSuite.externalModelSuiteId
            externalModelProviderId = modelSuite.externalModelProviderId
            activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
            type = ModelLine.Type.DEV
            displayName = "display name2"
            description = "description2"
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          modelLinesService.setModelLineHoldbackModelLine(
            setModelLineHoldbackModelLineRequest {
              externalModelLineId = modelLine1.externalModelLineId
              externalModelSuiteId = modelSuite.externalModelSuiteId
              externalModelProviderId = modelSuite.externalModelProviderId
              externalHoldbackModelLineId = modelLine2.externalModelLineId
              externalHoldbackModelSuiteId = modelSuite.externalModelSuiteId
              externalHoldbackModelProviderId = modelSuite.externalModelProviderId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("Only ModelLine with type == HOLDBACK can be set as Holdback ModelLine.")
    }

  @Test
  fun `setModelLineHoldbackModelLine fails if HoldbackModelLine and ModelLine are not part of the same ModelSuite`() =
    runBlocking {
      val modelSuite1 = population.createModelSuite(modelProvidersService, modelSuitesService)
      val modelLine1 =
        modelLinesService.createModelLine(
          modelLine {
            externalModelSuiteId = modelSuite1.externalModelSuiteId
            externalModelProviderId = modelSuite1.externalModelProviderId
            activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
            type = ModelLine.Type.PROD
            displayName = "display name1"
            description = "description1"
          }
        )

      val modelSuite2 = population.createModelSuite(modelProvidersService, modelSuitesService)
      val modelLine2 =
        modelLinesService.createModelLine(
          modelLine {
            externalModelSuiteId = modelSuite2.externalModelSuiteId
            externalModelProviderId = modelSuite2.externalModelProviderId
            activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
            type = ModelLine.Type.HOLDBACK
            displayName = "display name2"
            description = "description2"
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          modelLinesService.setModelLineHoldbackModelLine(
            setModelLineHoldbackModelLineRequest {
              externalModelLineId = modelLine1.externalModelLineId
              externalModelSuiteId = modelSuite1.externalModelSuiteId
              externalModelProviderId = modelSuite1.externalModelProviderId
              externalHoldbackModelLineId = modelLine2.externalModelLineId
              externalHoldbackModelSuiteId = modelSuite2.externalModelSuiteId
              externalHoldbackModelProviderId = modelSuite2.externalModelProviderId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("HoldbackModelLine and ModelLine must be part of the same ModelSuite.")
    }

  @Test
  fun `setModelLineHoldbackModelLine succeeds`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine1 =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          type = ModelLine.Type.PROD
          displayName = "display name1"
          description = "description1"
        }
      )

    val modelLine2 =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          type = ModelLine.Type.HOLDBACK
          displayName = "display name2"
          description = "description2"
        }
      )

    modelLinesService.setModelLineHoldbackModelLine(
      setModelLineHoldbackModelLineRequest {
        externalModelLineId = modelLine1.externalModelLineId
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        externalHoldbackModelLineId = modelLine2.externalModelLineId
        externalHoldbackModelSuiteId = modelSuite.externalModelSuiteId
        externalHoldbackModelProviderId = modelSuite.externalModelProviderId
      }
    )

    val modelLines: List<ModelLine> =
      modelLinesService
        .streamModelLines(
          streamModelLinesRequest {
            filter = filter {
              externalModelProviderId = modelSuite.externalModelProviderId
              externalModelSuiteId = modelSuite.externalModelSuiteId
              type += ModelLine.Type.PROD
            }
          }
        )
        .toList()

    assertThat(modelLines.get(0).externalHoldbackModelLineId)
      .isEqualTo(modelLine2.externalModelLineId)
  }

  @Test
  fun `enumerateValidModelLines returns model lines in order`(): Unit = runBlocking {
    val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val dataProvider = population.createDataProvider(dataProvidersService)
    val dataProvider2 = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(dataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(modelSuite, createdPopulation, modelReleasesService)

    val oldProdModelLine =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = futureTime
          type = ModelLine.Type.PROD
          displayName = "display name"
          description = "description"
        }
      )

    modelRolloutsService.createModelRollout(
      modelRollout {
        externalModelProviderId = oldProdModelLine.externalModelProviderId
        externalModelSuiteId = oldProdModelLine.externalModelSuiteId
        externalModelLineId = oldProdModelLine.externalModelLineId
        rolloutPeriodStartTime = futureTime
        rolloutPeriodEndTime = futureTime
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
    )

    val newProdModelLine =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = futureTime.copy { seconds += 5 }
          type = ModelLine.Type.PROD
          displayName = "display name"
          description = "description"
        }
      )

    modelRolloutsService.createModelRollout(
      modelRollout {
        externalModelProviderId = newProdModelLine.externalModelProviderId
        externalModelSuiteId = newProdModelLine.externalModelSuiteId
        externalModelLineId = newProdModelLine.externalModelLineId
        rolloutPeriodStartTime = futureTime.copy { seconds += 5 }
        rolloutPeriodEndTime = futureTime.copy { seconds += 5 }
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
    )

    val holdBackModelLine =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = futureTime
          type = ModelLine.Type.HOLDBACK
          displayName = "display name"
          description = "description"
        }
      )

    modelRolloutsService.createModelRollout(
      modelRollout {
        externalModelProviderId = holdBackModelLine.externalModelProviderId
        externalModelSuiteId = holdBackModelLine.externalModelSuiteId
        externalModelLineId = holdBackModelLine.externalModelLineId
        rolloutPeriodStartTime = futureTime
        rolloutPeriodEndTime = futureTime
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
    )

    val devModelLine =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          activeStartTime = futureTime
          type = ModelLine.Type.DEV
          displayName = "display name"
          description = "description"
        }
      )

    modelRolloutsService.createModelRollout(
      modelRollout {
        externalModelProviderId = devModelLine.externalModelProviderId
        externalModelSuiteId = devModelLine.externalModelSuiteId
        externalModelLineId = devModelLine.externalModelLineId
        rolloutPeriodStartTime = futureTime
        rolloutPeriodEndTime = futureTime
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
    )

    dataProvidersService.replaceDataAvailabilityIntervals(
      replaceDataAvailabilityIntervalsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = oldProdModelLine.externalModelProviderId
              externalModelSuiteId = oldProdModelLine.externalModelSuiteId
              externalModelLineId = oldProdModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = newProdModelLine.externalModelProviderId
              externalModelSuiteId = newProdModelLine.externalModelSuiteId
              externalModelLineId = newProdModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = holdBackModelLine.externalModelProviderId
              externalModelSuiteId = holdBackModelLine.externalModelSuiteId
              externalModelLineId = holdBackModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = devModelLine.externalModelProviderId
              externalModelSuiteId = devModelLine.externalModelSuiteId
              externalModelLineId = devModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
      }
    )

    dataProvidersService.replaceDataAvailabilityIntervals(
      replaceDataAvailabilityIntervalsRequest {
        externalDataProviderId = dataProvider2.externalDataProviderId
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = oldProdModelLine.externalModelProviderId
              externalModelSuiteId = oldProdModelLine.externalModelSuiteId
              externalModelLineId = oldProdModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = newProdModelLine.externalModelProviderId
              externalModelSuiteId = newProdModelLine.externalModelSuiteId
              externalModelLineId = newProdModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = holdBackModelLine.externalModelProviderId
              externalModelSuiteId = holdBackModelLine.externalModelSuiteId
              externalModelLineId = holdBackModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = devModelLine.externalModelProviderId
              externalModelSuiteId = devModelLine.externalModelSuiteId
              externalModelLineId = devModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
      }
    )

    val enumerateValidModelLinesResponse =
      modelLinesService.enumerateValidModelLines(
        enumerateValidModelLinesRequest {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          timeInterval = interval {
            startTime = futureTime.copy { seconds += 10 }
            endTime = futureTime.copy { seconds += 15 }
          }
          externalDataProviderIds += dataProvider.externalDataProviderId
          externalDataProviderIds += dataProvider2.externalDataProviderId
          types += ModelLine.Type.PROD
          types += ModelLine.Type.HOLDBACK
          types += ModelLine.Type.DEV
        }
      )

    assertThat(enumerateValidModelLinesResponse)
      .isEqualTo(
        enumerateValidModelLinesResponse {
          modelLines += newProdModelLine
          modelLines += oldProdModelLine
          modelLines += holdBackModelLine
          modelLines += devModelLine
        }
      )
  }

  @Test
  fun `enumerateValidModelLines returns model lines when activeEndTime included`(): Unit =
    runBlocking {
      val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
      val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val createdPopulation = population.createPopulation(dataProvider, populationsService)
      val modelRelease =
        population.createModelRelease(modelSuite, createdPopulation, modelReleasesService)

      val modelLine =
        modelLinesService.createModelLine(
          modelLine {
            externalModelSuiteId = modelSuite.externalModelSuiteId
            externalModelProviderId = modelSuite.externalModelProviderId
            activeStartTime = futureTime
            activeEndTime = futureTime.copy { seconds += 50 }
            type = ModelLine.Type.PROD
            displayName = "display name"
            description = "description"
          }
        )

      modelRolloutsService.createModelRollout(
        modelRollout {
          externalModelProviderId = modelLine.externalModelProviderId
          externalModelSuiteId = modelLine.externalModelSuiteId
          externalModelLineId = modelLine.externalModelLineId
          rolloutPeriodStartTime = futureTime
          rolloutPeriodEndTime = futureTime
          externalModelReleaseId = modelRelease.externalModelReleaseId
        }
      )

      dataProvidersService.replaceDataAvailabilityIntervals(
        replaceDataAvailabilityIntervalsRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          dataAvailabilityIntervals +=
            DataProviderKt.dataAvailabilityMapEntry {
              key = modelLineKey {
                externalModelProviderId = modelLine.externalModelProviderId
                externalModelSuiteId = modelLine.externalModelSuiteId
                externalModelLineId = modelLine.externalModelLineId
              }
              value = interval {
                startTime = futureTime
                endTime = futureTime.copy { seconds += 20 }
              }
            }
        }
      )

      val enumerateValidModelLinesResponse =
        modelLinesService.enumerateValidModelLines(
          enumerateValidModelLinesRequest {
            externalModelProviderId = modelSuite.externalModelProviderId
            externalModelSuiteId = modelSuite.externalModelSuiteId
            timeInterval = interval {
              startTime = futureTime.copy { seconds += 10 }
              endTime = futureTime.copy { seconds += 15 }
            }
            externalDataProviderIds += dataProvider.externalDataProviderId
          }
        )

      assertThat(enumerateValidModelLinesResponse)
        .isEqualTo(enumerateValidModelLinesResponse { modelLines += modelLine })
    }

  @Test
  fun `enumerateValidModelLines does not return model line if type not included`(): Unit =
    runBlocking {
      val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
      val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val createdPopulation = population.createPopulation(dataProvider, populationsService)
      val modelRelease =
        population.createModelRelease(modelSuite, createdPopulation, modelReleasesService)

      val modelLine = modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = futureTime
        type = ModelLine.Type.HOLDBACK
        displayName = "display name"
        description = "description"
      }

      val createdModelLine = modelLinesService.createModelLine(modelLine)

      val modelRollout = modelRollout {
        externalModelProviderId = createdModelLine.externalModelProviderId
        externalModelSuiteId = createdModelLine.externalModelSuiteId
        externalModelLineId = createdModelLine.externalModelLineId
        rolloutPeriodStartTime = futureTime
        rolloutPeriodEndTime = futureTime
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
      modelRolloutsService.createModelRollout(modelRollout)

      dataProvidersService.replaceDataAvailabilityIntervals(
        replaceDataAvailabilityIntervalsRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          dataAvailabilityIntervals +=
            DataProviderKt.dataAvailabilityMapEntry {
              key = modelLineKey {
                externalModelProviderId = createdModelLine.externalModelProviderId
                externalModelSuiteId = createdModelLine.externalModelSuiteId
                externalModelLineId = createdModelLine.externalModelLineId
              }
              value = interval {
                startTime = futureTime
                endTime = futureTime.copy { seconds += 20 }
              }
            }
        }
      )

      val enumerateValidModelLinesResponse =
        modelLinesService.enumerateValidModelLines(
          enumerateValidModelLinesRequest {
            externalModelProviderId = modelSuite.externalModelProviderId
            externalModelSuiteId = modelSuite.externalModelSuiteId
            timeInterval = interval {
              startTime = futureTime.copy { seconds += 10 }
              endTime = futureTime.copy { seconds += 15 }
            }
            externalDataProviderIds += dataProvider.externalDataProviderId
          }
        )

      assertThat(enumerateValidModelLinesResponse).isEqualTo(enumerateValidModelLinesResponse {})
    }

  @Test
  fun `enumerateValidModelLines does not return model line if 2 rollouts`(): Unit = runBlocking {
    val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val dataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(dataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(modelSuite, createdPopulation, modelReleasesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = futureTime
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val createdModelLine = modelLinesService.createModelLine(modelLine)

    val modelRollout = modelRollout {
      externalModelProviderId = createdModelLine.externalModelProviderId
      externalModelSuiteId = createdModelLine.externalModelSuiteId
      externalModelLineId = createdModelLine.externalModelLineId
      rolloutPeriodStartTime = futureTime
      rolloutPeriodEndTime = futureTime
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    modelRolloutsService.createModelRollout(modelRollout)
    modelRolloutsService.createModelRollout(modelRollout)

    dataProvidersService.replaceDataAvailabilityIntervals(
      replaceDataAvailabilityIntervalsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = createdModelLine.externalModelProviderId
              externalModelSuiteId = createdModelLine.externalModelSuiteId
              externalModelLineId = createdModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
      }
    )

    val enumerateValidModelLinesResponse =
      modelLinesService.enumerateValidModelLines(
        enumerateValidModelLinesRequest {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          timeInterval = interval {
            startTime = futureTime.copy { seconds += 10 }
            endTime = futureTime.copy { seconds += 15 }
          }
          externalDataProviderIds += dataProvider.externalDataProviderId
        }
      )

    assertThat(enumerateValidModelLinesResponse).isEqualTo(enumerateValidModelLinesResponse {})
  }

  @Test
  fun `enumerateValidModelLines does not return model line if 0 rollouts`(): Unit = runBlocking {
    val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = futureTime
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val createdModelLine = modelLinesService.createModelLine(modelLine)

    dataProvidersService.replaceDataAvailabilityIntervals(
      replaceDataAvailabilityIntervalsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = createdModelLine.externalModelProviderId
              externalModelSuiteId = createdModelLine.externalModelSuiteId
              externalModelLineId = createdModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
      }
    )

    val enumerateValidModelLinesResponse =
      modelLinesService.enumerateValidModelLines(
        enumerateValidModelLinesRequest {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          timeInterval = interval {
            startTime = futureTime.copy { seconds += 10 }
            endTime = futureTime.copy { seconds += 15 }
          }
          externalDataProviderIds += dataProvider.externalDataProviderId
        }
      )

    assertThat(enumerateValidModelLinesResponse).isEqualTo(enumerateValidModelLinesResponse {})
  }

  @Test
  fun `enumerateValidModelLines does not return model line if time interval not in active interval`():
    Unit = runBlocking {
    val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val dataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(dataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(modelSuite, createdPopulation, modelReleasesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = futureTime
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val createdModelLine = modelLinesService.createModelLine(modelLine)

    val modelRollout = modelRollout {
      externalModelProviderId = createdModelLine.externalModelProviderId
      externalModelSuiteId = createdModelLine.externalModelSuiteId
      externalModelLineId = createdModelLine.externalModelLineId
      rolloutPeriodStartTime = futureTime
      rolloutPeriodEndTime = futureTime
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    modelRolloutsService.createModelRollout(modelRollout)

    dataProvidersService.replaceDataAvailabilityIntervals(
      replaceDataAvailabilityIntervalsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = createdModelLine.externalModelProviderId
              externalModelSuiteId = createdModelLine.externalModelSuiteId
              externalModelLineId = createdModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
      }
    )

    val enumerateValidModelLinesResponse =
      modelLinesService.enumerateValidModelLines(
        enumerateValidModelLinesRequest {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          timeInterval = interval {
            startTime = futureTime.copy { seconds -= 10 }
            endTime = futureTime.copy { seconds += 15 }
          }
          externalDataProviderIds += dataProvider.externalDataProviderId
        }
      )

    assertThat(enumerateValidModelLinesResponse).isEqualTo(enumerateValidModelLinesResponse {})
  }

  @Test
  fun `enumerateValidModelLines returns no model lines if time interval not in edp interval`():
    Unit = runBlocking {
    val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val dataProvider = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(dataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(modelSuite, createdPopulation, modelReleasesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = futureTime
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val createdModelLine = modelLinesService.createModelLine(modelLine)

    val modelRollout = modelRollout {
      externalModelProviderId = createdModelLine.externalModelProviderId
      externalModelSuiteId = createdModelLine.externalModelSuiteId
      externalModelLineId = createdModelLine.externalModelLineId
      rolloutPeriodStartTime = futureTime
      rolloutPeriodEndTime = futureTime
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    modelRolloutsService.createModelRollout(modelRollout)

    dataProvidersService.replaceDataAvailabilityIntervals(
      replaceDataAvailabilityIntervalsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = createdModelLine.externalModelProviderId
              externalModelSuiteId = createdModelLine.externalModelSuiteId
              externalModelLineId = createdModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
      }
    )

    val enumerateValidModelLinesResponse =
      modelLinesService.enumerateValidModelLines(
        enumerateValidModelLinesRequest {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          timeInterval = interval {
            startTime = futureTime.copy { seconds += 10 }
            endTime = futureTime.copy { seconds += 30 }
          }
          externalDataProviderIds += dataProvider.externalDataProviderId
        }
      )

    assertThat(enumerateValidModelLinesResponse).isEqualTo(enumerateValidModelLinesResponse {})
  }

  @Test
  fun `enumerateValidModelLines returns no model lines if no edp interval for model line`(): Unit =
    runBlocking {
      val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
      val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val createdPopulation = population.createPopulation(dataProvider, populationsService)
      val modelRelease =
        population.createModelRelease(modelSuite, createdPopulation, modelReleasesService)

      val modelLine = modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = futureTime
        type = ModelLine.Type.PROD
        displayName = "display name"
        description = "description"
      }

      val createdModelLine = modelLinesService.createModelLine(modelLine)

      val modelRollout = modelRollout {
        externalModelProviderId = createdModelLine.externalModelProviderId
        externalModelSuiteId = createdModelLine.externalModelSuiteId
        externalModelLineId = createdModelLine.externalModelLineId
        rolloutPeriodStartTime = futureTime
        rolloutPeriodEndTime = futureTime
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
      modelRolloutsService.createModelRollout(modelRollout)

      val enumerateValidModelLinesResponse =
        modelLinesService.enumerateValidModelLines(
          enumerateValidModelLinesRequest {
            externalModelProviderId = modelSuite.externalModelProviderId
            externalModelSuiteId = modelSuite.externalModelSuiteId
            timeInterval = interval {
              startTime = futureTime.copy { seconds += 10 }
              endTime = futureTime.copy { seconds += 15 }
            }
            externalDataProviderIds += dataProvider.externalDataProviderId
          }
        )

      assertThat(enumerateValidModelLinesResponse).isEqualTo(enumerateValidModelLinesResponse {})
    }

  @Test
  fun `enumerateValidModelLines returns no lines if no interval for model line for every edp`():
    Unit = runBlocking {
    val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
    val dataProvider = population.createDataProvider(dataProvidersService)
    val dataProvider2 = population.createDataProvider(dataProvidersService)
    val createdPopulation = population.createPopulation(dataProvider, populationsService)
    val modelRelease =
      population.createModelRelease(modelSuite, createdPopulation, modelReleasesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = futureTime
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val createdModelLine = modelLinesService.createModelLine(modelLine)

    val modelRollout = modelRollout {
      externalModelProviderId = createdModelLine.externalModelProviderId
      externalModelSuiteId = createdModelLine.externalModelSuiteId
      externalModelLineId = createdModelLine.externalModelLineId
      rolloutPeriodStartTime = futureTime
      rolloutPeriodEndTime = futureTime
      externalModelReleaseId = modelRelease.externalModelReleaseId
    }
    modelRolloutsService.createModelRollout(modelRollout)

    dataProvidersService.replaceDataAvailabilityIntervals(
      replaceDataAvailabilityIntervalsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = createdModelLine.externalModelProviderId
              externalModelSuiteId = createdModelLine.externalModelSuiteId
              externalModelLineId = createdModelLine.externalModelLineId
            }
            value = interval {
              startTime = futureTime
              endTime = futureTime.copy { seconds += 20 }
            }
          }
      }
    )

    val enumerateValidModelLinesResponse =
      modelLinesService.enumerateValidModelLines(
        enumerateValidModelLinesRequest {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          timeInterval = interval {
            startTime = futureTime.copy { seconds += 10 }
            endTime = futureTime.copy { seconds += 15 }
          }
          externalDataProviderIds += dataProvider.externalDataProviderId
          externalDataProviderIds += dataProvider2.externalDataProviderId
        }
      )

    assertThat(enumerateValidModelLinesResponse).isEqualTo(enumerateValidModelLinesResponse {})
  }

  @Test
  fun `enumerateValidModelLines returns no lines if interval not in every edp interval`(): Unit =
    runBlocking {
      val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
      val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val dataProvider2 = population.createDataProvider(dataProvidersService)
      val createdPopulation = population.createPopulation(dataProvider, populationsService)
      val modelRelease =
        population.createModelRelease(modelSuite, createdPopulation, modelReleasesService)

      val modelLine = modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = futureTime
        type = ModelLine.Type.PROD
        displayName = "display name"
        description = "description"
      }

      val createdModelLine = modelLinesService.createModelLine(modelLine)

      val modelRollout = modelRollout {
        externalModelProviderId = createdModelLine.externalModelProviderId
        externalModelSuiteId = createdModelLine.externalModelSuiteId
        externalModelLineId = createdModelLine.externalModelLineId
        rolloutPeriodStartTime = futureTime
        rolloutPeriodEndTime = futureTime
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
      modelRolloutsService.createModelRollout(modelRollout)

      dataProvidersService.replaceDataAvailabilityIntervals(
        replaceDataAvailabilityIntervalsRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          dataAvailabilityIntervals +=
            DataProviderKt.dataAvailabilityMapEntry {
              key = modelLineKey {
                externalModelProviderId = createdModelLine.externalModelProviderId
                externalModelSuiteId = createdModelLine.externalModelSuiteId
                externalModelLineId = createdModelLine.externalModelLineId
              }
              value = interval {
                startTime = futureTime
                endTime = futureTime.copy { seconds += 20 }
              }
            }
        }
      )

      dataProvidersService.replaceDataAvailabilityIntervals(
        replaceDataAvailabilityIntervalsRequest {
          externalDataProviderId = dataProvider2.externalDataProviderId
          dataAvailabilityIntervals +=
            DataProviderKt.dataAvailabilityMapEntry {
              key = modelLineKey {
                externalModelProviderId = createdModelLine.externalModelProviderId
                externalModelSuiteId = createdModelLine.externalModelSuiteId
                externalModelLineId = createdModelLine.externalModelLineId
              }
              value = interval {
                startTime = futureTime
                endTime = futureTime.copy { seconds += 5 }
              }
            }
        }
      )

      val enumerateValidModelLinesResponse =
        modelLinesService.enumerateValidModelLines(
          enumerateValidModelLinesRequest {
            externalModelProviderId = modelSuite.externalModelProviderId
            externalModelSuiteId = modelSuite.externalModelSuiteId
            timeInterval = interval {
              startTime = futureTime.copy { seconds += 10 }
              endTime = futureTime.copy { seconds += 15 }
            }
            externalDataProviderIds += dataProvider.externalDataProviderId
            externalDataProviderIds += dataProvider2.externalDataProviderId
          }
        )

      assertThat(enumerateValidModelLinesResponse).isEqualTo(enumerateValidModelLinesResponse {})
    }

  @Test
  fun `enumerateValidModelLines returns no model lines when data provider not found`(): Unit =
    runBlocking {
      val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
      val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val createdPopulation = population.createPopulation(dataProvider, populationsService)
      val modelRelease =
        population.createModelRelease(modelSuite, createdPopulation, modelReleasesService)

      val modelLine = modelLine {
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalModelProviderId = modelSuite.externalModelProviderId
        activeStartTime = futureTime
        type = ModelLine.Type.PROD
        displayName = "display name"
        description = "description"
      }

      val createdModelLine = modelLinesService.createModelLine(modelLine)

      val modelRollout = modelRollout {
        externalModelProviderId = createdModelLine.externalModelProviderId
        externalModelSuiteId = createdModelLine.externalModelSuiteId
        externalModelLineId = createdModelLine.externalModelLineId
        rolloutPeriodStartTime = futureTime
        rolloutPeriodEndTime = futureTime
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
      modelRolloutsService.createModelRollout(modelRollout)

      val enumerateValidModelLinesResponse =
        modelLinesService.enumerateValidModelLines(
          enumerateValidModelLinesRequest {
            externalModelProviderId = modelSuite.externalModelProviderId
            externalModelSuiteId = modelSuite.externalModelSuiteId
            timeInterval = interval {
              startTime = futureTime.copy { seconds += 10 }
              endTime = futureTime.copy { seconds += 15 }
            }
            externalDataProviderIds += dataProvider.externalDataProviderId + 5L
          }
        )

      assertThat(enumerateValidModelLinesResponse).isEqualTo(enumerateValidModelLinesResponse {})
    }
}
