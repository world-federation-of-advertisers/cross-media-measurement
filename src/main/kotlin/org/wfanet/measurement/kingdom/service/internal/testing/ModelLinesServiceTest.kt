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
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.modelLine
import org.wfanet.measurement.internal.kingdom.setActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.setModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.streamModelLinesRequest

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ModelLinesServiceTest<T : ModelLinesCoroutineImplBase> {

  protected data class Services<T>(
    val modelLinesService: T,
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

  protected lateinit var modelLinesService: T
    private set

  protected abstract fun newServices(clock: Clock, idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(clock, idGenerator)
    modelLinesService = services.modelLinesService
    modelSuitesService = services.modelSuitesService
    modelProvidersService = services.modelProvidersService
  }

  @Test
  fun `createModelLine fails for missing activeStartTime`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
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
        ModelLine.EXTERNAL_MODEL_LINE_ID_FIELD_NUMBER
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
        ModelLine.EXTERNAL_MODEL_LINE_ID_FIELD_NUMBER
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
              types += ModelLine.Type.PROD
            }
          }
        )
        .toList()

    assertThat(modelLines).hasSize(1)
    assertThat(modelLines).contains(modelLine1)
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
      .contains(
        "Only ModelLines with type equal to 'PROD' can have a HoldbackModelLine having type equal to 'HOLDBACK'."
      )
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
        .contains(
          "Only ModelLines with type equal to 'PROD' can have a HoldbackModelLine having type equal to 'HOLDBACK'."
        )
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
              types += ModelLine.Type.PROD
            }
          }
        )
        .toList()

    assertThat(modelLines.get(0).externalHoldbackModelLineId)
      .isEqualTo(modelLine2.externalModelLineId)
  }
}
