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
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import com.google.rpc.errorInfo
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.enumerateValidModelLinesRequest
import org.wfanet.measurement.internal.kingdom.enumerateValidModelLinesResponse
import org.wfanet.measurement.internal.kingdom.getModelLineRequest
import org.wfanet.measurement.internal.kingdom.modelLine
import org.wfanet.measurement.internal.kingdom.modelLineKey
import org.wfanet.measurement.internal.kingdom.modelRollout
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.internal.kingdom.setActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.setModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.setModelLineTypeRequest
import org.wfanet.measurement.internal.kingdom.streamModelLinesRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException

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
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.REQUIRED_FIELD_NOT_SET.name
          metadata["field_name"] = "active_start_time"
        }
      )
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
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.INVALID_FIELD_VALUE.name
          metadata["field_name"] = "active_end_time"
        }
      )
  }

  @Test
  fun `createModelLine fails when 'type' is not specified`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
      displayName = "display name"
      description = "description"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelLinesService.createModelLine(modelLine) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.REQUIRED_FIELD_NOT_SET.name
          metadata["field_name"] = "type"
        }
      )
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
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.INVALID_FIELD_VALUE.name
            metadata["field_name"] = "external_holdback_model_line_id"
          }
        )
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

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.MODEL_LINE_TYPE_ILLEGAL.name
            metadata["external_model_provider_id"] =
              prodModelLine.externalModelProviderId.toString()
            metadata["external_model_suite_id"] = prodModelLine.externalModelSuiteId.toString()
            metadata["external_model_line_id"] =
              prodModelLine.externalHoldbackModelLineId.toString()
            metadata["model_line_type"] = ModelLine.Type.DEV.toString()
          }
        )
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
  fun `createModelLine returns created ModelLine`() = runBlocking {
    val now = Instant.now()
    val modelProvider: ModelProvider = population.createModelProvider(modelProvidersService)
    val modelSuite: ModelSuite = population.createModelSuite(modelSuitesService, modelProvider)
    val request = modelLine {
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
      type = ModelLine.Type.DEV
      activeStartTime = now.plusSeconds(2000L).toProtoTime()
      displayName = "model-line-1"
      description = "ModelLine One"
    }

    val response: ModelLine = modelLinesService.createModelLine(request)

    assertThat(response)
      .ignoringFields(
        ModelLine.EXTERNAL_MODEL_LINE_ID_FIELD_NUMBER,
        ModelLine.CREATE_TIME_FIELD_NUMBER,
        ModelLine.UPDATE_TIME_FIELD_NUMBER,
      )
      .isEqualTo(request)
    assertThat(response.externalModelLineId).isNotEqualTo(0L)
    assertThat(response.createTime.toInstant()).isGreaterThan(now)
    assertThat(response.updateTime).isEqualTo(response.createTime)
    assertThat(response)
      .isEqualTo(
        modelLinesService.getModelLine(
          getModelLineRequest {
            externalModelProviderId = response.externalModelProviderId
            externalModelSuiteId = response.externalModelSuiteId
            externalModelLineId = response.externalModelLineId
          }
        )
      )
  }

  @Test
  fun `createModelLine returns created ModelLine with holdback`() = runBlocking {
    val now = Instant.now()
    val modelProvider: ModelProvider = population.createModelProvider(modelProvidersService)
    val modelSuite: ModelSuite = population.createModelSuite(modelSuitesService, modelProvider)
    val holdbackModelLine: ModelLine =
      modelLinesService.createModelLine(
        modelLine {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          type = ModelLine.Type.HOLDBACK
          activeStartTime = now.plusSeconds(2000L).toProtoTime()
          displayName = "holdback"
          description = "Holdback ML"
        }
      )
    val request = modelLine {
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
      type = ModelLine.Type.PROD
      activeStartTime = now.plusSeconds(2000L).toProtoTime()
      displayName = "prod"
      description = "Prod ML"
      externalHoldbackModelLineId = holdbackModelLine.externalModelLineId
    }

    val response = modelLinesService.createModelLine(request)

    assertThat(response)
      .ignoringFields(
        ModelLine.EXTERNAL_MODEL_LINE_ID_FIELD_NUMBER,
        ModelLine.CREATE_TIME_FIELD_NUMBER,
        ModelLine.UPDATE_TIME_FIELD_NUMBER,
      )
      .isEqualTo(request)
    assertThat(response)
      .isEqualTo(
        modelLinesService.getModelLine(
          getModelLineRequest {
            externalModelProviderId = response.externalModelProviderId
            externalModelSuiteId = response.externalModelSuiteId
            externalModelLineId = response.externalModelLineId
          }
        )
      )
  }

  @Test
  fun `getModelLine throws NOT_FOUND when model line not found`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelLinesService.getModelLine(
          getModelLineRequest {
            externalModelProviderId = 123L
            externalModelSuiteId = 124L
            externalModelLineId = 125L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getModelLine throws INVALID_ARGUMENT when external_model_line_id missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelLinesService.getModelLine(
          getModelLineRequest {
            externalModelProviderId = 123L
            externalModelSuiteId = 124L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
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
  fun `streamModelLines returns ModelLines filtered by type`(): Unit = runBlocking {
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
  fun `streamModelLines returns ModelLines filtered by active interval`(): Unit = runBlocking {
    val now = clock.instant()
    val modelProvider = population.createModelProvider(modelProvidersService)
    val modelSuite = population.createModelSuite(modelSuitesService, modelProvider)
    val modelSuite2 = population.createModelSuite(modelSuitesService, modelProvider)
    val modelProvider2 = population.createModelProvider(modelProvidersService)
    val modelSuite3 = population.createModelSuite(modelSuitesService, modelProvider2)
    val expectedModelLines =
      listOf(
        modelLinesService.createModelLine(
          modelLine {
            externalModelProviderId = modelSuite.externalModelProviderId
            externalModelSuiteId = modelSuite.externalModelSuiteId
            type = ModelLine.Type.PROD
            activeStartTime = now.plus(1L, ChronoUnit.DAYS).toProtoTime()
          }
        ),
        modelLinesService.createModelLine(
          modelLine {
            externalModelProviderId = modelSuite2.externalModelProviderId
            externalModelSuiteId = modelSuite2.externalModelSuiteId
            type = ModelLine.Type.PROD
            activeStartTime = now.plus(2L, ChronoUnit.DAYS).toProtoTime()
            activeEndTime = now.plus(92L, ChronoUnit.DAYS).toProtoTime()
          }
        ),
        modelLinesService.createModelLine(
          modelLine {
            externalModelProviderId = modelSuite3.externalModelProviderId
            externalModelSuiteId = modelSuite3.externalModelSuiteId
            type = ModelLine.Type.PROD
            activeStartTime = now.plus(3L, ChronoUnit.DAYS).toProtoTime()
            activeEndTime = now.plus(93L, ChronoUnit.DAYS).toProtoTime()
          }
        ),
      )
    modelLinesService.createModelLine(
      modelLine {
        externalModelProviderId = modelSuite.externalModelProviderId
        externalModelSuiteId = modelSuite.externalModelSuiteId
        type = ModelLine.Type.PROD
        activeStartTime = now.plus(1L, ChronoUnit.DAYS).toProtoTime()
        activeEndTime = now.plus(30L, ChronoUnit.DAYS).toProtoTime()
      }
    )

    val responses: List<ModelLine> =
      modelLinesService
        .streamModelLines(
          streamModelLinesRequest {
            filter = filter {
              activeIntervalContains = interval {
                startTime = now.plus(3L, ChronoUnit.DAYS).toProtoTime()
                endTime = now.plus(90L, ChronoUnit.DAYS).toProtoTime()
              }
            }
          }
        )
        .toList()

    assertThat(responses).containsExactlyElementsIn(expectedModelLines)
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

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.MODEL_LINE_TYPE_ILLEGAL.name
          metadata["external_model_provider_id"] = modelLine1.externalModelProviderId.toString()
          metadata["external_model_suite_id"] = modelLine1.externalModelSuiteId.toString()
          metadata["external_model_line_id"] = modelLine1.externalModelLineId.toString()
          metadata["model_line_type"] = ModelLine.Type.DEV.name
        }
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

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.MODEL_LINE_TYPE_ILLEGAL.name
            metadata["external_model_provider_id"] = modelLine2.externalModelProviderId.toString()
            metadata["external_model_suite_id"] = modelLine2.externalModelSuiteId.toString()
            metadata["external_model_line_id"] = modelLine2.externalModelLineId.toString()
            metadata["model_line_type"] = ModelLine.Type.DEV.name
          }
        )
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
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.INVALID_FIELD_VALUE.name
            metadata["field_name"] = "external_holdback_model_provider_id"
          }
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
              type += ModelLine.Type.PROD
            }
          }
        )
        .toList()

    assertThat(modelLines.get(0).externalHoldbackModelLineId)
      .isEqualTo(modelLine2.externalModelLineId)
  }

  @Test
  fun `setModelLineType returns updated ModelLine`() = runBlocking {
    val modelProvider: ModelProvider = population.createModelProvider(modelProvidersService)
    val modelSuite: ModelSuite = population.createModelSuite(modelSuitesService, modelProvider)
    val modelLine: ModelLine =
      modelLinesService.createModelLine(
        modelLine {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          type = ModelLine.Type.DEV
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          displayName = "model-line-1"
          description = "ModelLine One"
        }
      )
    val request = setModelLineTypeRequest {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      type = ModelLine.Type.PROD
    }

    val response: ModelLine = modelLinesService.setModelLineType(request)

    assertThat(response)
      .ignoringFields(ModelLine.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(modelLine.copy { type = request.type })
    assertThat(response.updateTime.toInstant()).isGreaterThan(modelLine.updateTime.toInstant())
    assertThat(response)
      .isEqualTo(
        modelLinesService.getModelLine(
          getModelLineRequest {
            externalModelProviderId = modelLine.externalModelProviderId
            externalModelSuiteId = modelLine.externalModelSuiteId
            externalModelLineId = modelLine.externalModelLineId
          }
        )
      )
  }

  @Test
  fun `setModelLineType throws if ModelLine has holdback`() = runBlocking {
    val modelProvider: ModelProvider = population.createModelProvider(modelProvidersService)
    val modelSuite: ModelSuite = population.createModelSuite(modelSuitesService, modelProvider)
    val holdbackModelLine: ModelLine =
      modelLinesService.createModelLine(
        modelLine {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          type = ModelLine.Type.HOLDBACK
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          displayName = "holdback"
          description = "Holdback ML"
        }
      )
    val modelLine =
      modelLinesService.createModelLine(
        modelLine {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          type = ModelLine.Type.PROD
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          displayName = "prod"
          description = "Prod ML"
          externalHoldbackModelLineId = holdbackModelLine.externalModelLineId
        }
      )
    val request = setModelLineTypeRequest {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      type = ModelLine.Type.DEV
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelLinesService.setModelLineType(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.MODEL_LINE_INVALID_ARGS.name
          metadata["external_model_provider_id"] = modelLine.externalModelProviderId.toString()
          metadata["external_model_suite_id"] = modelLine.externalModelSuiteId.toString()
          metadata["external_model_line_id"] = modelLine.externalModelLineId.toString()
        }
      )
    assertThat(exception).hasMessageThat().contains("holdback")
  }

  @Test
  fun `setModelLineType throws if request type is HOLDBACK`() = runBlocking {
    val modelProvider: ModelProvider = population.createModelProvider(modelProvidersService)
    val modelSuite: ModelSuite = population.createModelSuite(modelSuitesService, modelProvider)
    val modelLine: ModelLine =
      modelLinesService.createModelLine(
        modelLine {
          externalModelProviderId = modelSuite.externalModelProviderId
          externalModelSuiteId = modelSuite.externalModelSuiteId
          type = ModelLine.Type.DEV
          activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
          displayName = "model-line-1"
          description = "ModelLine One"
        }
      )
    val request = setModelLineTypeRequest {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      type = ModelLine.Type.HOLDBACK
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelLinesService.setModelLineType(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.INVALID_FIELD_VALUE.name
          metadata["field_name"] = "type"
        }
      )
  }

  @Test
  fun `enumerateValidModelLines returns model lines in order`(): Unit = runBlocking {
    val futureTime = Instant.now().plusSeconds(50L).toProtoTime()
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)
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
    val dataProvider =
      population.createDataProvider(dataProvidersService) {
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = oldProdModelLine.externalModelProviderId
              externalModelSuiteId = oldProdModelLine.externalModelSuiteId
              externalModelLineId = oldProdModelLine.externalModelLineId
            }
            value = interval {
              startTime = oldProdModelLine.activeStartTime
              endTime = Timestamps.add(oldProdModelLine.activeStartTime, Durations.fromSeconds(20))
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
              startTime = newProdModelLine.activeStartTime
              endTime = Timestamps.add(newProdModelLine.activeStartTime, Durations.fromSeconds(20))
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
              startTime = holdBackModelLine.activeStartTime
              endTime = Timestamps.add(holdBackModelLine.activeStartTime, Durations.fromSeconds(20))
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
              startTime = devModelLine.activeStartTime
              endTime = Timestamps.add(devModelLine.activeStartTime, Durations.fromSeconds(20))
            }
          }
      }
    val dataProvider2 =
      population.createDataProvider(dataProvidersService) {
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = oldProdModelLine.externalModelProviderId
              externalModelSuiteId = oldProdModelLine.externalModelSuiteId
              externalModelLineId = oldProdModelLine.externalModelLineId
            }
            value = interval {
              startTime = oldProdModelLine.activeStartTime
              endTime = Timestamps.add(oldProdModelLine.activeStartTime, Durations.fromSeconds(20))
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
              startTime = newProdModelLine.activeStartTime
              endTime = Timestamps.add(newProdModelLine.activeStartTime, Durations.fromSeconds(20))
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
              startTime = holdBackModelLine.activeStartTime
              endTime = Timestamps.add(holdBackModelLine.activeStartTime, Durations.fromSeconds(20))
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
              startTime = devModelLine.activeStartTime
              endTime = Timestamps.add(devModelLine.activeStartTime, Durations.fromSeconds(20))
            }
          }
      }

    val response =
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

    assertThat(response.modelLinesList)
      .containsExactly(newProdModelLine, oldProdModelLine, holdBackModelLine, devModelLine)
      .inOrder()
  }

  @Test
  fun `enumerateValidModelLines returns ModelLines across ModelProviders`(): Unit = runBlocking {
    val activeStartTime = Instant.now().plusSeconds(50L).toProtoTime()
    val endTime = Timestamps.add(activeStartTime, Durations.fromSeconds(10))
    val modelProvider = population.createModelProvider(modelProvidersService)
    val modelProvider2 = population.createModelProvider(modelProvidersService)
    val modelSuite = population.createModelSuite(modelSuitesService, modelProvider)
    val modelSuite2 = population.createModelSuite(modelSuitesService, modelProvider2)
    val modelLine =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite.externalModelSuiteId
          externalModelProviderId = modelSuite.externalModelProviderId
          this.activeStartTime = activeStartTime
          type = ModelLine.Type.PROD
          displayName = "display name"
          description = "description"
        }
      )
    val modelLine2 =
      modelLinesService.createModelLine(
        modelLine {
          externalModelSuiteId = modelSuite2.externalModelSuiteId
          externalModelProviderId = modelSuite2.externalModelProviderId
          this.activeStartTime = activeStartTime
          type = ModelLine.Type.PROD
          displayName = "display name"
          description = "description"
        }
      )
    val dataProvider =
      population.createDataProvider(dataProvidersService) {
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
            }
            value = interval {
              startTime = activeStartTime
              this.endTime = endTime
            }
          }
        dataAvailabilityIntervals +=
          DataProviderKt.dataAvailabilityMapEntry {
            key = modelLineKey {
              externalModelProviderId = modelLine2.externalModelProviderId
              externalModelSuiteId = modelLine2.externalModelSuiteId
              externalModelLineId = modelLine2.externalModelLineId
            }
            value = interval {
              startTime = activeStartTime
              this.endTime = endTime
            }
          }
      }

    val response =
      modelLinesService.enumerateValidModelLines(
        enumerateValidModelLinesRequest {
          types += ModelLine.Type.PROD
          externalDataProviderIds += dataProvider.externalDataProviderId
          timeInterval = interval {
            startTime = activeStartTime
            this.endTime = endTime
          }
        }
      )

    assertThat(response.modelLinesList).containsExactly(modelLine, modelLine2)
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
  fun `enumerateValidModelLines does not return model line if time interval not in EDP interval`():
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
  fun `enumerateValidModelLines returns no model lines if time interval not in EDP interval`():
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
  fun `enumerateValidModelLines returns no model lines if no EDP interval for model line`(): Unit =
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
  fun `enumerateValidModelLines returns no lines if no interval for model line for every EDP`():
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
  fun `enumerateValidModelLines returns no lines if interval not in every EDP interval`(): Unit =
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
