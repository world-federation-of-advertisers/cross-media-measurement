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
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Instant
import kotlin.random.Random
import kotlin.test.assertFailsWith
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
import org.wfanet.measurement.internal.kingdom.ModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.modelLine
import org.wfanet.measurement.internal.kingdom.modelSuite
import org.wfanet.measurement.internal.kingdom.setActiveEndTimeRequest

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
  fun `createModelLine fails for missing fields`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      displayName = "display name"
      description = "description"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelLinesService.createModelLine(modelLine) }

    Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    Truth.assertThat(exception)
      .hasMessageThat()
      .contains("ActiveStartTime field of ModelLine is missing fields.")
  }

  @Test
  fun `createModelLine fails for wrong activeStartTime fields`() = runBlocking {
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
      assertFailsWith<IllegalArgumentException> { modelLinesService.createModelLine(modelLine) }

    Truth.assertThat(exception)
      .hasMessageThat()
      .contains("ActiveStartTime must be in the future.")
  }

  @Test
  fun `createModelLine fails for wrong type fields`() = runBlocking {
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

    Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    Truth.assertThat(exception)
      .hasMessageThat()
      .contains("Unrecognized ModelLine's type")
  }

  @Test
  fun `createModelLine succeeds`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val ast = Instant.now().plusSeconds(2000L).toProtoTime()

    val modelLine = modelLine {
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalModelProviderId = modelSuite.externalModelProviderId
      activeStartTime = ast
      type = ModelLine.Type.PROD
      displayName = "display name"
      description = "description"
    }

    val createdModelLine = modelLinesService.createModelLine(modelLine)

    ProtoTruth.assertThat(createdModelLine)
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
          displayName = "display name"
          description = "description"
        }
      )
  }

  @Test
  fun `setActiveEndTime fails if ModelLine is not found`() = runBlocking {
    val modelSuite = population.createModelSuite(modelProvidersService, modelSuitesService)

    val setActiveEndTimeRequest = setActiveEndTimeRequest {
      externalModelLineId = 123L
      externalModelSuiteId = 456L
      externalModelProviderId = 789L
      activeEndTime = Instant.now().plusSeconds(2000L).toProtoTime()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelLinesService.setActiveEndTime(setActiveEndTimeRequest) }

    Truth.assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    Truth.assertThat(exception)
      .hasMessageThat()
      .contains("ModelLine not found.")
  }

}
