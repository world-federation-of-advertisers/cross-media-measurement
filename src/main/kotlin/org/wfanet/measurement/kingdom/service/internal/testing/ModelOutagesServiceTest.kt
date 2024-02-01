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
import org.wfanet.measurement.internal.kingdom.ModelOutage
import org.wfanet.measurement.internal.kingdom.ModelOutagesGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequestKt.outageInterval
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.deleteModelOutageRequest
import org.wfanet.measurement.internal.kingdom.modelOutage
import org.wfanet.measurement.internal.kingdom.streamModelOutagesRequest

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ModelOutagesServiceTest<T : ModelOutagesGrpcKt.ModelOutagesCoroutineImplBase> {

  protected data class Services<T>(
    val modelOutagesService: T,
    val modelProvidersService: ModelProvidersCoroutineImplBase,
    val modelSuitesService: ModelSuitesCoroutineImplBase,
    val modelLinesService: ModelLinesCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var modelProvidersService: ModelProvidersCoroutineImplBase
    private set

  protected lateinit var modelSuitesService: ModelSuitesCoroutineImplBase
    private set

  protected lateinit var modelLinesService: ModelLinesCoroutineImplBase
    private set

  protected lateinit var modelOutagesService: T
    private set

  protected abstract fun newServices(clock: Clock, idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(clock, idGenerator)
    modelOutagesService = services.modelOutagesService
    modelProvidersService = services.modelProvidersService
    modelSuitesService = services.modelSuitesService
    modelLinesService = services.modelLinesService
  }

  @Test
  fun `createModelOutage succeeds`() = runBlocking {
    val modelLine =
      population.createModelLine(
        modelProvidersService,
        modelSuitesService,
        modelLinesService,
        ModelLine.Type.PROD,
        true,
      )

    val modelOutage = modelOutage {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
      modelOutageEndTime = Instant.now().plusSeconds(100L).toProtoTime()
    }
    val createdModelOutage = modelOutagesService.createModelOutage(modelOutage)

    assertThat(createdModelOutage)
      .ignoringFields(
        ModelOutage.CREATE_TIME_FIELD_NUMBER,
        ModelOutage.DELETE_TIME_FIELD_NUMBER,
        ModelOutage.EXTERNAL_MODEL_OUTAGE_ID_FIELD_NUMBER,
      )
      .isEqualTo(modelOutage.copy { state = ModelOutage.State.ACTIVE })
  }

  @Test
  fun `createModelOutage fails when Model Line type is not equal to PROD`() = runBlocking {
    val devModelLine =
      population.createModelLine(
        modelProvidersService,
        modelSuitesService,
        modelLinesService,
        ModelLine.Type.DEV,
      )

    val modelOutage = modelOutage {
      externalModelProviderId = devModelLine.externalModelProviderId
      externalModelSuiteId = devModelLine.externalModelSuiteId
      externalModelLineId = devModelLine.externalModelLineId
      modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
      modelOutageEndTime = Instant.now().plusSeconds(100L).toProtoTime()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelOutagesService.createModelOutage(modelOutage) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("ModelOutage can be created only for model lines having type equal to 'PROD'.")
  }

  @Test
  fun `createModelOutage fails when Model Line doesn't have a HoldbackModelLine set`() =
    runBlocking {
      val devModelLine =
        population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)

      val modelOutage = modelOutage {
        externalModelProviderId = devModelLine.externalModelProviderId
        externalModelSuiteId = devModelLine.externalModelSuiteId
        externalModelLineId = devModelLine.externalModelLineId
        modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
        modelOutageEndTime = Instant.now().plusSeconds(100L).toProtoTime()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          modelOutagesService.createModelOutage(modelOutage)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("ModelOutage can be created only for model lines having a HoldbackModelLine.")
    }

  @Test
  fun `createModelOutage fails when Model Line is not found`() = runBlocking {
    val modelOutage = modelOutage {
      externalModelProviderId = 123L
      externalModelSuiteId = 123L
      externalModelLineId = 123L
      modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
      modelOutageEndTime = Instant.now().plusSeconds(100L).toProtoTime()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelOutagesService.createModelOutage(modelOutage) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelLine not found")
  }

  @Test
  fun `createModelOutage fails when outage interval is missing`() = runBlocking {
    val modelOutage = modelOutage {
      externalModelProviderId = 123L
      externalModelSuiteId = 123L
      externalModelLineId = 123L
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelOutagesService.createModelOutage(modelOutage) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Outage interval is missing.")
  }

  @Test
  fun `createModelOutage fails when outage interval is invalid`() = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)

    val modelOutage = modelOutage {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      modelOutageStartTime = Instant.now().plusSeconds(100L).toProtoTime()
      modelOutageEndTime = Instant.now().minusSeconds(100L).toProtoTime()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelOutagesService.createModelOutage(modelOutage) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("ModelOutageStartTime cannot precede ModelOutageEndTime.")
  }

  @Test
  fun `deleteModelOutage succeeds`() = runBlocking {
    val modelLine =
      population.createModelLine(
        modelProvidersService,
        modelSuitesService,
        modelLinesService,
        ModelLine.Type.PROD,
        true,
      )

    val modelOutage = modelOutage {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
      modelOutageEndTime = Instant.now().plusSeconds(100L).toProtoTime()
    }
    val createdModelOutage = modelOutagesService.createModelOutage(modelOutage)

    val deletedModelOutage =
      modelOutagesService.deleteModelOutage(
        deleteModelOutageRequest {
          this.externalModelProviderId = modelOutage.externalModelProviderId
          this.externalModelSuiteId = modelOutage.externalModelSuiteId
          this.externalModelLineId = modelOutage.externalModelLineId
          this.externalModelOutageId = createdModelOutage.externalModelOutageId
        }
      )

    assertThat(deletedModelOutage)
      .isEqualTo(
        createdModelOutage.copy {
          this.deleteTime = deletedModelOutage.deleteTime
          this.state = ModelOutage.State.DELETED
        }
      )
  }

  @Test
  fun `deleteModelOutage fails when externalModelProviderId is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelOutagesService.deleteModelOutage(
          deleteModelOutageRequest {
            externalModelSuiteId = 123L
            externalModelLineId = 123L
            externalModelOutageId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalModelProviderId unspecified")
  }

  @Test
  fun `deleteModelOutage fails when externalModelSuiteId is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelOutagesService.deleteModelOutage(
          deleteModelOutageRequest {
            externalModelProviderId = 123L
            externalModelLineId = 123L
            externalModelOutageId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalModelSuiteId unspecified")
  }

  @Test
  fun `deleteModelOutage fails when externalModelLineId is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelOutagesService.deleteModelOutage(
          deleteModelOutageRequest {
            externalModelProviderId = 123L
            externalModelSuiteId = 123L
            externalModelOutageId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalModelLineId unspecified")
  }

  @Test
  fun `deleteModelOutage fails when externalModelOutageId is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelOutagesService.deleteModelOutage(
          deleteModelOutageRequest {
            externalModelProviderId = 123L
            externalModelSuiteId = 123L
            externalModelLineId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalModelOutageId unspecified")
  }

  @Test
  fun `deleteModelOutage fails when Model Outage is not found`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelOutagesService.deleteModelOutage(
          deleteModelOutageRequest {
            this.externalModelProviderId = 123L
            this.externalModelSuiteId = 123L
            this.externalModelLineId = 123L
            this.externalModelOutageId = 123L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelOutage not found")
  }

  @Test
  fun `deleteModelOutage fails for deleted ModelOutage`() = runBlocking {
    val modelLine =
      population.createModelLine(
        modelProvidersService,
        modelSuitesService,
        modelLinesService,
        ModelLine.Type.PROD,
        true,
      )

    val modelOutage = modelOutage {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
      modelOutageEndTime = Instant.now().plusSeconds(100L).toProtoTime()
    }
    val createdModelOutage = modelOutagesService.createModelOutage(modelOutage)

    val deleteModelOutageRequest = deleteModelOutageRequest {
      this.externalModelProviderId = modelOutage.externalModelProviderId
      this.externalModelSuiteId = modelOutage.externalModelSuiteId
      this.externalModelLineId = modelOutage.externalModelLineId
      this.externalModelOutageId = createdModelOutage.externalModelOutageId
    }

    modelOutagesService.deleteModelOutage(deleteModelOutageRequest)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelOutagesService.deleteModelOutage(deleteModelOutageRequest)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ModelOutage state is DELETED.")
  }

  @Test
  fun `streamModelOutages returns all ACTIVE model outages`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(
        modelProvidersService,
        modelSuitesService,
        modelLinesService,
        ModelLine.Type.PROD,
        true,
      )

    val modelOutage = modelOutage {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
      modelOutageEndTime = Instant.now().plusSeconds(100L).toProtoTime()
    }

    val modelOutage1 = modelOutagesService.createModelOutage(modelOutage)

    val modelOutage2 = modelOutagesService.createModelOutage(modelOutage)

    val modelOutage3 = modelOutagesService.createModelOutage(modelOutage)

    modelOutagesService.deleteModelOutage(
      deleteModelOutageRequest {
        externalModelProviderId = modelOutage3.externalModelProviderId
        externalModelSuiteId = modelOutage3.externalModelSuiteId
        externalModelLineId = modelOutage3.externalModelLineId
        externalModelOutageId = modelOutage3.externalModelOutageId
      }
    )

    val modelOutages: List<ModelOutage> =
      modelOutagesService
        .streamModelOutages(
          streamModelOutagesRequest {
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
            }
          }
        )
        .toList()

    assertThat(modelOutages)
      .comparingExpectedFieldsOnly()
      .containsExactly(modelOutage1, modelOutage2)
      .inOrder()
  }

  @Test
  fun `streamModelOutages returns all ACTIVE and DELETED model outages`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(
        modelProvidersService,
        modelSuitesService,
        modelLinesService,
        ModelLine.Type.PROD,
        true,
      )

    val modelOutage = modelOutage {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
      modelOutageEndTime = Instant.now().plusSeconds(100L).toProtoTime()
    }

    val modelOutage1 = modelOutagesService.createModelOutage(modelOutage)

    val modelOutage2 = modelOutagesService.createModelOutage(modelOutage)

    val modelOutage3 = modelOutagesService.createModelOutage(modelOutage)

    val deletedModelOutage3 =
      modelOutagesService.deleteModelOutage(
        deleteModelOutageRequest {
          externalModelProviderId = modelOutage3.externalModelProviderId
          externalModelSuiteId = modelOutage3.externalModelSuiteId
          externalModelLineId = modelOutage3.externalModelLineId
          externalModelOutageId = modelOutage3.externalModelOutageId
        }
      )

    val modelOutages: List<ModelOutage> =
      modelOutagesService
        .streamModelOutages(
          streamModelOutagesRequest {
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
              showDeleted = true
            }
          }
        )
        .toList()

    assertThat(modelOutages)
      .comparingExpectedFieldsOnly()
      .containsExactly(modelOutage1, modelOutage2, deletedModelOutage3)
      .inOrder()
  }

  @Test
  fun `streamModelOutages can get one page at a time`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(
        modelProvidersService,
        modelSuitesService,
        modelLinesService,
        ModelLine.Type.PROD,
        true,
      )

    val modelOutage = modelOutage {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
      modelOutageEndTime = Instant.now().plusSeconds(100L).toProtoTime()
    }

    val modelOutage1 = modelOutagesService.createModelOutage(modelOutage)

    val modelOutage2 = modelOutagesService.createModelOutage(modelOutage)

    val modelOutages: List<ModelOutage> =
      modelOutagesService
        .streamModelOutages(
          streamModelOutagesRequest {
            limit = 1
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
            }
          }
        )
        .toList()

    assertThat(modelOutages).hasSize(1)
    assertThat(modelOutages).contains(modelOutage1)

    val modelOutages2: List<ModelOutage> =
      modelOutagesService
        .streamModelOutages(
          streamModelOutagesRequest {
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
              after = afterFilter {
                createTime = modelOutages[0].createTime
                externalModelProviderId = modelOutages[0].externalModelProviderId
                externalModelSuiteId = modelOutages[0].externalModelSuiteId
                externalModelLineId = modelOutages[0].externalModelLineId
                externalModelOutageId = modelOutages[0].externalModelOutageId
              }
            }
          }
        )
        .toList()

    assertThat(modelOutages2).hasSize(1)
    assertThat(modelOutages2).contains(modelOutage2)
  }

  @Test
  fun `streamModelOutages fails for missing after filter fields`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(
        modelProvidersService,
        modelSuitesService,
        modelLinesService,
        ModelLine.Type.PROD,
        true,
      )

    val modelOutage = modelOutage {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
      modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
      modelOutageEndTime = Instant.now().plusSeconds(100L).toProtoTime()
    }

    modelOutagesService.createModelOutage(modelOutage)
    modelOutagesService.createModelOutage(modelOutage)

    val modelOutages: List<ModelOutage> =
      modelOutagesService
        .streamModelOutages(
          streamModelOutagesRequest {
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
        modelOutagesService.streamModelOutages(
          streamModelOutagesRequest {
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
              after = afterFilter {
                createTime = modelOutages[0].createTime
                externalModelSuiteId = modelOutages[0].externalModelSuiteId
                externalModelOutageId = modelOutages[0].externalModelOutageId
              }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Missing After filter fields")
  }

  @Test
  fun `streamModelOutages fails when limit is less than 0`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(modelProvidersService, modelSuitesService, modelLinesService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelOutagesService.streamModelOutages(
          streamModelOutagesRequest {
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
  fun `streamModelOutages filter by outage interval`(): Unit = runBlocking {
    val modelLine =
      population.createModelLine(
        modelProvidersService,
        modelSuitesService,
        modelLinesService,
        ModelLine.Type.PROD,
        true,
      )

    val START_OUTAGE_LIMIT_1 = Instant.now().minusSeconds(2000L).toProtoTime()
    val END_OUTAGE_LIMIT_1 = Instant.now().minusSeconds(1000L).toProtoTime()
    val START_OUTAGE_LIMIT_2 = Instant.now().plusSeconds(2000L).toProtoTime()
    val END_OUTAGE_LIMIT_2 = Instant.now().plusSeconds(3000L).toProtoTime()
    val END_OUTAGE_QUERY_LIMIT = Instant.now().plusSeconds(4000L).toProtoTime()

    val modelOutage = modelOutage {
      externalModelProviderId = modelLine.externalModelProviderId
      externalModelSuiteId = modelLine.externalModelSuiteId
      externalModelLineId = modelLine.externalModelLineId
    }

    modelOutagesService.createModelOutage(
      modelOutage.copy {
        modelOutageStartTime = START_OUTAGE_LIMIT_1
        modelOutageEndTime = END_OUTAGE_LIMIT_1
      }
    )

    modelOutagesService.createModelOutage(
      modelOutage.copy {
        modelOutageStartTime = START_OUTAGE_LIMIT_1
        modelOutageEndTime = END_OUTAGE_LIMIT_1
      }
    )

    val modelOutage3 =
      modelOutagesService.createModelOutage(
        modelOutage.copy {
          modelOutageStartTime = START_OUTAGE_LIMIT_2
          modelOutageEndTime = END_OUTAGE_LIMIT_2
        }
      )

    val modelOutages: List<ModelOutage> =
      modelOutagesService
        .streamModelOutages(
          streamModelOutagesRequest {
            filter = filter {
              externalModelProviderId = modelLine.externalModelProviderId
              externalModelSuiteId = modelLine.externalModelSuiteId
              externalModelLineId = modelLine.externalModelLineId
              outageInterval = outageInterval {
                modelOutageStartTime = START_OUTAGE_LIMIT_2
                modelOutageEndTime = END_OUTAGE_QUERY_LIMIT
              }
            }
          }
        )
        .toList()

    assertThat(modelOutages).comparingExpectedFieldsOnly().containsExactly(modelOutage3).inOrder()
  }

  @Test
  fun `streamModelOutages fails for missing outage interval filter fields`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelOutagesService.streamModelOutages(
          streamModelOutagesRequest {
            filter = filter {
              outageInterval = outageInterval {
                modelOutageStartTime = Instant.now().minusSeconds(100L).toProtoTime()
              }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Missing OutageInterval fields")
  }
}
