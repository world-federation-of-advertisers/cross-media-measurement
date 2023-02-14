// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntryKt
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryKt
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StateTransitionMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.createDuchyMeasurementLogEntryRequest
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.internal.kingdom.streamStateTransitionMeasurementLogEntriesRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private val EXTERNAL_DUCHY_IDS = listOf("Buck", "Rippon", "Shoaks")

@RunWith(JUnit4::class)
abstract class MeasurementLogEntriesServiceTest<T : MeasurementLogEntriesCoroutineImplBase> {

  @get:Rule val duchyIdSetter = DuchyIdSetter(EXTERNAL_DUCHY_IDS)

  protected data class Services<T>(
    val measurementLogEntriesService: T,
    val measurementsService: MeasurementsCoroutineImplBase,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val accountsService: AccountsCoroutineImplBase
  )
  private val testClock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(testClock, Random(RANDOM_SEED))
  private val population = Population(testClock, idGenerator)

  private lateinit var measurementLogEntriesService: T

  protected lateinit var measurementsService: MeasurementsCoroutineImplBase
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var accountsService: AccountsCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  private fun readMeasurementStateLog(
    externalMeasurementId: Long
  ): Flow<StateTransitionMeasurementLogEntry> {
    return measurementLogEntriesService.streamStateTransitionMeasurementLogEntry(
      streamStateTransitionMeasurementLogEntriesRequest {
        this.externalMeasurementId = externalMeasurementId
      }
    )
  }

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    measurementLogEntriesService = services.measurementLogEntriesService
    measurementConsumersService = services.measurementConsumersService
    measurementsService = services.measurementsService
    dataProvidersService = services.dataProvidersService
    accountsService = services.accountsService
  }

  @Test
  fun `createMeasurementLogEntry fails for wrong externalComputationId`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementLogEntriesService.createDuchyMeasurementLogEntry(
          createDuchyMeasurementLogEntryRequest {
            externalComputationId = 1234L // WrongID
            externalDuchyId = EXTERNAL_DUCHY_IDS[0]
            measurementLogEntryDetails =
              MeasurementLogEntryKt.details {
                error =
                  MeasurementLogEntryKt.errorDetails {
                    type = MeasurementLogEntry.ErrorDetails.Type.TRANSIENT
                  }
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Measurement not found")
  }

  @Test
  fun `createMeasurementLogEntry fails for missing Duchy`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementLogEntriesService.createDuchyMeasurementLogEntry(
          createDuchyMeasurementLogEntryRequest {
            externalComputationId = measurement.externalComputationId
            externalDuchyId = "wrong duchy id" // WrongID
            measurementLogEntryDetails =
              MeasurementLogEntryKt.details {
                error =
                  MeasurementLogEntryKt.errorDetails {
                    type = MeasurementLogEntry.ErrorDetails.Type.TRANSIENT
                  }
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Duchy not found")
  }

  @Test
  fun `createMeasurementLogEntry fails for PERMENANT error type`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementLogEntriesService.createDuchyMeasurementLogEntry(
          createDuchyMeasurementLogEntryRequest {
            externalComputationId = 1L
            externalDuchyId = EXTERNAL_DUCHY_IDS[0]
            measurementLogEntryDetails =
              MeasurementLogEntryKt.details {
                error =
                  MeasurementLogEntryKt.errorDetails {
                    type = MeasurementLogEntry.ErrorDetails.Type.PERMANENT
                  }
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `create transient error MeasurementLogEntry succeeds`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider
      )

    val measurementLogEntryDetails =
      MeasurementLogEntryKt.details {
        error =
          MeasurementLogEntryKt.errorDetails {
            type = MeasurementLogEntry.ErrorDetails.Type.TRANSIENT
          }
        logMessage = "some log message"
      }

    val duchyMeasurementLogEntryDetails =
      DuchyMeasurementLogEntryKt.details {
        duchyChildReferenceId = "some child reference"
        stageAttempt = DuchyMeasurementLogEntryKt.stageAttempt { stage = 1 }
      }

    val createdDuchyMeasurementLogEntry =
      measurementLogEntriesService.createDuchyMeasurementLogEntry(
        createDuchyMeasurementLogEntryRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = EXTERNAL_DUCHY_IDS[0]
          this.measurementLogEntryDetails = measurementLogEntryDetails
          details = duchyMeasurementLogEntryDetails
        }
      )

    val expectedDuchyMeasurementLogEntry = duchyMeasurementLogEntry {
      externalDuchyId = EXTERNAL_DUCHY_IDS[0]
      externalComputationLogEntryId = createdDuchyMeasurementLogEntry.externalComputationLogEntryId
      logEntry = measurementLogEntry {
        this.externalMeasurementId = measurement.externalMeasurementId
        this.externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
        details = measurementLogEntryDetails
        createTime = createdDuchyMeasurementLogEntry.logEntry.createTime
      }
      details = duchyMeasurementLogEntryDetails
    }

    assertThat(createdDuchyMeasurementLogEntry.externalComputationLogEntryId).isNotEqualTo(0L)
    assertThat(createdDuchyMeasurementLogEntry.logEntry.createTime.seconds).isGreaterThan(0L)
    assertThat(createdDuchyMeasurementLogEntry).isEqualTo(expectedDuchyMeasurementLogEntry)
  }

  @Test
  fun `create non-error MeasurementLogEntry succeeds`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider
      )

    val measurementLogEntryDetails = MeasurementLogEntryKt.details { logMessage = "This is a log." }

    val duchyMeasurementLogEntryDetails =
      DuchyMeasurementLogEntryKt.details {
        duchyChildReferenceId = "some child reference"
        stageAttempt = DuchyMeasurementLogEntryKt.stageAttempt { stage = 1 }
      }

    val createdDuchyMeasurementLogEntry =
      measurementLogEntriesService.createDuchyMeasurementLogEntry(
        createDuchyMeasurementLogEntryRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = EXTERNAL_DUCHY_IDS[0]
          this.measurementLogEntryDetails = measurementLogEntryDetails
          details = duchyMeasurementLogEntryDetails
        }
      )

    val expectedDuchyMeasurementLogEntry = duchyMeasurementLogEntry {
      externalDuchyId = EXTERNAL_DUCHY_IDS[0]
      externalComputationLogEntryId = createdDuchyMeasurementLogEntry.externalComputationLogEntryId
      logEntry = measurementLogEntry {
        this.externalMeasurementId = measurement.externalMeasurementId
        this.externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
        details = measurementLogEntryDetails
        createTime = createdDuchyMeasurementLogEntry.logEntry.createTime
      }
      details = duchyMeasurementLogEntryDetails
    }

    assertThat(createdDuchyMeasurementLogEntry.externalComputationLogEntryId).isNotEqualTo(0L)
    assertThat(createdDuchyMeasurementLogEntry.logEntry.createTime.seconds).isGreaterThan(0L)
    assertThat(createdDuchyMeasurementLogEntry).isEqualTo(expectedDuchyMeasurementLogEntry)
  }

  @Test
  fun `measurementState is consistent when creating and cancelling a Measurement`(): Unit =
    runBlocking {
      var measurement: Measurement
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider = population.createDataProvider(dataProvidersService)

      measurement =
        population.createComputedMeasurement(
          measurementsService,
          measurementConsumer,
          "measurement 1",
          dataProvider
        )

      assertThat(readMeasurementStateLog(measurement.externalMeasurementConsumerId).count())
        .isEqualTo(0)

      measurement =
        measurementsService.cancelMeasurement(
          cancelMeasurementRequest {
            externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
            externalMeasurementId = measurement.externalMeasurementId
          }
        )

      val measurementStateTransitionLogEntry: StateTransitionMeasurementLogEntry =
        readMeasurementStateLog(measurement.externalMeasurementId).single()

      assertThat(measurementStateTransitionLogEntry.currentState)
        .isEqualTo(Measurement.State.CANCELLED)
      assertThat(measurementStateTransitionLogEntry.previousState)
        .isEqualTo(Measurement.State.PENDING_REQUISITION_PARAMS)
    }
}
