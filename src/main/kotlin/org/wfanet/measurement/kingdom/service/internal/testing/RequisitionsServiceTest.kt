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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Clock
import kotlin.random.Random
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase as ComputationParticipantsCoroutineService
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as DataProvidersCoroutineService
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as MeasurementConsumersCoroutineService
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase as MeasurementsCoroutineService
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase as RequisitionsCoroutineService
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.getRequisitionByDataProviderIdRequest
import org.wfanet.measurement.internal.kingdom.getRequisitionRequest
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementConsumer
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1L
private val EXTERNAL_DUCHY_IDS = listOf("Buck", "Rippon", "Shoaks")

@RunWith(JUnit4::class)
abstract class RequisitionsServiceTest<T : RequisitionsCoroutineService> {
  data class TestDataServices(
    val measurementConsumersService: MeasurementConsumersCoroutineService,
    val dataProvidersService: DataProvidersCoroutineService,
    val measurementsService: MeasurementsCoroutineService,
    val computationParticipantsService: ComputationParticipantsCoroutineService
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)
  @get:Rule val duchyIdSetter = DuchyIdSetter("alpha", "bravo", "charlie")

  protected lateinit var dataServices: TestDataServices
    private set

  /** Subject under test (SUT). */
  protected lateinit var service: T
    private set

  /** Constructs services used to populate test data. */
  protected abstract fun newTestDataServices(
    clock: Clock,
    idGenerator: IdGenerator
  ): TestDataServices

  /** Constructs the service being tested. */
  protected abstract fun newService(): T

  @Before
  fun initDataServices() {
    dataServices = newTestDataServices(clock, idGenerator)
  }

  @Before
  fun initService() {
    service = newService()
  }

  @Test
  fun `streamRequisitions returns all requisitions for EDP in order`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(dataServices.measurementConsumersService)
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val measurement1 =
      population.createMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider
      )
    val measurement2 =
      population.createMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 2",
        dataProvider
      )
    population.createMeasurement(
      dataServices.measurementsService,
      measurementConsumer,
      "measurement 3",
      population.createDataProvider(dataServices.dataProvidersService)
    )

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementId = measurement1.externalMeasurementId
        },
        requisition {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementId = measurement2.externalMeasurementId
        }
      )
      .inOrder()
  }

  @Test
  fun `streamRequisitions returns all requisitions for MC`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(dataServices.measurementConsumersService)
    val dataProvider1 = population.createDataProvider(dataServices.dataProvidersService)
    val dataProvider2 = population.createDataProvider(dataServices.dataProvidersService)
    val measurement1 =
      population.createMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider1,
        dataProvider2
      )
    val measurement2 =
      population.createMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 2",
        dataProvider1
      )
    population.createMeasurement(
      dataServices.measurementsService,
      population.createMeasurementConsumer(dataServices.measurementConsumersService),
      "other MC measurement",
      dataProvider1
    )

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              filter {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              }
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementId = measurement1.externalMeasurementId
          externalDataProviderId = dataProvider1.externalDataProviderId
        },
        requisition {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementId = measurement1.externalMeasurementId
          externalDataProviderId = dataProvider2.externalDataProviderId
        },
        requisition {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementId = measurement2.externalMeasurementId
          externalDataProviderId = dataProvider1.externalDataProviderId
        }
      )
  }

  @Test
  fun `streamRequisitions returns all requisitions for measurement`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(dataServices.measurementConsumersService)
    val dataProvider1 = population.createDataProvider(dataServices.dataProvidersService)
    val dataProvider2 = population.createDataProvider(dataServices.dataProvidersService)
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider1,
        dataProvider2
      )
    population.createMeasurement(
      dataServices.measurementsService,
      measurementConsumer,
      "measurement 2",
      dataProvider1
    )

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          externalMeasurementId = measurement.externalMeasurementId
          externalDataProviderId = dataProvider1.externalDataProviderId
        },
        requisition {
          externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          externalMeasurementId = measurement.externalMeasurementId
          externalDataProviderId = dataProvider2.externalDataProviderId
        }
      )
  }

  @Test
  fun `streamRequisitions respects updated_after`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(dataServices.measurementConsumersService)
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val measurement1 =
      population.createMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider
      )
    val measurement2 =
      population.createMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 2",
        dataProvider
      )
    population.createMeasurement(
      dataServices.measurementsService,
      measurementConsumer,
      "measurement 3",
      population.createDataProvider(dataServices.dataProvidersService)
    )

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              filter {
                externalDataProviderId = dataProvider.externalDataProviderId
                updatedAfter = measurement1.updateTime
              }
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementId = measurement2.externalMeasurementId
        }
      )
  }

  @Test
  fun `streamRequisitions respects limit`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(dataServices.measurementConsumersService)
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val measurement1 =
      population.createMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider
      )
    population.createMeasurement(
      dataServices.measurementsService,
      measurementConsumer,
      "measurement 2",
      dataProvider
    )

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
            limit = 1
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementId = measurement1.externalMeasurementId
        }
      )
  }

  @Test
  fun `getRequisition returns expected requisition`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(dataServices.measurementConsumersService)
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val providedMeasurementId = "measurement"
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        providedMeasurementId,
        dataProvider
      )

    val externalDataProviderId = dataProvider.externalDataProviderId
    val dataProviderValue: Measurement.DataProviderValue =
      measurement.dataProvidersMap[externalDataProviderId]!!
    val listedRequisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
          }
        )
        .first()
    val externalRequisitionId = listedRequisition.externalRequisitionId

    val requisition =
      service.getRequisition(
        getRequisitionRequest {
          externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          externalMeasurementId = measurement.externalMeasurementId
          this.externalRequisitionId = externalRequisitionId
        }
      )

    val expectedRequisition = requisition {
      externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
      externalMeasurementId = measurement.externalMeasurementId
      this.externalDataProviderId = externalDataProviderId
      this.externalRequisitionId = externalRequisitionId
      externalComputationId = measurement.externalComputationId
      externalDataProviderCertificateId = dataProviderValue.externalDataProviderCertificateId
      state = Requisition.State.UNFULFILLED
      details =
        RequisitionKt.details {
          dataProviderPublicKey = dataProviderValue.dataProviderPublicKey
          dataProviderPublicKeySignature = dataProviderValue.dataProviderPublicKeySignature
          encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
        }
      parentMeasurement =
        parentMeasurement {
          apiVersion = measurement.details.apiVersion
          externalMeasurementConsumerCertificateId =
            measurement.externalMeasurementConsumerCertificateId
          measurementSpec = measurement.details.measurementSpec
          measurementSpecSignature = measurement.details.measurementSpecSignature
          state = Measurement.State.PENDING_REQUISITION_PARAMS
        }
    }
    assertThat(requisition)
      .ignoringFields(Requisition.UPDATE_TIME_FIELD_NUMBER, Requisition.DUCHIES_FIELD_NUMBER)
      .isEqualTo(expectedRequisition)
    assertThat(requisition.duchiesMap)
      .containsExactly(
        "Buck",
        Requisition.DuchyValue.getDefaultInstance(),
        "Rippon",
        Requisition.DuchyValue.getDefaultInstance(),
        "Shoaks",
        Requisition.DuchyValue.getDefaultInstance()
      )
    assertThat(requisition).isEqualTo(listedRequisition)
  }

  @Test
  fun `getRequisitionByDataProviderId returns requisition`() = runBlocking {
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(dataServices.measurementConsumersService),
        "measurement",
        dataProvider
      )
    val listedRequisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
          }
        )
        .first()
    val externalRequisitionId = listedRequisition.externalRequisitionId

    val requisition =
      service.getRequisitionByDataProviderId(
        getRequisitionByDataProviderIdRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          this.externalRequisitionId = externalRequisitionId
        }
      )
    assertThat(requisition).isEqualTo(listedRequisition)
  }

  companion object {
    protected val API_VERSION = Version.V2_ALPHA
  }
}
