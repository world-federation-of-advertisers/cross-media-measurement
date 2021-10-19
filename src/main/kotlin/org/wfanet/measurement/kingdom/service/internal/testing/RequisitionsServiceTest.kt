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
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase as CertificatesCoroutineService
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase as ComputationParticipantsCoroutineService
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as DataProvidersCoroutineService
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as MeasurementConsumersCoroutineService
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase as MeasurementsCoroutineService
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.RequisitionKt.refusal
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase as RequisitionsCoroutineService
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.getRequisitionByDataProviderIdRequest
import org.wfanet.measurement.internal.kingdom.getRequisitionRequest
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.internal.kingdom.refuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.internal.kingdom.setParticipantRequisitionParamsRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1L
private val EXTERNAL_DUCHY_IDS = listOf("Buck", "Rippon", "Shoaks")

private val REFUSAL = refusal {
  justification = Requisition.Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET
  message = "MC wrote check that EDP couldn't cash"
}

@RunWith(JUnit4::class)
abstract class RequisitionsServiceTest<T : RequisitionsCoroutineService> {
  data class TestDataServices(
    val measurementConsumersService: MeasurementConsumersCoroutineService,
    val dataProvidersService: DataProvidersCoroutineService,
    val measurementsService: MeasurementsCoroutineService,
    val computationParticipantsService: ComputationParticipantsCoroutineService,
    val certificatesService: CertificatesCoroutineService
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)
  @get:Rule val duchyIdSetter = DuchyIdSetter(EXTERNAL_DUCHY_IDS)

  protected lateinit var dataServices: TestDataServices
    private set

  protected lateinit var duchyCertificates: Map<String, Certificate>
    private set

  /** Subject under test (SUT). */
  protected lateinit var service: T
    private set

  /** Constructs services used to populate test data. */
  protected abstract fun newTestDataServices(idGenerator: IdGenerator): TestDataServices

  /** Constructs the service being tested. */
  protected abstract fun newService(): T

  @Before
  fun initDataServices() {
    dataServices = newTestDataServices(idGenerator)

    duchyCertificates =
      EXTERNAL_DUCHY_IDS.associateWith { externalDuchyId ->
        runBlocking {
          population.createDuchyCertificate(dataServices.certificatesService, externalDuchyId)
        }
      }
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
      state = Requisition.State.UNFULFILLED
      details =
        RequisitionKt.details {
          dataProviderPublicKey = dataProviderValue.dataProviderPublicKey
          dataProviderPublicKeySignature = dataProviderValue.dataProviderPublicKeySignature
          encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
        }
      dataProviderCertificate = dataProvider.certificate
      parentMeasurement =
        parentMeasurement {
          apiVersion = measurement.details.apiVersion
          externalMeasurementConsumerCertificateId =
            measurement.externalMeasurementConsumerCertificateId
          measurementSpec = measurement.details.measurementSpec
          measurementSpecSignature = measurement.details.measurementSpecSignature
          state = Measurement.State.PENDING_REQUISITION_PARAMS
          protocolConfig =
            protocolConfig { liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance() }
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

  @Test
  fun `fulfillRequisition transitions Requisition state`() = runBlocking {
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(dataServices.measurementConsumersService),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService)
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
        }
      )
    }
    val requisition =
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

    val participationSignature = ByteString.copyFromUtf8("Participation signature")
    val response =
      service.fulfillRequisition(
        fulfillRequisitionRequest {
          externalComputationId = measurement.externalComputationId
          externalRequisitionId = requisition.externalRequisitionId
          externalFulfillingDuchyId = "Buck"
          dataProviderParticipationSignature = participationSignature
        }
      )

    assertThat(response.state).isEqualTo(Requisition.State.FULFILLED)
    assertThat(response.externalFulfillingDuchyId).isEqualTo("Buck")
    assertThat(response.details.dataProviderParticipationSignature)
      .isEqualTo(participationSignature)
    assertThat(response.updateTime.toInstant()).isGreaterThan(requisition.updateTime.toInstant())
    assertThat(response)
      .isEqualTo(
        service.getRequisition(
          getRequisitionRequest {
            externalMeasurementId = measurement.externalMeasurementId
            externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
            externalRequisitionId = requisition.externalRequisitionId
          }
        )
      )
  }

  @Test
  fun `fulfillRequisition transitions Measurement state when all others fulfilled`() = runBlocking {
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(dataServices.measurementConsumersService),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService)
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
        }
      )
    }
    val requisitions =
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
    val participationSignature = ByteString.copyFromUtf8("Participation signature")
    service.fulfillRequisition(
      fulfillRequisitionRequest {
        externalComputationId = measurement.externalComputationId
        externalRequisitionId = requisitions[0].externalRequisitionId
        externalFulfillingDuchyId = "Buck"
        dataProviderParticipationSignature = participationSignature
      }
    )

    val response =
      service.fulfillRequisition(
        fulfillRequisitionRequest {
          externalComputationId = measurement.externalComputationId
          externalRequisitionId = requisitions[1].externalRequisitionId
          externalFulfillingDuchyId = "Rippon"
          dataProviderParticipationSignature = participationSignature
        }
      )

    assertThat(response.parentMeasurement.state)
      .isEqualTo(Measurement.State.PENDING_PARTICIPANT_CONFIRMATION)
    assertThat(response)
      .isEqualTo(
        service.getRequisition(
          getRequisitionRequest {
            externalMeasurementId = measurement.externalMeasurementId
            externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
            externalRequisitionId = requisitions[1].externalRequisitionId
          }
        )
      )
  }

  @Test
  fun `fulfillRequisition throws NOT_FOUND if Requisition not found`() = runBlocking {
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(dataServices.measurementConsumersService),
        "measurement",
        dataProvider
      )

    val nonExistantExternalRequisitionId = idGenerator.generateExternalId()
    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.fulfillRequisition(
          fulfillRequisitionRequest {
            externalComputationId = measurement.externalComputationId
            externalRequisitionId = nonExistantExternalRequisitionId.value
            externalFulfillingDuchyId = "Buck"
            dataProviderParticipationSignature = ByteString.copyFromUtf8("Participation signature")
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `fulfillRequisition throws FAILED_PRECONDITION if Duchy not found`() = runBlocking {
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(dataServices.measurementConsumersService),
        "measurement",
        dataProvider
      )
    val requisition =
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

    val nonExistantExternalDuchyId = "Chalced"
    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.fulfillRequisition(
          fulfillRequisitionRequest {
            externalComputationId = measurement.externalComputationId
            externalRequisitionId = requisition.externalRequisitionId
            externalFulfillingDuchyId = nonExistantExternalDuchyId
            dataProviderParticipationSignature = ByteString.copyFromUtf8("Participation signature")
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `fulfillRequisition throws FAILED_PRECONDITION if Measurement in illegal state`() =
      runBlocking {
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(dataServices.measurementConsumersService),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService)
      )
    val requisition =
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

    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.fulfillRequisition(
          fulfillRequisitionRequest {
            externalComputationId = measurement.externalComputationId
            externalRequisitionId = requisition.externalRequisitionId
            externalFulfillingDuchyId = "Buck"
            dataProviderParticipationSignature = ByteString.copyFromUtf8("Participation signature")
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `fulfillRequisition throws INVALID_ARGUMENT when signature not specified`() = runBlocking {
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(dataServices.measurementConsumersService),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService)
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
        }
      )
    }
    val requisition =
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

    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.fulfillRequisition(
          fulfillRequisitionRequest {
            externalComputationId = measurement.externalComputationId
            externalRequisitionId = requisition.externalRequisitionId
            externalFulfillingDuchyId = "Buck"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisition transitions Requisition and Measurement states`() = runBlocking {
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(dataServices.measurementConsumersService),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService)
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
        }
      )
    }
    val requisition =
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

    val response =
      service.refuseRequisition(
        refuseRequisitionRequest {
          externalDataProviderId = requisition.externalDataProviderId
          externalRequisitionId = requisition.externalRequisitionId
          refusal = REFUSAL
        }
      )

    assertThat(response.state).isEqualTo(Requisition.State.REFUSED)
    assertThat(response.details.refusal).isEqualTo(REFUSAL)
    assertThat(response.parentMeasurement.state).isEqualTo(Measurement.State.FAILED)
    assertThat(response.updateTime.toInstant()).isGreaterThan(requisition.updateTime.toInstant())
    assertThat(response)
      .isEqualTo(
        service.getRequisition(
          getRequisitionRequest {
            externalMeasurementId = measurement.externalMeasurementId
            externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
            externalRequisitionId = requisition.externalRequisitionId
          }
        )
      )
  }

  @Test
  fun `refuseRequisition throws FAILED_PRECONDITION if Measurement in illegal state`() =
      runBlocking {
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(dataServices.measurementConsumersService),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService)
      )
    val requisition =
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

    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.refuseRequisition(
          refuseRequisitionRequest {
            externalDataProviderId = requisition.externalDataProviderId
            externalRequisitionId = requisition.externalRequisitionId
            refusal = REFUSAL
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `refuseRequisition throws NOT_FOUND if Requisition not found`() = runBlocking {
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    population.createMeasurement(
      dataServices.measurementsService,
      population.createMeasurementConsumer(dataServices.measurementConsumersService),
      "measurement",
      dataProvider
    )

    val nonExistantExternalRequisitionId = idGenerator.generateExternalId()
    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.refuseRequisition(
          refuseRequisitionRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalRequisitionId = nonExistantExternalRequisitionId.value
            refusal = REFUSAL
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `refuseRequisition throws INVALID_ARGUMENT when refusal justification not specified`() =
      runBlocking {
    val measurement =
      population.createMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(dataServices.measurementConsumersService),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService)
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
        }
      )
    }
    val requisition =
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

    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.refuseRequisition(
          refuseRequisitionRequest {
            externalDataProviderId = requisition.externalDataProviderId
            externalRequisitionId = requisition.externalRequisitionId
            refusal = refusal { message = "Refusal without justification" }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
