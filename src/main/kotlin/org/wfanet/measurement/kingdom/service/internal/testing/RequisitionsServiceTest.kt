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
import com.google.protobuf.kotlin.toByteStringUtf8
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase as AccountsCoroutineService
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase as CertificatesCoroutineService
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase as ComputationParticipantsCoroutineService
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as DataProvidersCoroutineService
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt.computedRequisitionParams
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt.directRequisitionParams
import org.wfanet.measurement.internal.kingdom.LiquidLegionsV2Params
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as MeasurementConsumersCoroutineService
import org.wfanet.measurement.internal.kingdom.MeasurementFailure
import org.wfanet.measurement.internal.kingdom.MeasurementKt.resultInfo
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase as MeasurementsCoroutineService
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetailsKt
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.RequisitionRefusal
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase as RequisitionsCoroutineService
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt.FilterKt.after
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementRequest
import org.wfanet.measurement.internal.kingdom.getRequisitionRequest
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.internal.kingdom.refuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.internal.kingdom.requisitionDetails
import org.wfanet.measurement.internal.kingdom.requisitionRefusal
import org.wfanet.measurement.internal.kingdom.setParticipantRequisitionParamsRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

private const val RANDOM_SEED = 1L
private const val NONCE_1 = 3127743798281582205L
private const val NONCE_2 = -7004399847946251733L
private val REQUISITION_ENCRYPTED_DATA = "foo".toByteStringUtf8()

private val REFUSAL = requisitionRefusal {
  justification = RequisitionRefusal.Justification.INSUFFICIENT_PRIVACY_BUDGET
  message = "MC wrote check that EDP couldn't cash"
}

@RunWith(JUnit4::class)
abstract class RequisitionsServiceTest<T : RequisitionsCoroutineService> {
  data class TestDataServices(
    val measurementConsumersService: MeasurementConsumersCoroutineService,
    val dataProvidersService: DataProvidersCoroutineService,
    val measurementsService: MeasurementsCoroutineService,
    val computationParticipantsService: ComputationParticipantsCoroutineService,
    val certificatesService: CertificatesCoroutineService,
    val accountsService: AccountsCoroutineService,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHIES)

  protected lateinit var dataServices: TestDataServices
    private set

  private lateinit var duchyCertificates: Map<String, Certificate>

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

    val externalDuchyIds = DUCHIES.map { it.externalDuchyId }
    duchyCertificates =
      externalDuchyIds.associateWith { externalDuchyId ->
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
  fun `streamRequisitions returns all requisitions for DataProvider`(): Unit = runBlocking {
    val measurementConsumer1 =
      population.createMeasurementConsumer(
        dataServices.measurementConsumersService,
        dataServices.accountsService,
      )
    val measurementConsumer2 =
      population.createMeasurementConsumer(
        dataServices.measurementConsumersService,
        dataServices.accountsService,
      )
    val dataProvider1 = population.createDataProvider(dataServices.dataProvidersService)
    val dataProvider2 = population.createDataProvider(dataServices.dataProvidersService)
    val measurement1 =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        measurementConsumer1,
        "measurement 1",
        dataProvider1,
        dataProvider2,
      )
    val measurement2 =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        measurementConsumer2,
        "other MC measurement",
        dataProvider1,
      )
    population.createLlv2Measurement(
      dataServices.measurementsService,
      measurementConsumer1,
      "other EDP measurement",
      dataProvider2,
    )

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter { externalDataProviderId = dataProvider1.externalDataProviderId }
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
          externalMeasurementId = measurement1.externalMeasurementId
          externalDataProviderId = dataProvider1.externalDataProviderId
        },
        requisition {
          externalMeasurementConsumerId = measurement2.externalMeasurementConsumerId
          externalMeasurementId = measurement2.externalMeasurementId
          externalDataProviderId = dataProvider1.externalDataProviderId
        },
      )
  }

  @Test
  fun `streamRequisitions respects state filter`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(
        dataServices.measurementConsumersService,
        dataServices.accountsService,
      )
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val dataProvider2 = population.createDataProvider(dataServices.dataProvidersService)
    population.createLlv2Measurement(
      dataServices.measurementsService,
      measurementConsumer,
      "pending params",
      dataProvider,
      dataProvider2,
    )
    val measurement1 =
      population.createDirectMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        "withdrawn",
        dataProvider,
        dataProvider2,
      )
    val measurement2 =
      population.createDirectMeasurement(
        dataServices.measurementsService,
        measurementConsumer,
        "unfulfilled",
        dataProvider,
      )
    dataServices.measurementsService.cancelMeasurement(
      cancelMeasurementRequest {
        externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
        externalMeasurementId = measurement1.externalMeasurementId
      }
    )

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalDataProviderId = dataProvider.externalDataProviderId
              states += Requisition.State.UNFULFILLED
              states += Requisition.State.WITHDRAWN
            }
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
          externalMeasurementId = measurement1.externalMeasurementId
          externalDataProviderId = dataProvider.externalDataProviderId
          state = Requisition.State.WITHDRAWN
        },
        requisition {
          externalMeasurementConsumerId = measurement2.externalMeasurementConsumerId
          externalMeasurementId = measurement2.externalMeasurementId
          externalDataProviderId = dataProvider.externalDataProviderId
          state = Requisition.State.UNFULFILLED
        },
      )
  }

  @Test
  fun `streamRequisitions returns all requisitions for measurement`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(
        dataServices.measurementConsumersService,
        dataServices.accountsService,
      )
    val dataProvider1 = population.createDataProvider(dataServices.dataProvidersService)
    val dataProvider2 = population.createDataProvider(dataServices.dataProvidersService)
    val measurement =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider1,
        dataProvider2,
      )
    population.createLlv2Measurement(
      dataServices.measurementsService,
      measurementConsumer,
      "measurement 2",
      dataProvider1,
    )

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
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
        },
      )
  }

  @Test
  fun `streamRequisitions respects updated_after`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(
        dataServices.measurementConsumersService,
        dataServices.accountsService,
      )
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val measurement1 =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider,
      )
    val measurement2 =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        measurementConsumer,
        "measurement 2",
        dataProvider,
      )
    population.createLlv2Measurement(
      dataServices.measurementsService,
      measurementConsumer,
      "measurement 3",
      population.createDataProvider(dataServices.dataProvidersService),
    )

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
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
      population.createMeasurementConsumer(
        dataServices.measurementConsumersService,
        dataServices.accountsService,
      )
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    population.createLlv2Measurement(
      dataServices.measurementsService,
      measurementConsumer,
      "measurement 1",
      dataProvider,
    )

    population.createLlv2Measurement(
      dataServices.measurementsService,
      measurementConsumer,
      "measurement 2",
      dataProvider,
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
      .containsExactly(requisition { externalDataProviderId = dataProvider.externalDataProviderId })
  }

  @Test
  fun `streamRequisitions can get one page at a time`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(
        dataServices.measurementConsumersService,
        dataServices.accountsService,
      )
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    population.createLlv2Measurement(
      dataServices.measurementsService,
      measurementConsumer,
      "measurement 1",
      dataProvider,
    )

    population.createLlv2Measurement(
      dataServices.measurementsService,
      measurementConsumer,
      "measurement 2",
      dataProvider,
    )

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
            limit = 2
          }
        )
        .toList()

    val requisitions2: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalDataProviderId = dataProvider.externalDataProviderId
              after = after {
                updateTime = requisitions[0].updateTime
                externalDataProviderId = requisitions[0].externalDataProviderId
                externalRequisitionId = requisitions[0].externalRequisitionId
              }
            }
            limit = 1
          }
        )
        .toList()

    assertThat(requisitions2).containsExactly(requisitions[1])
  }

  @Test
  fun `getRequisition returns expected requisition`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(
        dataServices.measurementConsumersService,
        dataServices.accountsService,
      )
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val dataProviderValue = dataProvider.toDataProviderValue()
    val providedMeasurementId = "measurement"
    val measurement =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        measurementConsumer,
        providedMeasurementId,
        mapOf(dataProvider.externalDataProviderId to dataProviderValue),
      )

    val externalDataProviderId = dataProvider.externalDataProviderId
    val listedRequisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
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
          this.externalDataProviderId = externalDataProviderId
          this.externalRequisitionId = externalRequisitionId
        }
      )

    val expectedRequisition = requisition {
      externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
      externalMeasurementId = measurement.externalMeasurementId
      this.externalDataProviderId = externalDataProviderId
      this.externalRequisitionId = externalRequisitionId
      externalComputationId = measurement.externalComputationId
      state = Requisition.State.PENDING_PARAMS
      details = requisitionDetails {
        dataProviderPublicKey = dataProviderValue.dataProviderPublicKey
        encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
        nonceHash = dataProviderValue.nonceHash

        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting these
        // fields.
        dataProviderPublicKeySignature = dataProviderValue.dataProviderPublicKeySignature
        dataProviderPublicKeySignatureAlgorithmOid =
          dataProviderValue.dataProviderPublicKeySignatureAlgorithmOid
      }
      dataProviderCertificate = dataProvider.certificate
      parentMeasurement = parentMeasurement {
        apiVersion = measurement.details.apiVersion
        externalMeasurementConsumerCertificateId =
          measurement.externalMeasurementConsumerCertificateId
        measurementSpec = measurement.details.measurementSpec
        measurementSpecSignature = measurement.details.measurementSpecSignature
        measurementSpecSignatureAlgorithmOid =
          measurement.details.measurementSpecSignatureAlgorithmOid
        state = Measurement.State.PENDING_REQUISITION_PARAMS
        protocolConfig = protocolConfig {
          liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
        }
        dataProvidersCount = 1
        createTime = measurement.createTime
      }
      etag = listedRequisition.etag
    }
    assertThat(requisition)
      .ignoringFields(Requisition.UPDATE_TIME_FIELD_NUMBER, Requisition.DUCHIES_FIELD_NUMBER)
      .isEqualTo(expectedRequisition)
    assertThat(requisition.duchiesMap)
      .containsExactly(
        Population.AGGREGATOR_DUCHY.externalDuchyId,
        Requisition.DuchyValue.getDefaultInstance(),
        Population.WORKER1_DUCHY.externalDuchyId,
        Requisition.DuchyValue.getDefaultInstance(),
        Population.WORKER2_DUCHY.externalDuchyId,
        Requisition.DuchyValue.getDefaultInstance(),
      )
    assertThat(requisition).isEqualTo(listedRequisition)
  }

  @Test
  fun `getRequisition returns expected direct requisition`() = runBlocking {
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val dataProviderValue = dataProvider.toDataProviderValue()
    val measurement =
      population.createDirectMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "direct_measurement",
        mapOf(dataProvider.externalDataProviderId to dataProviderValue),
      )

    val listedRequisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          }
        )
        .first()

    val requisition =
      service.getRequisition(
        getRequisitionRequest {
          externalDataProviderId = listedRequisition.externalDataProviderId
          externalRequisitionId = listedRequisition.externalRequisitionId
        }
      )

    val expectedRequisition = requisition {
      externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
      externalMeasurementId = measurement.externalMeasurementId
      externalDataProviderId = dataProvider.externalDataProviderId
      this.externalRequisitionId = listedRequisition.externalRequisitionId
      state = Requisition.State.UNFULFILLED
      details = requisitionDetails {
        dataProviderPublicKey = dataProviderValue.dataProviderPublicKey
        encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
        nonceHash = dataProviderValue.nonceHash

        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting these
        // fields.
        dataProviderPublicKeySignature = dataProviderValue.dataProviderPublicKeySignature
        dataProviderPublicKeySignatureAlgorithmOid =
          dataProviderValue.dataProviderPublicKeySignatureAlgorithmOid
      }
      dataProviderCertificate = dataProvider.certificate
      parentMeasurement = parentMeasurement {
        apiVersion = measurement.details.apiVersion
        externalMeasurementConsumerCertificateId =
          measurement.externalMeasurementConsumerCertificateId
        measurementSpec = measurement.details.measurementSpec
        measurementSpecSignature = measurement.details.measurementSpecSignature
        measurementSpecSignatureAlgorithmOid =
          measurement.details.measurementSpecSignatureAlgorithmOid
        state = Measurement.State.PENDING_REQUISITION_FULFILLMENT
        protocolConfig = protocolConfig { direct = ProtocolConfig.Direct.getDefaultInstance() }
        dataProvidersCount = 1
        createTime = measurement.createTime
      }
      etag = listedRequisition.etag
    }
    assertThat(requisition)
      .ignoringFields(Requisition.UPDATE_TIME_FIELD_NUMBER, Requisition.DUCHIES_FIELD_NUMBER)
      .isEqualTo(expectedRequisition)
    assertThat(requisition).isEqualTo(listedRequisition)
  }

  @Test
  fun `fulfillRequisition transitions Requisition state`() = runBlocking {
    val measurement =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService),
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
          liquidLegionsV2 = LiquidLegionsV2Params.getDefaultInstance()
        }
      )
    }
    val requisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          }
        )
        .first()

    val response =
      service.fulfillRequisition(
        fulfillRequisitionRequest {
          externalRequisitionId = requisition.externalRequisitionId
          nonce = NONCE_1
          computedParams = computedRequisitionParams {
            externalComputationId = measurement.externalComputationId
            externalFulfillingDuchyId = Population.WORKER1_DUCHY.externalDuchyId
          }
          etag = requisition.etag
        }
      )

    assertThat(response.state).isEqualTo(Requisition.State.FULFILLED)
    assertThat(response.externalFulfillingDuchyId)
      .isEqualTo(Population.WORKER1_DUCHY.externalDuchyId)
    assertThat(response.details.nonce).isEqualTo(NONCE_1)
    assertThat(response.updateTime.toInstant()).isGreaterThan(requisition.updateTime.toInstant())
    assertThat(response)
      .isEqualTo(
        service.getRequisition(
          getRequisitionRequest {
            externalDataProviderId = requisition.externalDataProviderId
            externalRequisitionId = requisition.externalRequisitionId
          }
        )
      )
  }

  @Test
  fun `fulfillRequisition transitions Measurement state when all others fulfilled`() = runBlocking {
    val measurement =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService),
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
          liquidLegionsV2 = LiquidLegionsV2Params.getDefaultInstance()
        }
      )
    }
    val requisitions =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          }
        )
        .toList()
    service.fulfillRequisition(
      fulfillRequisitionRequest {
        externalRequisitionId = requisitions[0].externalRequisitionId
        nonce = NONCE_1
        computedParams = computedRequisitionParams {
          externalComputationId = measurement.externalComputationId
          externalFulfillingDuchyId = Population.WORKER1_DUCHY.externalDuchyId
        }
        etag = requisitions[0].etag
      }
    )

    val response =
      service.fulfillRequisition(
        fulfillRequisitionRequest {
          externalRequisitionId = requisitions[1].externalRequisitionId
          nonce = NONCE_2
          computedParams = computedRequisitionParams {
            externalComputationId = measurement.externalComputationId
            externalFulfillingDuchyId = Population.WORKER1_DUCHY.externalDuchyId
          }
        }
      )

    assertThat(response.parentMeasurement.state)
      .isEqualTo(Measurement.State.PENDING_PARTICIPANT_CONFIRMATION)
    assertThat(response)
      .isEqualTo(
        service.getRequisition(
          getRequisitionRequest {
            externalDataProviderId = requisitions[1].externalDataProviderId
            externalRequisitionId = requisitions[1].externalRequisitionId
          }
        )
      )
  }

  @Test
  fun `fulfillRequisition transitions Measurement state when etags is not specified`() =
    runBlocking {
      val measurement =
        population.createLlv2Measurement(
          dataServices.measurementsService,
          population.createMeasurementConsumer(
            dataServices.measurementConsumersService,
            dataServices.accountsService,
          ),
          "measurement",
          population.createDataProvider(dataServices.dataProvidersService),
          population.createDataProvider(dataServices.dataProvidersService),
        )
      for (duchyCertificate in duchyCertificates.values) {
        dataServices.computationParticipantsService.setParticipantRequisitionParams(
          setParticipantRequisitionParamsRequest {
            externalComputationId = measurement.externalComputationId
            externalDuchyId = duchyCertificate.externalDuchyId
            externalDuchyCertificateId = duchyCertificate.externalCertificateId
            liquidLegionsV2 = LiquidLegionsV2Params.getDefaultInstance()
          }
        )
      }
      val requisition =
        service
          .streamRequisitions(
            streamRequisitionsRequest {
              filter = filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
            }
          )
          .first()

      val response =
        service.fulfillRequisition(
          fulfillRequisitionRequest {
            externalRequisitionId = requisition.externalRequisitionId
            nonce = NONCE_1
            computedParams = computedRequisitionParams {
              externalComputationId = measurement.externalComputationId
              externalFulfillingDuchyId = Population.WORKER1_DUCHY.externalDuchyId
            }
            // No etag specified
          }
        )

      assertThat(response.state).isEqualTo(Requisition.State.FULFILLED)
      assertThat(response.externalFulfillingDuchyId)
        .isEqualTo(Population.WORKER1_DUCHY.externalDuchyId)
      assertThat(response.details.nonce).isEqualTo(NONCE_1)
      assertThat(response.updateTime.toInstant()).isGreaterThan(requisition.updateTime.toInstant())
      assertThat(response)
        .isEqualTo(
          service.getRequisition(
            getRequisitionRequest {
              externalDataProviderId = requisition.externalDataProviderId
              externalRequisitionId = requisition.externalRequisitionId
            }
          )
        )
    }

  @Test
  fun `fulfillRequisition persists fulfillment context`() = runBlocking {
    val measurement =
      population.createDirectMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "direct_measurement",
        population.createDataProvider(dataServices.dataProvidersService),
      )
    val requisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          }
        )
        .first()
    val fulfillmentContext = RequisitionDetailsKt.fulfillmentContext { buildLabel = "1.2.3" }

    val fulfilledRequisition =
      service.fulfillRequisition(
        fulfillRequisitionRequest {
          externalRequisitionId = requisition.externalRequisitionId
          nonce = NONCE_1
          directParams = directRequisitionParams {
            externalDataProviderId = requisition.externalDataProviderId
            encryptedData = REQUISITION_ENCRYPTED_DATA
            externalCertificateId = requisition.details.externalCertificateId
            apiVersion = PUBLIC_API_VERSION
          }
          this.fulfillmentContext = fulfillmentContext
        }
      )

    assertThat(fulfilledRequisition.details.fulfillmentContext).isEqualTo(fulfillmentContext)
    assertThat(fulfilledRequisition)
      .isEqualTo(
        service.getRequisition(
          getRequisitionRequest {
            externalDataProviderId = fulfilledRequisition.externalDataProviderId
            externalRequisitionId = fulfilledRequisition.externalRequisitionId
          }
        )
      )
  }

  @Test
  fun `fulfillRequisition throws ABORTED if etags mismatch`() = runBlocking {
    val measurement =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService),
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
          liquidLegionsV2 = LiquidLegionsV2Params.getDefaultInstance()
        }
      )
    }
    val requisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
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
            externalRequisitionId = requisition.externalRequisitionId
            nonce = NONCE_1
            computedParams = computedRequisitionParams {
              externalComputationId = measurement.externalComputationId
              externalFulfillingDuchyId = Population.WORKER1_DUCHY.externalDuchyId
            }
            etag = "random_etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `fulfillRequisition throws NOT_FOUND if Requisition not found`() = runBlocking {
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val measurement =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "measurement",
        dataProvider,
      )

    val nonExistentExternalRequisitionId = idGenerator.generateExternalId()
    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.fulfillRequisition(
          fulfillRequisitionRequest {
            externalRequisitionId = nonExistentExternalRequisitionId.value
            nonce = NONCE_1
            computedParams = computedRequisitionParams {
              externalComputationId = measurement.externalComputationId
              externalFulfillingDuchyId = Population.WORKER1_DUCHY.externalDuchyId
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `fulfillRequisition throws FAILED_PRECONDITION if Duchy not found`() = runBlocking {
    val dataProvider = population.createDataProvider(dataServices.dataProvidersService)
    val measurement =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "measurement",
        dataProvider,
      )
    val requisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          }
        )
        .first()

    val nonExistentExternalDuchyId = "Chalced"
    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.fulfillRequisition(
          fulfillRequisitionRequest {
            externalRequisitionId = requisition.externalRequisitionId
            nonce = NONCE_1
            computedParams = computedRequisitionParams {
              externalComputationId = measurement.externalComputationId
              externalFulfillingDuchyId = nonExistentExternalDuchyId
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `fulfillRequisition throws FAILED_PRECONDITION if Measurement in illegal state`() =
    runBlocking {
      val measurement =
        population.createLlv2Measurement(
          dataServices.measurementsService,
          population.createMeasurementConsumer(
            dataServices.measurementConsumersService,
            dataServices.accountsService,
          ),
          "measurement",
          population.createDataProvider(dataServices.dataProvidersService),
          population.createDataProvider(dataServices.dataProvidersService),
        )
      val requisition =
        service
          .streamRequisitions(
            streamRequisitionsRequest {
              filter = filter {
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
              externalRequisitionId = requisition.externalRequisitionId
              nonce = NONCE_1
              computedParams = computedRequisitionParams {
                externalComputationId = measurement.externalComputationId
                externalFulfillingDuchyId = Population.WORKER1_DUCHY.externalDuchyId
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `fulfillRequisition throws INVALID_ARGUMENT when signature not specified`() = runBlocking {
    val measurement =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService),
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
          liquidLegionsV2 = LiquidLegionsV2Params.getDefaultInstance()
        }
      )
    }
    val requisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
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
            externalRequisitionId = requisition.externalRequisitionId
            computedParams = computedRequisitionParams {
              externalComputationId = measurement.externalComputationId
              externalFulfillingDuchyId = Population.WORKER1_DUCHY.externalDuchyId
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `direct fulfillRequisition transitions Requisition state`() = runBlocking {
    val measurement =
      population.createDirectMeasurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "direct_measurement",
        population.createDataProvider(dataServices.dataProvidersService),
      )

    val requisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          }
        )
        .first()

    val fulfilledRequisition =
      service.fulfillRequisition(
        fulfillRequisitionRequest {
          externalRequisitionId = requisition.externalRequisitionId
          nonce = NONCE_1
          directParams = directRequisitionParams {
            externalDataProviderId = requisition.externalDataProviderId
            encryptedData = REQUISITION_ENCRYPTED_DATA
            externalCertificateId = requisition.details.externalCertificateId
            apiVersion = PUBLIC_API_VERSION
          }
        }
      )

    assertThat(fulfilledRequisition.state).isEqualTo(Requisition.State.FULFILLED)
    assertThat(fulfilledRequisition.details.nonce).isEqualTo(NONCE_1)
    assertThat(fulfilledRequisition.details.encryptedData).isEqualTo(REQUISITION_ENCRYPTED_DATA)
    assertThat(fulfilledRequisition.updateTime.toInstant())
      .isGreaterThan(requisition.updateTime.toInstant())
    val expectedRequisition =
      service.getRequisition(
        getRequisitionRequest {
          externalDataProviderId = requisition.externalDataProviderId
          externalRequisitionId = requisition.externalRequisitionId
        }
      )
    assertThat(fulfilledRequisition).isEqualTo(expectedRequisition)
  }

  @Test
  fun `direct fulfillRequisition transitions Measurement state when all others fulfilled`() =
    runBlocking {
      val measurement =
        population.createDirectMeasurement(
          dataServices.measurementsService,
          population.createMeasurementConsumer(
            dataServices.measurementConsumersService,
            dataServices.accountsService,
          ),
          "direct_measurement",
          population.createDataProvider(dataServices.dataProvidersService),
          population.createDataProvider(dataServices.dataProvidersService),
        )

      val requisitions =
        service
          .streamRequisitions(
            streamRequisitionsRequest {
              filter = filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
            }
          )
          .toList()

      service.fulfillRequisition(
        fulfillRequisitionRequest {
          externalRequisitionId = requisitions[0].externalRequisitionId
          nonce = NONCE_1
          directParams = directRequisitionParams {
            externalDataProviderId = requisitions[0].externalDataProviderId
            encryptedData = REQUISITION_ENCRYPTED_DATA
            externalCertificateId = requisitions[0].details.externalCertificateId
            apiVersion = PUBLIC_API_VERSION
          }
          etag = requisitions[0].etag
        }
      )
      val fulfilledRequisition =
        service.fulfillRequisition(
          fulfillRequisitionRequest {
            externalRequisitionId = requisitions[1].externalRequisitionId
            nonce = NONCE_1
            directParams = directRequisitionParams {
              externalDataProviderId = requisitions[1].externalDataProviderId
              encryptedData = REQUISITION_ENCRYPTED_DATA
              externalCertificateId = requisitions[1].details.externalCertificateId
              apiVersion = PUBLIC_API_VERSION
            }
          }
        )

      assertThat(fulfilledRequisition.parentMeasurement.state)
        .isEqualTo(Measurement.State.SUCCEEDED)
      assertThat(fulfilledRequisition)
        .isEqualTo(
          service.getRequisition(
            getRequisitionRequest {
              externalDataProviderId = requisitions[1].externalDataProviderId
              externalRequisitionId = requisitions[1].externalRequisitionId
            }
          )
        )
    }

  @Test
  fun `direct fulfillRequisition sets measurement result when all requisitions fulfilled`(): Unit =
    runBlocking {
      val measurement =
        population.createDirectMeasurement(
          dataServices.measurementsService,
          population.createMeasurementConsumer(
            dataServices.measurementConsumersService,
            dataServices.accountsService,
          ),
          "direct_measurement",
          population.createDataProvider(dataServices.dataProvidersService),
          population.createDataProvider(dataServices.dataProvidersService),
        )

      val requisitions =
        service
          .streamRequisitions(
            streamRequisitionsRequest {
              filter = filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
            }
          )
          .toList()

      service.fulfillRequisition(
        fulfillRequisitionRequest {
          externalRequisitionId = requisitions[0].externalRequisitionId
          nonce = NONCE_1
          directParams = directRequisitionParams {
            externalDataProviderId = requisitions[0].externalDataProviderId
            encryptedData = REQUISITION_ENCRYPTED_DATA
            externalCertificateId = requisitions[0].details.externalCertificateId
            apiVersion = PUBLIC_API_VERSION
          }
        }
      )

      service.fulfillRequisition(
        fulfillRequisitionRequest {
          externalRequisitionId = requisitions[1].externalRequisitionId
          nonce = NONCE_1
          directParams = directRequisitionParams {
            externalDataProviderId = requisitions[1].externalDataProviderId
            encryptedData = REQUISITION_ENCRYPTED_DATA
            externalCertificateId = requisitions[1].details.externalCertificateId
            apiVersion = PUBLIC_API_VERSION
          }
        }
      )

      val succeededMeasurement =
        dataServices.measurementsService.getMeasurement(
          getMeasurementRequest {
            externalMeasurementId = measurement.externalMeasurementId
            externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          }
        )

      assertThat(succeededMeasurement.resultsList.size).isEqualTo(2)
      assertThat(succeededMeasurement.resultsList)
        .ignoringRepeatedFieldOrder()
        .containsAtLeast(
          resultInfo {
            externalDataProviderId = requisitions[0].externalDataProviderId
            externalCertificateId = requisitions[0].dataProviderCertificate.externalCertificateId
            encryptedResult = REQUISITION_ENCRYPTED_DATA
            apiVersion = PUBLIC_API_VERSION
          },
          resultInfo {
            externalDataProviderId = requisitions[1].externalDataProviderId
            externalCertificateId = requisitions[1].dataProviderCertificate.externalCertificateId
            encryptedResult = REQUISITION_ENCRYPTED_DATA
            apiVersion = PUBLIC_API_VERSION
          },
        )
    }

  @Test
  fun `direct fulfillRequisition throws NOT_FOUND if requisition not found`() = runBlocking {
    val provider = population.createDataProvider(dataServices.dataProvidersService)
    population.createDirectMeasurement(
      dataServices.measurementsService,
      population.createMeasurementConsumer(
        dataServices.measurementConsumersService,
        dataServices.accountsService,
      ),
      "direct_measurement",
      provider,
    )

    val nonExistentExternalRequisitionId = idGenerator.generateExternalId()
    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.fulfillRequisition(
          fulfillRequisitionRequest {
            externalRequisitionId = nonExistentExternalRequisitionId.value
            nonce = NONCE_1
            directParams = directRequisitionParams {
              externalDataProviderId = provider.externalDataProviderId
              encryptedData = REQUISITION_ENCRYPTED_DATA
              externalCertificateId = idGenerator.generateExternalId().value
              apiVersion = PUBLIC_API_VERSION
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `refuseRequisition transitions Requisition and Measurement states`() = runBlocking {
    val measurement =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService),
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
          liquidLegionsV2 = LiquidLegionsV2Params.getDefaultInstance()
        }
      )
    }
    val requisitions =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          }
        )
        .toList()
    val requisition: Requisition = requisitions.first()

    val response: Requisition =
      service.refuseRequisition(
        refuseRequisitionRequest {
          externalDataProviderId = requisition.externalDataProviderId
          externalRequisitionId = requisition.externalRequisitionId
          refusal = REFUSAL
          etag = requisition.etag
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
            externalDataProviderId = requisition.externalDataProviderId
            externalRequisitionId = requisition.externalRequisitionId
          }
        )
      )
    val otherRequisition: Requisition = requisitions[1]
    assertThat(
        service
          .getRequisition(
            getRequisitionRequest {
              externalDataProviderId = otherRequisition.externalDataProviderId
              externalRequisitionId = otherRequisition.externalRequisitionId
            }
          )
          .state
      )
      .isEqualTo(Requisition.State.WITHDRAWN)
    val updatedMeasurement =
      dataServices.measurementsService.getMeasurement(
        getMeasurementRequest {
          externalMeasurementId = measurement.externalMeasurementId
          externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
        }
      )
    assertThat(updatedMeasurement.state).isEqualTo(Measurement.State.FAILED)
    assertThat(updatedMeasurement.details.failure.reason)
      .isEqualTo(MeasurementFailure.Reason.REQUISITION_REFUSED)
  }

  @Test
  fun `refuseRequisition transitions Measurement state when etags is not specified`() =
    runBlocking {
      val measurement =
        population.createLlv2Measurement(
          dataServices.measurementsService,
          population.createMeasurementConsumer(
            dataServices.measurementConsumersService,
            dataServices.accountsService,
          ),
          "measurement",
          population.createDataProvider(dataServices.dataProvidersService),
          population.createDataProvider(dataServices.dataProvidersService),
        )
      for (duchyCertificate in duchyCertificates.values) {
        dataServices.computationParticipantsService.setParticipantRequisitionParams(
          setParticipantRequisitionParamsRequest {
            externalComputationId = measurement.externalComputationId
            externalDuchyId = duchyCertificate.externalDuchyId
            externalDuchyCertificateId = duchyCertificate.externalCertificateId
            liquidLegionsV2 = LiquidLegionsV2Params.getDefaultInstance()
          }
        )
      }
      val requisitions =
        service
          .streamRequisitions(
            streamRequisitionsRequest {
              filter = filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
            }
          )
          .toList()
      val requisition: Requisition = requisitions.first()

      val response: Requisition =
        service.refuseRequisition(
          refuseRequisitionRequest {
            externalDataProviderId = requisition.externalDataProviderId
            externalRequisitionId = requisition.externalRequisitionId
            refusal = REFUSAL
            // No etag
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
              externalDataProviderId = requisition.externalDataProviderId
              externalRequisitionId = requisition.externalRequisitionId
            }
          )
        )
      val otherRequisition: Requisition = requisitions[1]
      assertThat(
          service
            .getRequisition(
              getRequisitionRequest {
                externalDataProviderId = otherRequisition.externalDataProviderId
                externalRequisitionId = otherRequisition.externalRequisitionId
              }
            )
            .state
        )
        .isEqualTo(Requisition.State.WITHDRAWN)
      val updatedMeasurement =
        dataServices.measurementsService.getMeasurement(
          getMeasurementRequest {
            externalMeasurementId = measurement.externalMeasurementId
            externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          }
        )
      assertThat(updatedMeasurement.state).isEqualTo(Measurement.State.FAILED)
      assertThat(updatedMeasurement.details.failure.reason)
        .isEqualTo(MeasurementFailure.Reason.REQUISITION_REFUSED)
    }

  @Test
  fun `refuseRequisition throws ABORTED if etags mismatch`() = runBlocking {
    val measurement =
      population.createLlv2Measurement(
        dataServices.measurementsService,
        population.createMeasurementConsumer(
          dataServices.measurementConsumersService,
          dataServices.accountsService,
        ),
        "measurement",
        population.createDataProvider(dataServices.dataProvidersService),
        population.createDataProvider(dataServices.dataProvidersService),
      )
    for (duchyCertificate in duchyCertificates.values) {
      dataServices.computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
          liquidLegionsV2 = LiquidLegionsV2Params.getDefaultInstance()
        }
      )
    }
    val requisitions =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          }
        )
        .toList()
    val requisition: Requisition = requisitions.first()

    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.refuseRequisition(
          refuseRequisitionRequest {
            externalDataProviderId = requisition.externalDataProviderId
            externalRequisitionId = requisition.externalRequisitionId
            refusal = REFUSAL

            etag = "random_etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `refuseRequisition throws FAILED_PRECONDITION if Measurement in illegal state`() =
    runBlocking {
      val measurement =
        population.createLlv2Measurement(
          dataServices.measurementsService,
          population.createMeasurementConsumer(
            dataServices.measurementConsumersService,
            dataServices.accountsService,
          ),
          "measurement",
          population.createDataProvider(dataServices.dataProvidersService),
          population.createDataProvider(dataServices.dataProvidersService),
        )
      val requisition =
        service
          .streamRequisitions(
            streamRequisitionsRequest {
              filter = filter {
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
    population.createLlv2Measurement(
      dataServices.measurementsService,
      population.createMeasurementConsumer(
        dataServices.measurementConsumersService,
        dataServices.accountsService,
      ),
      "measurement",
      dataProvider,
    )

    val nonExistentExternalRequisitionId = idGenerator.generateExternalId()
    val exception =
      assertFailsWith(StatusRuntimeException::class) {
        service.refuseRequisition(
          refuseRequisitionRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalRequisitionId = nonExistentExternalRequisitionId.value
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
        population.createLlv2Measurement(
          dataServices.measurementsService,
          population.createMeasurementConsumer(
            dataServices.measurementConsumersService,
            dataServices.accountsService,
          ),
          "measurement",
          population.createDataProvider(dataServices.dataProvidersService),
          population.createDataProvider(dataServices.dataProvidersService),
        )
      for (duchyCertificate in duchyCertificates.values) {
        dataServices.computationParticipantsService.setParticipantRequisitionParams(
          setParticipantRequisitionParamsRequest {
            externalComputationId = measurement.externalComputationId
            externalDuchyId = duchyCertificate.externalDuchyId
            externalDuchyCertificateId = duchyCertificate.externalCertificateId
            liquidLegionsV2 = LiquidLegionsV2Params.getDefaultInstance()
          }
        )
      }
      val requisition =
        service
          .streamRequisitions(
            streamRequisitionsRequest {
              filter = filter {
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
              refusal = requisitionRefusal { message = "Refusal without justification" }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  companion object {
    @BeforeClass
    @JvmStatic
    fun initConfig() {
      Llv2ProtocolConfig.setForTest(
        ProtocolConfig.LiquidLegionsV2.getDefaultInstance(),
        DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance(),
        setOf(Population.AGGREGATOR_DUCHY.externalDuchyId),
        2,
      )
    }

    private val PUBLIC_API_VERSION = Version.V2_ALPHA.string
  }
}
