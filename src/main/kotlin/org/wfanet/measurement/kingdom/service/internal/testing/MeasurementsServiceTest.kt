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
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.random.Random
import kotlin.test.assertFailsWith
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
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.CancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DeleteMeasurementRequest
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.resultInfo
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt.details
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt
import org.wfanet.measurement.internal.kingdom.batchCancelMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.batchDeleteMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createMeasurementRequest
import org.wfanet.measurement.internal.kingdom.deleteMeasurementRequest
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.getMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementRequest
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementKey
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.internal.kingdom.revokeCertificateRequest
import org.wfanet.measurement.internal.kingdom.setMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

private const val RANDOM_SEED = 1
private const val API_VERSION = "v2alpha"
private const val PROVIDED_MEASUREMENT_ID = "ProvidedMeasurementId"
private const val MAX_BATCH_DELETE = 1000
private const val MAX_BATCH_CANCEL = 1000

private val MEASUREMENT = measurement {
  providedMeasurementId = PROVIDED_MEASUREMENT_ID
  details =
    MeasurementKt.details {
      apiVersion = API_VERSION
      measurementSpec = ByteString.copyFromUtf8("MeasurementSpec")
      measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
      measurementSpecSignatureAlgorithmOid = "2.9999"
      duchyProtocolConfig = duchyProtocolConfig {
        liquidLegionsV2 = DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance()
      }
      protocolConfig = protocolConfig {
        liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
      }
    }
}

private val REACH_ONLY_MEASUREMENT =
  MEASUREMENT.copy {
    details =
      details.copy {
        duchyProtocolConfig = duchyProtocolConfig {
          reachOnlyLiquidLegionsV2 = DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance()
        }
        protocolConfig = protocolConfig {
          reachOnlyLiquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
        }
      }
  }

private val INVALID_WORKER_DUCHY =
  DuchyIds.Entry(4, "worker3", Instant.now().minusSeconds(100L)..Instant.now().minusSeconds(50L))

@RunWith(JUnit4::class)
abstract class MeasurementsServiceTest<T : MeasurementsCoroutineImplBase> {

  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHIES + INVALID_WORKER_DUCHY)

  protected data class Services<T>(
    val measurementsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val certificatesService: CertificatesGrpcKt.CertificatesCoroutineImplBase,
    val accountsService: AccountsCoroutineImplBase,
    val requisitionsService: RequisitionsCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var measurementsService: T
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var certificatesService: CertificatesGrpcKt.CertificatesCoroutineImplBase
    private set

  protected lateinit var accountsService: AccountsCoroutineImplBase
    private set

  protected lateinit var requisitionsService: RequisitionsCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    measurementConsumersService = services.measurementConsumersService
    measurementsService = services.measurementsService
    dataProvidersService = services.dataProvidersService
    certificatesService = services.certificatesService
    accountsService = services.accountsService
    requisitionsService = services.requisitionsService
  }

  @Test
  fun `getMeasurementByComputationId fails for missing Measurement`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.getMeasurementByComputationId(
          getMeasurementByComputationIdRequest { externalComputationId = 1L }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Measurement not found")
  }

  @Test
  fun `createMeasurement fails for missing data provider`() = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.createMeasurement(
          createMeasurementRequest {
            measurement =
              MEASUREMENT.copy {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
                dataProviders[404L] = dataProvider.toDataProviderValue()
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `createMeasurement fails for missing measurement consumer`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.createMeasurement(
          createMeasurementRequest {
            measurement =
              MEASUREMENT.copy {
                externalMeasurementConsumerId = 404L
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
                dataProviders[dataProvider.externalDataProviderId] =
                  dataProvider.toDataProviderValue()
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `createMeasurement fails for revoked Measurement Consumer Certificate`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    certificatesService.revokeCertificate(
      revokeCertificateRequest {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalCertificateId = measurementConsumer.certificate.externalCertificateId
        revocationState = Certificate.RevocationState.REVOKED
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.createMeasurement(
          createMeasurementRequest {
            measurement =
              MEASUREMENT.copy {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `createMeasurement fails when current time is before mc certificate is valid`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(
          measurementConsumersService,
          accountsService,
          notValidBefore = clock.instant().plus(1L, ChronoUnit.DAYS),
          notValidAfter = clock.instant().plus(10L, ChronoUnit.DAYS)
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          measurementsService.createMeasurement(
            createMeasurementRequest {
              measurement =
                MEASUREMENT.copy {
                  externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                  externalMeasurementConsumerCertificateId =
                    measurementConsumer.certificate.externalCertificateId
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception).hasMessageThat().contains("Certificate is invalid")
    }

  @Test
  fun `createMeasurement fails when current time is after mc certificate is valid`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(
        measurementConsumersService,
        accountsService,
        notValidBefore = clock.instant().minus(10L, ChronoUnit.DAYS),
        notValidAfter = clock.instant().minus(1L, ChronoUnit.DAYS)
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.createMeasurement(
          createMeasurementRequest {
            measurement =
              MEASUREMENT.copy {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `createMeasurement fails for revoked Data Provider Certificate`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    certificatesService.revokeCertificate(
      revokeCertificateRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalCertificateId = dataProvider.certificate.externalCertificateId
        revocationState = Certificate.RevocationState.REVOKED
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.createMeasurement(
          createMeasurementRequest {
            measurement =
              MEASUREMENT.copy {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
                dataProviders[dataProvider.externalDataProviderId] =
                  dataProvider.toDataProviderValue()
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `createMeasurement fails when current time is before edp certificate is valid`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider =
        population.createDataProvider(
          dataProvidersService,
          notValidBefore = clock.instant().plus(1L, ChronoUnit.DAYS),
          notValidAfter = clock.instant().plus(10L, ChronoUnit.DAYS)
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          measurementsService.createMeasurement(
            createMeasurementRequest {
              measurement =
                MEASUREMENT.copy {
                  externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                  externalMeasurementConsumerCertificateId =
                    measurementConsumer.certificate.externalCertificateId
                  dataProviders[dataProvider.externalDataProviderId] =
                    dataProvider.toDataProviderValue()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception).hasMessageThat().contains("Certificate is invalid")
    }

  @Test
  fun `createMeasurement fails when current time is after edp certificate is valid`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider =
        population.createDataProvider(
          dataProvidersService,
          notValidBefore = clock.instant().minus(10L, ChronoUnit.DAYS),
          notValidAfter = clock.instant().minus(1L, ChronoUnit.DAYS)
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          measurementsService.createMeasurement(
            createMeasurementRequest {
              measurement =
                MEASUREMENT.copy {
                  externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                  externalMeasurementConsumerCertificateId =
                    measurementConsumer.certificate.externalCertificateId
                  dataProviders[dataProvider.externalDataProviderId] =
                    dataProvider.toDataProviderValue()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception).hasMessageThat().contains("Certificate is invalid")
    }

  @Test
  fun `createMeasurement for duchy measurement succeeds`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      MEASUREMENT.copy {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalMeasurementConsumerCertificateId =
          measurementConsumer.certificate.externalCertificateId
        dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
      }

    val createdMeasurement =
      measurementsService.createMeasurement(
        createMeasurementRequest { this.measurement = measurement }
      )
    assertThat(createdMeasurement.externalMeasurementId).isNotEqualTo(0L)
    assertThat(createdMeasurement.externalComputationId).isNotEqualTo(0L)
    assertThat(createdMeasurement.createTime.seconds).isGreaterThan(0L)
    assertThat(createdMeasurement.updateTime).isEqualTo(createdMeasurement.createTime)
    assertThat(createdMeasurement)
      .ignoringFields(
        Measurement.EXTERNAL_MEASUREMENT_ID_FIELD_NUMBER,
        Measurement.EXTERNAL_COMPUTATION_ID_FIELD_NUMBER,
        Measurement.CREATE_TIME_FIELD_NUMBER,
        Measurement.UPDATE_TIME_FIELD_NUMBER,
        Measurement.ETAG_FIELD_NUMBER,
      )
      .isEqualTo(measurement.copy { state = Measurement.State.PENDING_REQUISITION_PARAMS })
  }

  @Test
  fun `createMeasurement for duchy REACH measurement succeeds`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      REACH_ONLY_MEASUREMENT.copy {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalMeasurementConsumerCertificateId =
          measurementConsumer.certificate.externalCertificateId
        dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
      }

    val createdMeasurement =
      measurementsService.createMeasurement(
        createMeasurementRequest { this.measurement = measurement }
      )
    assertThat(createdMeasurement.externalMeasurementId).isNotEqualTo(0L)
    assertThat(createdMeasurement.externalComputationId).isNotEqualTo(0L)
    assertThat(createdMeasurement.createTime.seconds).isGreaterThan(0L)
    assertThat(createdMeasurement.updateTime).isEqualTo(createdMeasurement.createTime)
    assertThat(createdMeasurement)
      .ignoringFields(
        Measurement.EXTERNAL_MEASUREMENT_ID_FIELD_NUMBER,
        Measurement.EXTERNAL_COMPUTATION_ID_FIELD_NUMBER,
        Measurement.CREATE_TIME_FIELD_NUMBER,
        Measurement.UPDATE_TIME_FIELD_NUMBER,
        Measurement.ETAG_FIELD_NUMBER,
      )
      .isEqualTo(measurement.copy { state = Measurement.State.PENDING_REQUISITION_PARAMS })
  }

  @Test
  fun `createMeasurement for duchy measurement contains required duchies and the aggregator`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider = population.createDataProvider(dataProvidersService)

      val measurement =
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
          dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
        }
      val createdMeasurement =
        measurementsService.createMeasurement(
          createMeasurementRequest { this.measurement = measurement }
        )

      val retrievedMeasurement =
        measurementsService.getMeasurementByComputationId(
          getMeasurementByComputationIdRequest {
            externalComputationId = createdMeasurement.externalComputationId
          }
        )

      assertThat(retrievedMeasurement.computationParticipantsCount).isEqualTo(3)
      assertThat(retrievedMeasurement.computationParticipantsList[0].externalDuchyId)
        .isEqualTo(DUCHIES[0].externalDuchyId)
      assertThat(retrievedMeasurement.computationParticipantsList[1].externalDuchyId)
        .isEqualTo(DUCHIES[1].externalDuchyId)
      assertThat(retrievedMeasurement.computationParticipantsList[2].externalDuchyId)
        .isEqualTo(DUCHIES[2].externalDuchyId)
    }

  @Test
  fun `createMeasurement for duchy measurement fails for inactive required duchy`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider =
      population.createDataProvider(
        dataProvidersService,
        customize = { requiredExternalDuchyIds += INVALID_WORKER_DUCHY.externalDuchyId }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.createMeasurement(
          createMeasurementRequest {
            measurement =
              MEASUREMENT.copy {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
                dataProviders[dataProvider.externalDataProviderId] =
                  dataProvider.toDataProviderValue()
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Inactive required duchy.")
  }

  @Test
  fun `createMeasurement for duchy measurement creates computation participants`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      MEASUREMENT.copy {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalMeasurementConsumerCertificateId =
          measurementConsumer.certificate.externalCertificateId
        dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
      }

    val createdMeasurement =
      measurementsService.createMeasurement(
        createMeasurementRequest { this.measurement = measurement }
      )
    val measurements =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter {
              externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
            }
            measurementView = Measurement.View.COMPUTATION
          }
        )
        .toList()

    assertThat(measurements[0].externalComputationId).isNotEqualTo(0L)
    assertThat(measurements[0].computationParticipantsCount).isEqualTo(3)
  }

  @Test
  fun `createMeasurement for duchy measurement creates requisitions with PENDING_PARAMS state`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider = population.createDataProvider(dataProvidersService)

      val measurement =
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
          dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
        }

      val createdMeasurement =
        measurementsService.createMeasurement(
          createMeasurementRequest { this.measurement = measurement }
        )
      val requisitions: List<Requisition> =
        requisitionsService
          .streamRequisitions(
            streamRequisitionsRequest {
              filter =
                StreamRequisitionsRequestKt.filter {
                  externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
                }
            }
          )
          .toList()

      requisitions.forEach { assertThat(it.state).isEqualTo(Requisition.State.PENDING_PARAMS) }
    }

  @Test
  fun `createMeasurement for direct measurement succeeds`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      MEASUREMENT.copy {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalMeasurementConsumerCertificateId =
          measurementConsumer.certificate.externalCertificateId
        dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
        details =
          details.copy {
            clearDuchyProtocolConfig()
            protocolConfig = protocolConfig { direct = ProtocolConfig.Direct.getDefaultInstance() }
          }
      }

    val createdMeasurement =
      measurementsService.createMeasurement(
        createMeasurementRequest { this.measurement = measurement }
      )

    assertThat(createdMeasurement.externalMeasurementId).isNotEqualTo(0L)
    assertThat(createdMeasurement.externalComputationId).isEqualTo(0L)
    assertThat(createdMeasurement.createTime.seconds).isGreaterThan(0L)
    assertThat(createdMeasurement.updateTime).isEqualTo(createdMeasurement.createTime)
    assertThat(createdMeasurement)
      .ignoringFields(
        Measurement.EXTERNAL_MEASUREMENT_ID_FIELD_NUMBER,
        Measurement.EXTERNAL_COMPUTATION_ID_FIELD_NUMBER,
        Measurement.CREATE_TIME_FIELD_NUMBER,
        Measurement.UPDATE_TIME_FIELD_NUMBER,
        Measurement.ETAG_FIELD_NUMBER,
      )
      .isEqualTo(measurement.copy { state = Measurement.State.PENDING_REQUISITION_FULFILLMENT })
  }

  @Test
  fun `createMeasurement for direct measurement doesn't set computation id`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      MEASUREMENT.copy {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalMeasurementConsumerCertificateId =
          measurementConsumer.certificate.externalCertificateId
        dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
        details =
          details.copy {
            clearDuchyProtocolConfig()
            protocolConfig = protocolConfig { direct = ProtocolConfig.Direct.getDefaultInstance() }
          }
      }

    val createdMeasurement =
      measurementsService.createMeasurement(
        createMeasurementRequest { this.measurement = measurement }
      )
    val retrievedMeasurement =
      measurementsService.getMeasurement(
        getMeasurementRequest {
          externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
          externalMeasurementId = createdMeasurement.externalMeasurementId
        }
      )

    assertThat(retrievedMeasurement.externalComputationId).isEqualTo(0L)
  }

  @Test
  fun `createMeasurement for direct measurement creates requisitions with UNFUlFILLED state`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider = population.createDataProvider(dataProvidersService)

      val measurement =
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
          dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
          details =
            details.copy {
              clearDuchyProtocolConfig()
              protocolConfig = protocolConfig {
                direct = ProtocolConfig.Direct.getDefaultInstance()
              }
            }
        }

      val createdMeasurement =
        measurementsService.createMeasurement(
          createMeasurementRequest { this.measurement = measurement }
        )
      val requisitions: List<Requisition> =
        requisitionsService
          .streamRequisitions(
            streamRequisitionsRequest {
              filter =
                StreamRequisitionsRequestKt.filter {
                  externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
                }
            }
          )
          .toList()

      requisitions.forEach { assertThat(it.state).isEqualTo(Requisition.State.UNFULFILLED) }
    }

  @Test
  fun `createMeasurement returns existing measurement for the same request ID`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val request = createMeasurementRequest {
      measurement =
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
        }
      requestId = "request-id"
    }
    val existingMeasurement = measurementsService.createMeasurement(request)

    val measurement = measurementsService.createMeasurement(request)

    assertThat(measurement).isEqualTo(existingMeasurement)
  }

  @Test
  fun `createMeasurement returns new measurement for different request ID`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val request = createMeasurementRequest {
      measurement =
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
        }
      requestId = "request-id"
    }
    val existingMeasurement = measurementsService.createMeasurement(request)

    val measurement =
      measurementsService.createMeasurement(request.copy { requestId = request.requestId + 2 })

    assertThat(measurement.externalMeasurementId)
      .isNotEqualTo(existingMeasurement.externalMeasurementId)
  }

  @Test
  fun `getMeasurementByComputationId returns created measurement`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val createdMeasurement =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val measurement =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          externalComputationId = createdMeasurement.externalComputationId
        }
      )

    assertThat(measurement)
      .ignoringFields(
        Measurement.REQUISITIONS_FIELD_NUMBER,
        Measurement.COMPUTATION_PARTICIPANTS_FIELD_NUMBER
      )
      .isEqualTo(createdMeasurement)
  }

  @Test
  fun `getMeasurement succeeds`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val createdMeasurement =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val measurement =
      measurementsService.getMeasurement(
        getMeasurementRequest {
          externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
          externalMeasurementId = createdMeasurement.externalMeasurementId
        }
      )

    assertThat(measurement).isEqualTo(createdMeasurement)
  }

  @Test
  fun `getMeasurementByComputationId succeeds`() =
    runBlocking<Unit> {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val dataProviderValue = dataProvider.toDataProviderValue()
      val createdMeasurement =
        measurementsService.createMeasurement(
          createMeasurementRequest {
            measurement =
              MEASUREMENT.copy {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
                dataProviders[dataProvider.externalDataProviderId] = dataProviderValue
              }
          }
        )
      val measurement =
        measurementsService.getMeasurementByComputationId(
          getMeasurementByComputationIdRequest {
            externalComputationId = createdMeasurement.externalComputationId
          }
        )

      assertThat(measurement)
        .ignoringFields(
          Measurement.REQUISITIONS_FIELD_NUMBER,
          Measurement.COMPUTATION_PARTICIPANTS_FIELD_NUMBER
        )
        .isEqualTo(createdMeasurement.copy { dataProviders.clear() })
      assertThat(measurement.requisitionsList)
        .ignoringFields(Requisition.EXTERNAL_REQUISITION_ID_FIELD_NUMBER)
        .containsExactly(
          requisition {
            externalMeasurementId = createdMeasurement.externalMeasurementId
            externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
            externalComputationId = measurement.externalComputationId
            externalDataProviderId = dataProvider.externalDataProviderId
            updateTime = createdMeasurement.createTime
            state = Requisition.State.PENDING_PARAMS
            dataProviderCertificate = dataProvider.certificate
            parentMeasurement = parentMeasurement {
              apiVersion = createdMeasurement.details.apiVersion
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
              state = createdMeasurement.state
              measurementSpec = createdMeasurement.details.measurementSpec
              measurementSpecSignature = createdMeasurement.details.measurementSpecSignature
              measurementSpecSignatureAlgorithmOid =
                createdMeasurement.details.measurementSpecSignatureAlgorithmOid
              protocolConfig = protocolConfig {
                liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
              }
              dataProvidersCount = 1
            }
            details = details {
              dataProviderPublicKey = dataProviderValue.dataProviderPublicKey
              encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
              nonceHash = dataProviderValue.nonceHash

              // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting
              // these fields.
              dataProviderPublicKeySignature = dataProviderValue.dataProviderPublicKeySignature
              dataProviderPublicKeySignatureAlgorithmOid =
                dataProviderValue.dataProviderPublicKeySignatureAlgorithmOid
            }
            duchies[Population.AGGREGATOR_DUCHY.externalDuchyId] =
              Requisition.DuchyValue.getDefaultInstance()
            duchies[Population.WORKER1_DUCHY.externalDuchyId] =
              Requisition.DuchyValue.getDefaultInstance()
            duchies[Population.WORKER2_DUCHY.externalDuchyId] =
              Requisition.DuchyValue.getDefaultInstance()
          }
        )

      // TODO(@SanjayVas): Verify requisition params once SetParticipantRequisitionParams can be
      // called from this test.
      // TODO(@SanjayVas): Verify requisition params once FailComputationParticipant can be called
      // from this test.
      val templateParticipant = computationParticipant {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalMeasurementId = createdMeasurement.externalMeasurementId
        externalComputationId = createdMeasurement.externalComputationId
        updateTime = createdMeasurement.createTime
        state = ComputationParticipant.State.CREATED
        details = ComputationParticipant.Details.getDefaultInstance()
        apiVersion = createdMeasurement.details.apiVersion
      }
      assertThat(measurement.computationParticipantsList)
        .containsExactly(
          templateParticipant.copy {
            externalDuchyId = Population.AGGREGATOR_DUCHY.externalDuchyId
          },
          templateParticipant.copy { externalDuchyId = Population.WORKER1_DUCHY.externalDuchyId },
          templateParticipant.copy { externalDuchyId = Population.WORKER2_DUCHY.externalDuchyId }
        )
    }

  @Test
  fun `setMeasurementResult fails for wrong externalComputationId`() = runBlocking {
    val aggregatorDuchyId = Population.AGGREGATOR_DUCHY.externalDuchyId
    val duchyCertificate = population.createDuchyCertificate(certificatesService, aggregatorDuchyId)
    val request = setMeasurementResultRequest {
      externalComputationId = 1234L // externalComputationId for Measurement that doesn't exist
      externalAggregatorDuchyId = aggregatorDuchyId
      externalAggregatorCertificateId = duchyCertificate.externalCertificateId
      resultPublicKey = ByteString.copyFromUtf8("resultPublicKey")
      encryptedResult = ByteString.copyFromUtf8("encryptedResult")
      publicApiVersion = Version.V2_ALPHA.string
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.setMeasurementResult(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Measurement not found")
  }

  @Test
  fun `setMeasurementResult fails for wrong aggregator certificate ID`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val createdMeasurement =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val aggregatorDuchyId = Population.AGGREGATOR_DUCHY.externalDuchyId
    val request = setMeasurementResultRequest {
      externalComputationId = createdMeasurement.externalComputationId
      externalAggregatorDuchyId = aggregatorDuchyId
      externalAggregatorCertificateId = 404L
      resultPublicKey = ByteString.copyFromUtf8("resultPublicKey")
      encryptedResult = ByteString.copyFromUtf8("encryptedResult")
      publicApiVersion = Version.V2_ALPHA.string
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.setMeasurementResult(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().ignoringCase().contains("certificate")
  }

  @Test
  fun `setMeasurementResult succeeds`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val createdMeasurement =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val aggregatorDuchyId = Population.AGGREGATOR_DUCHY.externalDuchyId
    val duchyCertificate = population.createDuchyCertificate(certificatesService, aggregatorDuchyId)

    val request = setMeasurementResultRequest {
      externalComputationId = createdMeasurement.externalComputationId
      externalAggregatorDuchyId = aggregatorDuchyId
      externalAggregatorCertificateId = duchyCertificate.externalCertificateId
      resultPublicKey = ByteString.copyFromUtf8("resultPublicKey")
      encryptedResult = ByteString.copyFromUtf8("encryptedResult")
      publicApiVersion = Version.V2_ALPHA.string
    }

    val response = measurementsService.setMeasurementResult(request)

    assertThat(response.updateTime.toInstant())
      .isGreaterThan(createdMeasurement.updateTime.toInstant())
    assertThat(response)
      .ignoringFields(Measurement.UPDATE_TIME_FIELD_NUMBER, Measurement.ETAG_FIELD_NUMBER)
      .isEqualTo(
        createdMeasurement.copy {
          state = Measurement.State.SUCCEEDED
          results += resultInfo {
            externalAggregatorDuchyId = aggregatorDuchyId
            externalCertificateId = duchyCertificate.externalCertificateId
            encryptedResult = request.encryptedResult
            apiVersion = request.publicApiVersion
          }
        }
      )

    val succeededMeasurement =
      measurementsService.getMeasurement(
        getMeasurementRequest {
          externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
          externalMeasurementId = createdMeasurement.externalMeasurementId
        }
      )

    assertThat(response).isEqualTo(succeededMeasurement)
    assertThat(succeededMeasurement.resultsList.size).isEqualTo(1)
  }

  @Test
  fun `cancelMeasurement transitions Measurement state`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurement =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val response =
      measurementsService.cancelMeasurement(
        cancelMeasurementRequest {
          externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          externalMeasurementId = measurement.externalMeasurementId
        }
      )

    assertThat(response.state).isEqualTo(Measurement.State.CANCELLED)
    assertThat(response.updateTime.toInstant()).isGreaterThan(measurement.updateTime.toInstant())
    assertThat(response)
      .isEqualTo(
        measurementsService.getMeasurement(
          getMeasurementRequest {
            externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
            externalMeasurementId = measurement.externalMeasurementId
          }
        )
      )
  }

  @Test
  fun `cancelMeasurement throws FAILED_PRECONDITION when Measurement in illegal state`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val measurement =
        measurementsService.createMeasurement(
          createMeasurementRequest {
            measurement =
              MEASUREMENT.copy {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
              }
          }
        )

      measurementsService.cancelMeasurement(
        cancelMeasurementRequest {
          externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          externalMeasurementId = measurement.externalMeasurementId
        }
      )

      // Should fail as Measurement is already in CANCELLED state.
      val exception =
        assertFailsWith(StatusRuntimeException::class) {
          measurementsService.cancelMeasurement(
            cancelMeasurementRequest {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `streamMeasurements returns all measurements in update time order`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val cancelledMeasurement =
      measurementsService.cancelMeasurement(
        cancelMeasurementRequest {
          externalMeasurementId = measurement1.externalMeasurementId
          externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
        }
      )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
          }
        )
        .toList()

    assertThat(measurements)
      .comparingExpectedFieldsOnly()
      .containsExactly(measurement2, cancelledMeasurement)
      .inOrder()
  }

  @Test
  fun `streamMeasurements returns all measurements in id order`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
          }
        )
        .toList()

    assertThat(measurements)
      .comparingExpectedFieldsOnly()
      .containsExactly(measurement1, measurement2)
      .inOrder()
  }

  @Test
  fun `streamMeasurements can get one page at a time`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val streamMeasurementsRequest = streamMeasurementsRequest {
      limit = 1
      filter = filter {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      }
    }

    val measurements: List<Measurement> =
      measurementsService.streamMeasurements(streamMeasurementsRequest).toList()

    assertThat(measurements).hasSize(1)
    assertThat(measurements).contains(measurement1)

    val measurements2: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest.copy {
            filter =
              filter.copy {
                after =
                  StreamMeasurementsRequestKt.FilterKt.after {
                    updateTime = measurements[0].updateTime
                    measurement = measurementKey {
                      externalMeasurementConsumerId = measurements[0].externalMeasurementConsumerId
                      externalMeasurementId = measurements[0].externalMeasurementId
                    }
                  }
              }
          }
        )
        .toList()

    assertThat(measurements2).hasSize(1)
    assertThat(measurements2).contains(measurement2)
  }

  @Test
  fun `streamMeasurements with duchy filter only returns measurements with duchy as participant`():
    Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    measurementsService.createMeasurement(
      createMeasurementRequest {
        measurement =
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
            details =
              details.copy {
                protocolConfig = protocolConfig {
                  direct = ProtocolConfig.Direct.getDefaultInstance()
                }
                clearDuchyProtocolConfig()
              }
          }
      }
    )
    val measurement3 =
      measurementsService.createMeasurement(createMeasurementRequest { measurement = measurement1 })

    val streamMeasurementsRequest = streamMeasurementsRequest {
      limit = 2
      filter = filter {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalDuchyId = DUCHIES[0].externalDuchyId
      }
      measurementView = Measurement.View.COMPUTATION
    }

    val responses: List<Measurement> =
      measurementsService.streamMeasurements(streamMeasurementsRequest).toList()

    val computationMeasurement1 =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          externalComputationId = measurement1.externalComputationId
        }
      )
    val computationMeasurement3 =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          externalComputationId = measurement3.externalComputationId
        }
      )
    assertThat(responses)
      .containsExactly(computationMeasurement1, computationMeasurement3)
      .inOrder()
  }

  @Test
  fun `streamMeasurements respects limit`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    measurementsService.createMeasurement(
      createMeasurementRequest {
        measurement =
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
          }
      }
    )

    val measurements: List<Measurement> =
      measurementsService.streamMeasurements(streamMeasurementsRequest { limit = 1 }).toList()

    assertThat(measurements).comparingExpectedFieldsOnly().containsExactly(measurement1)
  }

  @Test
  fun `streamMeasurements respects externalMeasurementConsumerId`(): Unit = runBlocking {
    val measurementConsumer1 =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurementConsumer2 =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    measurementsService.createMeasurement(
      createMeasurementRequest {
        measurement =
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer1.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer1.certificate.externalCertificateId
          }
      }
    )

    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer2.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer2.certificate.externalCertificateId
            }
        }
      )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer2.externalMeasurementConsumerId
            }
          }
        )
        .toList()

    assertThat(measurements).comparingExpectedFieldsOnly().containsExactly(measurement2)
  }

  @Test
  fun `streamMeasurements respects states`(): Unit = runBlocking {
    val measurementConsumer1 =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurementConsumer2 =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    measurementsService.createMeasurement(
      createMeasurementRequest {
        measurement =
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer1.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer1.certificate.externalCertificateId
          }
      }
    )

    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer2.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer2.certificate.externalCertificateId
            }
        }
      )

    // SUCCEED second measurement.
    val aggregatorDuchyId = Population.AGGREGATOR_DUCHY.externalDuchyId
    val aggregatorCertificate =
      population.createDuchyCertificate(certificatesService, aggregatorDuchyId)
    val succeededMeasurement =
      measurementsService.setMeasurementResult(
        setMeasurementResultRequest {
          externalComputationId = measurement2.externalComputationId
          externalAggregatorDuchyId = aggregatorDuchyId
          externalAggregatorCertificateId = aggregatorCertificate.externalCertificateId
          resultPublicKey = ByteString.copyFromUtf8("resultPublicKey")
          encryptedResult = ByteString.copyFromUtf8("encryptedResult")
          publicApiVersion = Version.V2_ALPHA.string
        }
      )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest { filter = filter { states += Measurement.State.SUCCEEDED } }
        )
        .toList()

    assertThat(measurements).containsExactly(succeededMeasurement)
  }

  @Test
  fun `batchDeleteMeasurements deletes all requested Measurements`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val measurement3 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val deleteMeasurementRequest1 = deleteMeasurementRequest {
      externalMeasurementId = measurement1.externalMeasurementId
      externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
    }

    val deleteMeasurementRequest2 = deleteMeasurementRequest {
      externalMeasurementId = measurement2.externalMeasurementId
      externalMeasurementConsumerId = measurement2.externalMeasurementConsumerId
    }

    measurementsService.batchDeleteMeasurements(
      batchDeleteMeasurementsRequest {
        requests += listOf(deleteMeasurementRequest1, deleteMeasurementRequest2)
      }
    )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
          }
        )
        .toList()

    assertThat(measurements).containsExactly(measurement3)
  }

  @Test
  fun `batchDeleteMeasurements does not delete any Measurements when any are missing`(): Unit =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val measurement =
        measurementsService.createMeasurement(
          createMeasurementRequest {
            measurement =
              MEASUREMENT.copy {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
              }
          }
        )
      val validMeasurementRequest = deleteMeasurementRequest {
        externalMeasurementId = measurement.externalMeasurementId
        externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
      }

      val missingMeasurementRequest = deleteMeasurementRequest {
        externalMeasurementId = 123L
        externalMeasurementConsumerId = 123L
      }

      assertFailsWith<StatusRuntimeException> {
        measurementsService.batchDeleteMeasurements(
          batchDeleteMeasurementsRequest {
            requests += listOf(validMeasurementRequest, missingMeasurementRequest)
          }
        )
      }

      val measurements: List<Measurement> =
        measurementsService
          .streamMeasurements(
            streamMeasurementsRequest {
              filter = filter {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              }
            }
          )
          .toList()

      assertThat(measurements).containsExactly(measurement)
    }

  @Test
  fun `batchDeleteMeasurements throws NOT_FOUND when Measurement is missing`(): Unit = runBlocking {
    val missingMeasurementRequest = deleteMeasurementRequest {
      externalMeasurementId = 123L
      externalMeasurementConsumerId = 123L
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.batchDeleteMeasurements(
          batchDeleteMeasurementsRequest { requests += missingMeasurementRequest }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Measurement not found")
  }

  @Test
  fun `batchDeleteMeasurements throws INVALID_ARGUMENT when Measurement ids are not specified`():
    Unit = runBlocking {
    val invalidMeasurementRequest = deleteMeasurementRequest {}
    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.batchDeleteMeasurements(
          batchDeleteMeasurementsRequest { requests += invalidMeasurementRequest }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("not specified")
  }

  @Test
  fun `batchDeleteMeasurements throws INVALID_ARGUMENT when Measurements requested exceed limit`():
    Unit = runBlocking {
    val deletionRequests = mutableListOf<DeleteMeasurementRequest>()
    for (i in 1..MAX_BATCH_DELETE + 1) {
      deletionRequests.add(
        deleteMeasurementRequest {
          externalMeasurementId = (123L + 2 * i)
          externalMeasurementConsumerId = (123L + 2 * i)
        }
      )
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.batchDeleteMeasurements(
          batchDeleteMeasurementsRequest { requests += deletionRequests }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("exceeds limit")
  }

  @Test
  fun `batchCancelMeasurements cancels all requested Measurements`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val cancelMeasurementRequest1 = cancelMeasurementRequest {
      externalMeasurementId = measurement1.externalMeasurementId
      externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
    }

    val cancelMeasurementRequest2 = cancelMeasurementRequest {
      externalMeasurementId = measurement2.externalMeasurementId
      externalMeasurementConsumerId = measurement2.externalMeasurementConsumerId
    }

    val cancelledMeasurements =
      measurementsService
        .batchCancelMeasurements(
          batchCancelMeasurementsRequest {
            requests += listOf(cancelMeasurementRequest1, cancelMeasurementRequest2)
          }
        )
        .measurementsList

    val cancelledMeasurement1 =
      measurementsService.getMeasurement(
        getMeasurementRequest {
          externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
          externalMeasurementId = measurement1.externalMeasurementId
        }
      )

    val cancelledMeasurement2 =
      measurementsService.getMeasurement(
        getMeasurementRequest {
          externalMeasurementConsumerId = measurement2.externalMeasurementConsumerId
          externalMeasurementId = measurement2.externalMeasurementId
        }
      )

    assertThat(cancelledMeasurement1.state).isEqualTo(Measurement.State.CANCELLED)
    assertThat(cancelledMeasurement2.state).isEqualTo(Measurement.State.CANCELLED)
    assertThat(cancelledMeasurement1.updateTime.toInstant())
      .isGreaterThan(measurement1.updateTime.toInstant())
    assertThat(cancelledMeasurement2.updateTime.toInstant())
      .isGreaterThan(measurement2.updateTime.toInstant())
    assertThat(cancelledMeasurements)
      .containsExactly(cancelledMeasurement1, cancelledMeasurement2)
      .inOrder()
  }

  @Test
  fun `batchCancelMeasurements does not cancel any Measurements when any are missing`(): Unit =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val measurement =
        measurementsService.createMeasurement(
          createMeasurementRequest {
            measurement =
              MEASUREMENT.copy {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
              }
          }
        )
      val validMeasurementRequest = cancelMeasurementRequest {
        externalMeasurementId = measurement.externalMeasurementId
        externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
      }

      val missingMeasurementRequest = cancelMeasurementRequest {
        externalMeasurementId = 123L
        externalMeasurementConsumerId = 123L
      }

      assertFailsWith<StatusRuntimeException> {
        measurementsService.batchCancelMeasurements(
          batchCancelMeasurementsRequest {
            requests += listOf(validMeasurementRequest, missingMeasurementRequest)
          }
        )
      }

      val measurements: List<Measurement> =
        measurementsService
          .streamMeasurements(
            streamMeasurementsRequest {
              filter = filter {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              }
            }
          )
          .toList()

      assertThat(measurements).containsExactly(measurement)
    }

  @Test
  fun `batchCancelMeasurements throws NOT_FOUND when Measurement is missing`(): Unit = runBlocking {
    val missingMeasurementRequest = cancelMeasurementRequest {
      externalMeasurementId = 123L
      externalMeasurementConsumerId = 123L
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.batchCancelMeasurements(
          batchCancelMeasurementsRequest { requests += missingMeasurementRequest }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Measurement not found")
  }

  @Test
  fun `batchCancelMeasurements throws INVALID_ARGUMENT when Measurement ids are not specified`():
    Unit = runBlocking {
    val invalidMeasurementRequest = cancelMeasurementRequest {}
    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.batchCancelMeasurements(
          batchCancelMeasurementsRequest { requests += invalidMeasurementRequest }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("not specified")
  }

  @Test
  fun `batchCancelMeasurements throws INVALID_ARGUMENT when Measurements requested exceed limit`():
    Unit = runBlocking {
    val cancelRequests = mutableListOf<CancelMeasurementRequest>()
    for (i in 1..MAX_BATCH_CANCEL + 1) {
      cancelRequests.add(
        cancelMeasurementRequest {
          externalMeasurementId = (123L + 2 * i)
          externalMeasurementConsumerId = (123L + 2 * i)
        }
      )
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.batchCancelMeasurements(
          batchCancelMeasurementsRequest { requests += cancelRequests }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("exceeds limit")
  }

  @Test
  fun `batchDeleteMeasurements deletes Measurements when all etags match`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val deleteMeasurementRequest1 = deleteMeasurementRequest {
      externalMeasurementId = measurement1.externalMeasurementId
      externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
      etag = measurement1.etag
    }

    val deleteMeasurementRequest2 = deleteMeasurementRequest {
      externalMeasurementId = measurement2.externalMeasurementId
      externalMeasurementConsumerId = measurement2.externalMeasurementConsumerId
      etag = measurement2.etag
    }

    measurementsService.batchDeleteMeasurements(
      batchDeleteMeasurementsRequest {
        requests += listOf(deleteMeasurementRequest1, deleteMeasurementRequest2)
      }
    )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
          }
        )
        .toList()

    assertThat(measurements).isEmpty()
  }

  @Test
  fun `batchDeleteMeasurements throws ABORTED when etags do not match`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val deleteMeasurementRequest1 = deleteMeasurementRequest {
      externalMeasurementId = measurement1.externalMeasurementId
      externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
      etag = "123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.batchDeleteMeasurements(
          batchDeleteMeasurementsRequest { requests += listOf(deleteMeasurementRequest1) }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception).hasMessageThat().contains("Measurement etag mismatch")
  }

  @Test
  fun `batchDeleteMeasurements does not delete any Measurements when any etags do not match`():
    Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val deleteMeasurementRequest1 = deleteMeasurementRequest {
      externalMeasurementId = measurement1.externalMeasurementId
      externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
      etag = measurement1.etag
    }

    val deleteMeasurementRequest2 = deleteMeasurementRequest {
      externalMeasurementId = measurement2.externalMeasurementId
      externalMeasurementConsumerId = measurement2.externalMeasurementConsumerId
      etag = "123"
    }

    assertFailsWith<StatusRuntimeException> {
      measurementsService.batchDeleteMeasurements(
        batchDeleteMeasurementsRequest {
          requests += listOf(deleteMeasurementRequest1, deleteMeasurementRequest2)
        }
      )
    }

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
          }
        )
        .toList()

    assertThat(measurements).containsExactly(measurement1, measurement2)
  }

  @Test
  fun `batchCancelMeasurements cancels Measurements when all etags match`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val cancelMeasurementRequest1 = cancelMeasurementRequest {
      externalMeasurementId = measurement1.externalMeasurementId
      externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
      etag = measurement1.etag
    }

    val cancelMeasurementRequest2 = cancelMeasurementRequest {
      externalMeasurementId = measurement2.externalMeasurementId
      externalMeasurementConsumerId = measurement2.externalMeasurementConsumerId
      etag = measurement2.etag
    }

    val cancelledMeasurements =
      measurementsService
        .batchCancelMeasurements(
          batchCancelMeasurementsRequest {
            requests += listOf(cancelMeasurementRequest1, cancelMeasurementRequest2)
          }
        )
        .measurementsList

    val cancelledMeasurement1 =
      measurementsService.getMeasurement(
        getMeasurementRequest {
          externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
          externalMeasurementId = measurement1.externalMeasurementId
        }
      )

    val cancelledMeasurement2 =
      measurementsService.getMeasurement(
        getMeasurementRequest {
          externalMeasurementConsumerId = measurement2.externalMeasurementConsumerId
          externalMeasurementId = measurement2.externalMeasurementId
        }
      )

    assertThat(cancelledMeasurement1.state).isEqualTo(Measurement.State.CANCELLED)
    assertThat(cancelledMeasurement2.state).isEqualTo(Measurement.State.CANCELLED)
    assertThat(cancelledMeasurement1.updateTime.toInstant())
      .isGreaterThan(measurement1.updateTime.toInstant())
    assertThat(cancelledMeasurement2.updateTime.toInstant())
      .isGreaterThan(measurement2.updateTime.toInstant())
    assertThat(cancelledMeasurements)
      .containsExactly(cancelledMeasurement1, cancelledMeasurement2)
      .inOrder()
  }

  @Test
  fun `batchCancelMeasurements throws ABORTED when etags do not match`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val cancelMeasurementRequest1 = cancelMeasurementRequest {
      externalMeasurementId = measurement1.externalMeasurementId
      externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
      etag = "123"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.batchCancelMeasurements(
          batchCancelMeasurementsRequest { requests += listOf(cancelMeasurementRequest1) }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception).hasMessageThat().contains("Measurement etag mismatch")
  }

  @Test
  fun `batchCancelMeasurements does not cancel any Measurements when any etags do not match`():
    Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )
    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val cancelMeasurementRequest1 = cancelMeasurementRequest {
      externalMeasurementId = measurement1.externalMeasurementId
      externalMeasurementConsumerId = measurement1.externalMeasurementConsumerId
      etag = measurement1.etag
    }

    val cancelMeasurementRequest2 = cancelMeasurementRequest {
      externalMeasurementId = measurement2.externalMeasurementId
      externalMeasurementConsumerId = measurement2.externalMeasurementConsumerId
      etag = "123"
    }

    assertFailsWith<StatusRuntimeException> {
      measurementsService.batchCancelMeasurements(
        batchCancelMeasurementsRequest {
          requests += listOf(cancelMeasurementRequest1, cancelMeasurementRequest2)
        }
      )
    }

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
          }
        )
        .toList()

    assertThat(measurements).containsExactly(measurement1, measurement2)
  }

  @Test
  fun `streamMeasurements respects updated before time`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest { filter = filter { updatedBefore = measurement2.updateTime } }
        )
        .toList()

    assertThat(measurements).containsExactly(measurement1)
  }

  @Test
  fun `streamMeasurements respects created before time`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val measurement1 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        createMeasurementRequest {
          measurement =
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
            }
        }
      )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest { filter = filter { createdBefore = measurement2.createTime } }
        )
        .toList()

    assertThat(measurements).containsExactly(measurement1)
  }

  companion object {
    @BeforeClass
    @JvmStatic
    fun initConfig() {
      Llv2ProtocolConfig.setForTest(
        ProtocolConfig.LiquidLegionsV2.getDefaultInstance(),
        DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance(),
        setOf(
          Population.AGGREGATOR_DUCHY.externalDuchyId,
          Population.WORKER1_DUCHY.externalDuchyId
        ),
        2
      )
      RoLlv2ProtocolConfig.setForTest(
        ProtocolConfig.LiquidLegionsV2.getDefaultInstance(),
        DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance(),
        setOf(
          Population.AGGREGATOR_DUCHY.externalDuchyId,
          Population.WORKER1_DUCHY.externalDuchyId
        ),
        2
      )
    }
  }
}
