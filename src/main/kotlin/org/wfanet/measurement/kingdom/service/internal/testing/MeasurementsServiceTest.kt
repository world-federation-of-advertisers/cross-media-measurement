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
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.resultInfo
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt.details
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.getMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementRequest
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.internal.kingdom.revokeCertificateRequest
import org.wfanet.measurement.internal.kingdom.setMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig.requiredExternalDuchyIds
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

private const val RANDOM_SEED = 1
private const val API_VERSION = "v2alpha"
private const val PROVIDED_MEASUREMENT_ID = "ProvidedMeasurementId"

private val MEASUREMENT = measurement {
  providedMeasurementId = PROVIDED_MEASUREMENT_ID
  details =
    MeasurementKt.details {
      apiVersion = API_VERSION
      measurementSpec = ByteString.copyFromUtf8("MeasurementSpec")
      measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
      duchyProtocolConfig = duchyProtocolConfig {
        liquidLegionsV2 = DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance()
      }
      protocolConfig = protocolConfig {
        liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
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
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
            dataProviders[404L] = dataProvider.toDataProviderValue()
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
          MEASUREMENT.copy {
            externalMeasurementConsumerId = 404L
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
            dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
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
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
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
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
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
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
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
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
            dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
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
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
              dataProviders[dataProvider.externalDataProviderId] =
                dataProvider.toDataProviderValue()
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
            MEASUREMENT.copy {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalMeasurementConsumerCertificateId =
                measurementConsumer.certificate.externalCertificateId
              dataProviders[dataProvider.externalDataProviderId] =
                dataProvider.toDataProviderValue()
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

    val createdMeasurement = measurementsService.createMeasurement(measurement)
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
      val createdMeasurement = measurementsService.createMeasurement(measurement)

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
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
            dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
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

    val createdMeasurement = measurementsService.createMeasurement(measurement)
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

      val createdMeasurement = measurementsService.createMeasurement(measurement)
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
            clearProtocolConfig()
          }
      }

    val createdMeasurement = measurementsService.createMeasurement(measurement)

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
            clearProtocolConfig()
          }
      }

    val createdMeasurement = measurementsService.createMeasurement(measurement)
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
              clearProtocolConfig()
            }
        }

      val createdMeasurement = measurementsService.createMeasurement(measurement)
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
  fun `createMeasurement returns already created measurement for the same ProvidedMeasurementId`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)

      val createdMeasurement =
        measurementsService.createMeasurement(
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            providedMeasurementId = PROVIDED_MEASUREMENT_ID
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
          }
        )

      val secondCreateMeasurementAttempt =
        measurementsService.createMeasurement(
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            providedMeasurementId = PROVIDED_MEASUREMENT_ID
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
          }
        )
      assertThat(secondCreateMeasurementAttempt).isEqualTo(createdMeasurement)
    }

  @Test
  fun `createMeasurement returns new measurement when called without providedMeasurementId`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)

      val measurement =
        measurementsService.createMeasurement(
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            providedMeasurementId = PROVIDED_MEASUREMENT_ID
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
          }
        )

      val otherMeasurement =
        measurementsService.createMeasurement(
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            providedMeasurementId = ""
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
          }
        )

      assertThat(measurement.externalMeasurementId)
        .isNotEqualTo(otherMeasurement.externalMeasurementId)
    }

  @Test
  fun `getMeasurementByComputationId returns created measurement`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val createdMeasurement =
      measurementsService.createMeasurement(
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
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
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
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
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
            dataProviders[dataProvider.externalDataProviderId] = dataProviderValue
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
              protocolConfig = protocolConfig {
                liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
              }
              dataProvidersCount = 1
            }
            details = details {
              dataProviderPublicKey = dataProviderValue.dataProviderPublicKey
              dataProviderPublicKeySignature = dataProviderValue.dataProviderPublicKeySignature
              encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
              nonceHash = dataProviderValue.nonceHash
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
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
        }
      )
    val aggregatorDuchyId = Population.AGGREGATOR_DUCHY.externalDuchyId
    val request = setMeasurementResultRequest {
      externalComputationId = createdMeasurement.externalComputationId
      externalAggregatorDuchyId = aggregatorDuchyId
      externalAggregatorCertificateId = 404L
      resultPublicKey = ByteString.copyFromUtf8("resultPublicKey")
      encryptedResult = ByteString.copyFromUtf8("encryptedResult")
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
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
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
    }

    val response = measurementsService.setMeasurementResult(request)

    assertThat(response.updateTime.toInstant())
      .isGreaterThan(createdMeasurement.updateTime.toInstant())
    assertThat(response)
      .ignoringFields(Measurement.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        createdMeasurement.copy {
          state = Measurement.State.SUCCEEDED
          results += resultInfo {
            externalAggregatorDuchyId = aggregatorDuchyId
            externalCertificateId = duchyCertificate.externalCertificateId
            encryptedResult = request.encryptedResult
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
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
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
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
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
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID + 2
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
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
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID + 2
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
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
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID + 2
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
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
                updatedAfter = measurements[0].updateTime
                externalMeasurementIdAfter = measurements[0].externalMeasurementId
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
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID + 2
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
          details =
            details.copy {
              clearProtocolConfig()
              clearDuchyProtocolConfig()
            }
        }
      )

    val streamMeasurementsRequest = streamMeasurementsRequest {
      limit = 2
      filter = filter {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalDuchyId = DUCHIES[0].externalDuchyId
      }
      measurementView = Measurement.View.COMPUTATION
    }

    val measurements: List<Measurement> =
      measurementsService.streamMeasurements(streamMeasurementsRequest).toList()

    assertThat(measurements).hasSize(1)
    assertThat(measurements[0].externalMeasurementId).isEqualTo(measurement1.externalMeasurementId)
    assertThat(measurements[0].externalMeasurementId)
      .isNotEqualTo(measurement2.externalMeasurementId)
  }

  @Test
  fun `streamMeasurements respects limit`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val measurement1 =
      measurementsService.createMeasurement(
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
        }
      )

    measurementsService.createMeasurement(
      MEASUREMENT.copy {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        providedMeasurementId = PROVIDED_MEASUREMENT_ID + 2
        externalMeasurementConsumerCertificateId =
          measurementConsumer.certificate.externalCertificateId
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
      MEASUREMENT.copy {
        externalMeasurementConsumerId = measurementConsumer1.externalMeasurementConsumerId
        providedMeasurementId = PROVIDED_MEASUREMENT_ID
        externalMeasurementConsumerCertificateId =
          measurementConsumer1.certificate.externalCertificateId
      }
    )

    val measurement2 =
      measurementsService.createMeasurement(
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer2.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID + 2
          externalMeasurementConsumerCertificateId =
            measurementConsumer2.certificate.externalCertificateId
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
      MEASUREMENT.copy {
        externalMeasurementConsumerId = measurementConsumer1.externalMeasurementConsumerId
        providedMeasurementId = PROVIDED_MEASUREMENT_ID
        externalMeasurementConsumerCertificateId =
          measurementConsumer1.certificate.externalCertificateId
      }
    )

    val measurement2 =
      measurementsService.createMeasurement(
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer2.externalMeasurementConsumerId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID + 2
          externalMeasurementConsumerCertificateId =
            measurementConsumer2.certificate.externalCertificateId
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
    }
  }
}
