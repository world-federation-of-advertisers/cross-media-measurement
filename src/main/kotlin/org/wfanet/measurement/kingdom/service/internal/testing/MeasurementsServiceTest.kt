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
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt.details
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
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
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private const val API_VERSION = "v2alpha"
private const val PROVIDED_MEASUREMENT_ID = "ProvidedMeasurementId"
private val EXTERNAL_DUCHY_IDS = listOf("Buck", "Rippon", "Shoaks")

private val MEASUREMENT = measurement {
  providedMeasurementId = PROVIDED_MEASUREMENT_ID
  details =
    MeasurementKt.details {
      apiVersion = API_VERSION
      measurementSpec = ByteString.copyFromUtf8("MeasurementSpec")
      measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
      dataProviderList = ByteString.copyFromUtf8("EDP list")
      dataProviderListSalt = ByteString.copyFromUtf8("EDP list salt")
      duchyProtocolConfig =
        duchyProtocolConfig {
          liquidLegionsV2 = DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance()
        }
      protocolConfig =
        protocolConfig { liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance() }
    }
}

@RunWith(JUnit4::class)
abstract class MeasurementsServiceTest<T : MeasurementsCoroutineImplBase> {

  @get:Rule val duchyIdSetter = DuchyIdSetter(EXTERNAL_DUCHY_IDS)

  protected data class Services<T>(
    val measurementsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val certificatesService: CertificatesGrpcKt.CertificatesCoroutineImplBase
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

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    measurementConsumersService = services.measurementConsumersService
    measurementsService = services.measurementsService
    dataProvidersService = services.dataProvidersService
    certificatesService = services.certificatesService
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
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.createMeasurement(
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
            dataProviders[404L] = Measurement.DataProviderValue.getDefaultInstance()
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `createMeasurement fails for missing measurement consumer`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.createMeasurement(
          MEASUREMENT.copy { externalMeasurementConsumerId = 404L }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `createMeasurement fails for revoked Measurement Consumer Certificate`() = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)

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
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
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
            dataProviders[dataProvider.externalDataProviderId] =
              dataProviderValue {
                externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
                dataProviderPublicKey = dataProvider.details.publicKey
                dataProviderPublicKeySignature = dataProvider.details.publicKeySignature
                encryptedRequisitionSpec = ByteString.copyFromUtf8("Encrypted RequisitionSpec")
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
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
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
              dataProviderValue {
                externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
                dataProviderPublicKey = dataProvider.details.publicKey
                dataProviderPublicKeySignature = dataProvider.details.publicKeySignature
                encryptedRequisitionSpec = ByteString.copyFromUtf8("Encrypted RequisitionSpec")
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
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
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
              dataProviderValue {
                externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
                dataProviderPublicKey = dataProvider.details.publicKey
                dataProviderPublicKeySignature = dataProvider.details.publicKeySignature
                encryptedRequisitionSpec = ByteString.copyFromUtf8("Encrypted RequisitionSpec")
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `createMeasurement succeeds`() = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      MEASUREMENT.copy {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalMeasurementConsumerCertificateId =
          measurementConsumer.certificate.externalCertificateId
        dataProviders[dataProvider.externalDataProviderId] =
          dataProviderValue {
            externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
          }
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
  fun `createMeasurement returns already created measurement for the same ProvidedMeasurementId`() =
      runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)

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
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)

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
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
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
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
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
      val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val createdMeasurement =
        measurementsService.createMeasurement(
          MEASUREMENT.copy {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId =
              measurementConsumer.certificate.externalCertificateId
            dataProviders[dataProvider.externalDataProviderId] =
              dataProviderValue {
                externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
                dataProviderPublicKey = dataProvider.details.publicKey
                dataProviderPublicKeySignature = dataProvider.details.publicKeySignature
                encryptedRequisitionSpec = ByteString.copyFromUtf8("Encrypted RequisitionSpec")
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
            state = Requisition.State.UNFULFILLED
            dataProviderCertificate = dataProvider.certificate
            parentMeasurement =
              parentMeasurement {
                apiVersion = createdMeasurement.details.apiVersion
                externalMeasurementConsumerCertificateId =
                  measurementConsumer.certificate.externalCertificateId
                state = createdMeasurement.state
                measurementSpec = createdMeasurement.details.measurementSpec
                measurementSpecSignature = createdMeasurement.details.measurementSpecSignature
                protocolConfig =
                  protocolConfig {
                    liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
                    measurementType = ProtocolConfig.MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED
                  }
              }
            details =
              details {
                dataProviderPublicKey = dataProvider.details.publicKey
                dataProviderPublicKeySignature = dataProvider.details.publicKeySignature
                encryptedRequisitionSpec =
                  createdMeasurement.dataProvidersMap[dataProvider.externalDataProviderId]!!
                    .encryptedRequisitionSpec
              }
            duchies["Buck"] = Requisition.DuchyValue.getDefaultInstance()
            duchies["Rippon"] = Requisition.DuchyValue.getDefaultInstance()
            duchies["Shoaks"] = Requisition.DuchyValue.getDefaultInstance()
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
          templateParticipant.copy { externalDuchyId = "Buck" },
          templateParticipant.copy { externalDuchyId = "Rippon" },
          templateParticipant.copy { externalDuchyId = "Shoaks" }
        )
    }

  @Test
  fun `setMeasurementResult fails for wrong externalComputationId`() = runBlocking {
    val request = setMeasurementResultRequest {
      externalComputationId = 1234L // externalComputationId for Measurement that doesn't exist
      aggregatorCertificate = ByteString.copyFromUtf8("aggregatorCertificate")
      resultPublicKey = ByteString.copyFromUtf8("resultPublicKey")
      encryptedResult = ByteString.copyFromUtf8("encryptedResult")
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.setMeasurementResult(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Measurement not found")
  }

  @Test
  fun `setMeasurementResult succeeds`() = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
    val createdMeasurement =
      measurementsService.createMeasurement(
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
        }
      )

    val request = setMeasurementResultRequest {
      externalComputationId = createdMeasurement.externalComputationId
      aggregatorCertificate = ByteString.copyFromUtf8("aggregatorCertificate")
      resultPublicKey = ByteString.copyFromUtf8("resultPublicKey")
      encryptedResult = ByteString.copyFromUtf8("encryptedResult")
    }

    val measurementWithResult = measurementsService.setMeasurementResult(request)

    val expectedMeasurementDetails =
      createdMeasurement.details.copy {
        aggregatorCertificate = request.aggregatorCertificate
        resultPublicKey = request.resultPublicKey
        encryptedResult = request.encryptedResult
      }
    assertThat(measurementWithResult.updateTime.toInstant())
      .isGreaterThan(createdMeasurement.updateTime.toInstant())

    assertThat(measurementWithResult)
      .ignoringFields(Measurement.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        createdMeasurement.copy {
          state = Measurement.State.SUCCEEDED
          details = expectedMeasurementDetails
        }
      )
  }

  @Test
  fun `cancelMeasurement transitions Measurement state`() = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
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
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
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
  fun `streamMeasurements returns all measurements in order`(): Unit = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)

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
            filter =
              filter {
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
  fun `streamMeasurements respects updated_after`(): Unit = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)

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
          streamMeasurementsRequest { filter = filter { updatedAfter = measurement1.updateTime } }
        )
        .toList()

    assertThat(measurements).comparingExpectedFieldsOnly().containsExactly(measurement2)
  }

  @Test
  fun `streamMeasurements respects limit`(): Unit = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)

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
    val measurementConsumer1 = population.createMeasurementConsumer(measurementConsumersService)
    val measurementConsumer2 = population.createMeasurementConsumer(measurementConsumersService)

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
            filter =
              filter {
                externalMeasurementConsumerId = measurementConsumer2.externalMeasurementConsumerId
              }
          }
        )
        .toList()

    assertThat(measurements).comparingExpectedFieldsOnly().containsExactly(measurement2)
  }

  @Test
  fun `streamMeasurements respects states`(): Unit = runBlocking {
    val measurementConsumer1 = population.createMeasurementConsumer(measurementConsumersService)
    val measurementConsumer2 = population.createMeasurementConsumer(measurementConsumersService)

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
    measurementsService.setMeasurementResult(
      setMeasurementResultRequest {
        externalComputationId = measurement2.externalComputationId
        aggregatorCertificate = ByteString.copyFromUtf8("aggregatorCertificate")
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

    assertThat(measurements)
      .comparingExpectedFieldsOnly()
      .ignoringFields(
        Measurement.UPDATE_TIME_FIELD_NUMBER,
      )
      .containsExactly(measurement2.copy { state = Measurement.State.SUCCEEDED })
  }
}
