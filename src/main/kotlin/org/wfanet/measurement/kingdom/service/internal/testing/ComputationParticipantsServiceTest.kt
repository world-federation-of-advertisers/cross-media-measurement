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
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantKt.details
import org.wfanet.measurement.internal.kingdom.ComputationParticipantKt.liquidLegionsV2Details
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt.computedRequisitionParams
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.confirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.failComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.revokeCertificateRequest
import org.wfanet.measurement.internal.kingdom.setMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.setParticipantRequisitionParamsRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

private const val RANDOM_SEED = 1
private const val PROVIDED_MEASUREMENT_ID = "measurement"

private val EL_GAMAL_PUBLIC_KEY = ByteString.copyFromUtf8("This is an ElGamal Public Key.")
private val EL_GAMAL_PUBLIC_KEY_SIGNATURE =
  ByteString.copyFromUtf8("This is an ElGamal Public Key signature.")

@RunWith(JUnit4::class)
abstract class ComputationParticipantsServiceTest<T : ComputationParticipantsCoroutineImplBase> {

  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHIES)

  protected data class Services<T>(
    val computationParticipantsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val measurementsService: MeasurementsCoroutineImplBase,
    val certificatesService: CertificatesCoroutineImplBase,
    val requisitionsService: RequisitionsCoroutineImplBase,
    val accountsService: AccountsCoroutineImplBase
  )

  private val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var computationParticipantsService: T
    private set

  private lateinit var duchyCertificates: Map<String, Certificate>
    private set

  protected lateinit var measurementsService: MeasurementsCoroutineImplBase
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var certificatesService: CertificatesCoroutineImplBase
    private set

  protected lateinit var requisitionsService: RequisitionsCoroutineImplBase
    private set

  protected lateinit var accountsService: AccountsCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    computationParticipantsService = services.computationParticipantsService
    measurementConsumersService = services.measurementConsumersService
    dataProvidersService = services.dataProvidersService
    measurementsService = services.measurementsService
    certificatesService = services.certificatesService
    requisitionsService = services.requisitionsService
    accountsService = services.accountsService
  }

  private fun createDuchyCertificates() {
    val externalDuchyIds = DUCHIES.map { it.externalDuchyId }
    duchyCertificates =
      externalDuchyIds.associateWith { externalDuchyId ->
        runBlocking { population.createDuchyCertificate(certificatesService, externalDuchyId) }
      }
  }

  @Test
  fun `setParticipantRequisitionParams fails for wrong externalDuchyId`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = "wrong_external_duchy_id"
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Details {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.setParticipantRequisitionParams(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Duchy not found")
  }

  @Test
  fun `setParticipantRequisitionParams fails for wrong externalComputationId`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    measurementConsumer.certificate.externalCertificateId
    val dataProvider = population.createDataProvider(dataProvidersService)

    population.createComputedMeasurement(
      measurementsService,
      measurementConsumer,
      PROVIDED_MEASUREMENT_ID,
      dataProvider
    )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = 12345L // Wrong ExternalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Details {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.setParticipantRequisitionParams(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ComputationParticipant not found")
  }

  @Test
  fun `setParticipantRequisitionParams fails for wrong certificate for computationParticipant`() =
    runBlocking {
      createDuchyCertificates()
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider = population.createDataProvider(dataProvidersService)

      val measurement =
        population.createComputedMeasurement(
          measurementsService,
          measurementConsumer,
          PROVIDED_MEASUREMENT_ID,
          dataProvider
        )

      val request = setParticipantRequisitionParamsRequest {
        externalComputationId = measurement.externalComputationId
        externalDuchyId = DUCHIES[0].externalDuchyId
        externalDuchyCertificateId = 12345L // Wrong External Duchy Certificate Id
        liquidLegionsV2 = liquidLegionsV2Details {
          elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
          elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          computationParticipantsService.setParticipantRequisitionParams(request)
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception).hasMessageThat().contains("Duchy's Certificate not found")
    }

  @Test
  fun `setParticipantRequisitionParams fails for revoked certificate`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Details {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
    }

    certificatesService.revokeCertificate(
      revokeCertificateRequest {
        externalDuchyId = request.externalDuchyId
        externalCertificateId = request.externalDuchyCertificateId
        revocationState = Certificate.RevocationState.REVOKED
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.setParticipantRequisitionParams(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `setParticipantRequisitionParams fails for expired certificate`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider
      )

    val certificate =
      population.createDuchyCertificate(
        certificatesService,
        DUCHIES[0].externalDuchyId,
        clock.instant().minus(2L, ChronoUnit.DAYS),
        clock.instant().minus(1L, ChronoUnit.DAYS)
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = certificate.externalDuchyId
      externalDuchyCertificateId = certificate.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Details {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.setParticipantRequisitionParams(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `setParticipantRequisitionParams fails for not yet valid certificate`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider
      )

    val certificate =
      population.createDuchyCertificate(
        certificatesService,
        DUCHIES[0].externalDuchyId,
        clock.instant().plus(1L, ChronoUnit.DAYS),
        clock.instant().plus(2L, ChronoUnit.DAYS)
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = certificate.externalDuchyId
      externalDuchyCertificateId = certificate.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Details {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.setParticipantRequisitionParams(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `setParticipantRequisitionParams fails for measurement in the wrong state`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider
      )

    measurementsService.cancelMeasurement(
      cancelMeasurementRequest {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalMeasurementId = measurement.externalMeasurementId
      }
    )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Details {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.setParticipantRequisitionParams(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception)
      .hasMessageThat()
      .contains("Measurement not in PENDING_REQUISITION_PARAMS state.")
  }

  @Test
  fun `setParticipantRequisitionParams succeeds for non-final Duchy`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Details {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
    }

    val expectedComputationParticipant = computationParticipant {
      state = ComputationParticipant.State.REQUISITION_PARAMS_SET
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      externalMeasurementId = measurement.externalMeasurementId
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      details = details { liquidLegionsV2 = request.liquidLegionsV2 }
      apiVersion = measurement.details.apiVersion
      duchyCertificate = duchyCertificates[DUCHIES[0].externalDuchyId]!!
    }

    val computationParticipant =
      computationParticipantsService.setParticipantRequisitionParams(request)
    assertThat(computationParticipant)
      .ignoringFields(ComputationParticipant.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(expectedComputationParticipant)

    val nonUpdatedMeasurement =
      measurementsService.getMeasurementByComputationId(
        GetMeasurementByComputationIdRequest.newBuilder()
          .apply { externalComputationId = measurement.externalComputationId }
          .build()
      )
    assertThat(nonUpdatedMeasurement.state).isEqualTo(Measurement.State.PENDING_REQUISITION_PARAMS)
  }

  @Test
  fun `setParticipantRequisitionParams for final Duchy updates Measurement state`() {
    runBlocking {
      createDuchyCertificates()
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val externalComputationId =
        population
          .createComputedMeasurement(
            measurementsService,
            measurementConsumer,
            PROVIDED_MEASUREMENT_ID,
            dataProvider
          )
          .externalComputationId

      // Set Participant Params for first computationParticipant.
      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          this.externalComputationId = externalComputationId
          externalDuchyId = DUCHIES[0].externalDuchyId
          externalDuchyCertificateId =
            duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
          liquidLegionsV2 = liquidLegionsV2Details {
            elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
            elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
          }
        }
      )

      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          this.externalComputationId = externalComputationId
          externalDuchyId = DUCHIES[1].externalDuchyId
          externalDuchyCertificateId =
            duchyCertificates[DUCHIES[1].externalDuchyId]!!.externalCertificateId
          liquidLegionsV2 = liquidLegionsV2Details {
            elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
            elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
          }
        }
      )

      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          this.externalComputationId = externalComputationId
          externalDuchyId = DUCHIES[2].externalDuchyId
          externalDuchyCertificateId =
            duchyCertificates[DUCHIES[2].externalDuchyId]!!.externalCertificateId
          liquidLegionsV2 = liquidLegionsV2Details {
            elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
            elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
          }
        }
      )

      val measurement =
        measurementsService.getMeasurementByComputationId(
          getMeasurementByComputationIdRequest {
            this.externalComputationId = externalComputationId
          }
        )
      assertThat(measurement.state).isEqualTo(Measurement.State.PENDING_REQUISITION_FULFILLMENT)
    }
  }

  @Test
  fun `confirmComputationParticipant succeeds for non-last duchy`(): Unit = runBlocking {
    createDuchyCertificates()
    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        population.createMeasurementConsumer(measurementConsumersService, accountsService),
        "measurement",
        population.createDataProvider(dataProvidersService),
        population.createDataProvider(dataProvidersService)
      )

    val setParticipantRequisitionParamsDetails = liquidLegionsV2Details {
      elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
      elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
    }

    // Step 1 - SetParticipantRequisitionParams for all ComputationParticipants. This transitions
    // the measurement state to PENDING_REQUISITION_FULFILLMENT.
    for (duchyCertificate in duchyCertificates.values) {
      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
          liquidLegionsV2 = setParticipantRequisitionParamsDetails
        }
      )
    }

    val requisitions =
      requisitionsService
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          }
        )
        .toList()

    // Step 2 - FulfillRequisitions for all Requisitions. This transitions the measurement state to
    // PENDING_PARTICIPANT_CONFIRMATION.
    val nonce = 3127743798281582205L
    for ((requisition, duchy) in requisitions zip DUCHIES) {
      requisitionsService.fulfillRequisition(
        fulfillRequisitionRequest {
          externalRequisitionId = requisition.externalRequisitionId
          this.nonce = nonce
          computedParams = computedRequisitionParams {
            externalComputationId = measurement.externalComputationId
            externalFulfillingDuchyId = duchy.externalDuchyId
          }
        }
      )
    }

    // Step 3 - ConfirmComputationParticipant for just 1 ComputationParticipant. This should NOT
    // transitions the measurement state.
    val updatedComputationParticipant =
      computationParticipantsService.confirmComputationParticipant(
        confirmComputationParticipantRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalDuchyId
        }
      )

    assertThat(updatedComputationParticipant.state).isEqualTo(ComputationParticipant.State.READY)
    assertThat(updatedComputationParticipant.details.liquidLegionsV2)
      .isEqualTo(setParticipantRequisitionParamsDetails)

    val relatedMeasurement =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          externalComputationId = measurement.externalComputationId
        }
      )

    assertThat(
        relatedMeasurement.computationParticipantsList.singleOrNull {
          it.externalDuchyId == DUCHIES[0].externalDuchyId
        }
      )
      .isEqualTo(updatedComputationParticipant)
  }

  @Test
  fun `confirmComputationParticipant succeeds for last duchy`() = runBlocking {
    createDuchyCertificates()
    val measurement =
      population.createComputedMeasurement(
        measurementsService,
        population.createMeasurementConsumer(measurementConsumersService, accountsService),
        "measurement",
        population.createDataProvider(dataProvidersService),
        population.createDataProvider(dataProvidersService)
      )
    val setParticipantRequisitionParamsDetails = liquidLegionsV2Details {
      elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
      elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
    }
    // Step 1 - SetParticipantRequisitionParams for all ComputationParticipants. This transitions
    // the measurement state to PENDING_REQUISITION_FULFILLMENT.
    for (duchyCertificate in duchyCertificates.values) {
      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
          externalDuchyCertificateId = duchyCertificate.externalCertificateId
          liquidLegionsV2 = setParticipantRequisitionParamsDetails
        }
      )
    }
    val requisitions =
      requisitionsService
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
              externalMeasurementId = measurement.externalMeasurementId
            }
          }
        )
        .toList()

    // Step 2 - FulfillRequisitions for all Requisitions. This transitions the measurement state to
    // PENDING_PARTICIPANT_CONFIRMATION.
    val nonce = 3127743798281582205L
    for ((requisition, duchy) in requisitions zip DUCHIES) {
      requisitionsService.fulfillRequisition(
        fulfillRequisitionRequest {
          externalRequisitionId = requisition.externalRequisitionId
          this.nonce = nonce
          computedParams = computedRequisitionParams {
            externalComputationId = measurement.externalComputationId
            externalFulfillingDuchyId = duchy.externalDuchyId
          }
        }
      )
    }

    // Step 3 - ConfirmComputationParticipant for all ComputationParticipants. This transitions
    // the measurement state to PENDING_COMPUTATION.
    for (duchies in DUCHIES) {
      computationParticipantsService.confirmComputationParticipant(
        confirmComputationParticipantRequest {
          externalComputationId = measurement.externalComputationId
          this.externalDuchyId = duchies.externalDuchyId
        }
      )
    }

    val updatedMeasurement =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          this.externalComputationId = measurement.externalComputationId
        }
      )
    assertThat(updatedMeasurement.state).isEqualTo(Measurement.State.PENDING_COMPUTATION)
  }

  @Test
  fun `failComputationParticipant fails due to illegal  measurement state`() = runBlocking {
    createDuchyCertificates()
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

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Details {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
    }

    computationParticipantsService.setParticipantRequisitionParams(request)

    measurementsService.setMeasurementResult(
      setMeasurementResultRequest {
        externalComputationId = measurement.externalComputationId
        externalAggregatorDuchyId = request.externalDuchyId
        externalAggregatorCertificateId = request.externalDuchyCertificateId
        resultPublicKey = ByteString.copyFromUtf8("resultPublicKey")
        encryptedResult = ByteString.copyFromUtf8("encryptedResult")
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.failComputationParticipant(
          failComputationParticipantRequest {
            externalComputationId = measurement.externalComputationId
            externalDuchyId = DUCHIES[0].externalDuchyId
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Measurement state is illegal")
  }

  @Test
  fun `failComputationParticipant succeeds`() = runBlocking {
    createDuchyCertificates()
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

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Details {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
    }

    computationParticipantsService.setParticipantRequisitionParams(request)

    val failedComputationParticipant =
      computationParticipantsService.failComputationParticipant(
        failComputationParticipantRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = DUCHIES[0].externalDuchyId
          errorMessage = "Failure message."
        }
      )
    assertThat(failedComputationParticipant.state).isEqualTo(ComputationParticipant.State.FAILED)

    val failedMeasurement =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          externalComputationId = measurement.externalComputationId
        }
      )
    assertThat(failedMeasurement.state).isEqualTo(Measurement.State.FAILED)
    assertThat(failedMeasurement.details.failure.reason)
      .isEqualTo(Measurement.Failure.Reason.COMPUTATION_PARTICIPANT_FAILED)
    assertThat(failedMeasurement.details.failure.message)
      .contains("Computation Participant failed.")
    assertThat(failedMeasurement.details.failure.message).contains("Failure message.")

    assertThat(
        failedMeasurement.computationParticipantsList.singleOrNull {
          it.externalDuchyId == DUCHIES[0].externalDuchyId
        }
      )
      .isEqualTo(failedComputationParticipant)
  }

  companion object {
    @BeforeClass
    @JvmStatic
    fun initConfig() {
      Llv2ProtocolConfig.setForTest(
        ProtocolConfig.LiquidLegionsV2.getDefaultInstance(),
        DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance(),
        setOf(Population.AGGREGATOR_DUCHY.externalDuchyId),
        2
      )
    }
  }
}
