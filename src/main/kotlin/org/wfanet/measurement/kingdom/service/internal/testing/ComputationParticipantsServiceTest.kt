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
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantKt.details
import org.wfanet.measurement.internal.kingdom.ComputationParticipantKt.liquidLegionsV2Details
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.confirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.failComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.setParticipantRequisitionParamsRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private const val PROVIDED_MEASUREMENT_ID = "measurement"
private val TEST_INSTANT = Instant.ofEpochMilli(123456789L)
private val EXTERNAL_DUCHY_IDS = listOf("duchy_1", "duchy_2")

private val EL_GAMAL_PUBLIC_KEY = ByteString.copyFromUtf8("This is an ElGamal Public Key.")
private val EL_GAMAL_PUBLIC_KEY_SIGNATURE =
  ByteString.copyFromUtf8("This is an ElGamal Public Key signature.")

@RunWith(JUnit4::class)
abstract class ComputationParticipantsServiceTest<T : ComputationParticipantsCoroutineImplBase> {

  @get:Rule val duchyIdSetter = DuchyIdSetter(EXTERNAL_DUCHY_IDS)

  protected data class Services<T>(
    val computationParticipantsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val measurementsService: MeasurementsCoroutineImplBase,
    val certificatesService: CertificatesCoroutineImplBase,
    val requisitionsService: RequisitionsCoroutineImplBase
  )

  private val clock: Clock = TestClockWithNamedInstants(TEST_INSTANT)
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var computationParticipantsService: T
    private set

  protected lateinit var duchyCertificates: Map<String, Certificate>
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

    duchyCertificates =
      EXTERNAL_DUCHY_IDS.associateWith { externalDuchyId ->
        runBlocking { population.createDuchyCertificate(certificatesService, externalDuchyId) }
      }
  }

  @Test
  fun `setParticipantRequisitionParams fails for wrong externalDuchyId`() = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createMeasurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = "wrong_external_duchy_id"
      externalDuchyCertificateId =
        duchyCertificates.get(EXTERNAL_DUCHY_IDS.get(0))!!.externalCertificateId
      liquidLegionsV2 =
        liquidLegionsV2Details {
          elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
          elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.setParticipantRequisitionParams(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Duchy not found")
  }

  @Test
  fun `setParticipantRequisitionParams fails for wrong externalComputationId`() = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
    measurementConsumer.certificate.externalCertificateId
    val dataProvider = population.createDataProvider(dataProvidersService)

    population.createMeasurement(
      measurementsService,
      measurementConsumer,
      PROVIDED_MEASUREMENT_ID,
      dataProvider
    )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = 12345L // Wrong ExternalComputationId
      externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      externalDuchyCertificateId =
        duchyCertificates.get(EXTERNAL_DUCHY_IDS.get(0))!!.externalCertificateId
      liquidLegionsV2 =
        liquidLegionsV2Details {
          elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
          elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.setParticipantRequisitionParams(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Computation participant not found")
  }

  @Test
  fun `setParticipantRequisitionParams fails for wrong certificate for computationParticipant`() =
      runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createMeasurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      externalDuchyCertificateId = 12345L // Wrong External Duchy Certificate Id
      liquidLegionsV2 =
        liquidLegionsV2Details {
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
      .contains("Certificate for Computation participant not found")
  }

  @Test
  fun `setParticipantRequisitionParams succeeds for non-final Duchy`() = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createMeasurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      externalDuchyCertificateId =
        duchyCertificates.get(EXTERNAL_DUCHY_IDS.get(0))!!.externalCertificateId
      liquidLegionsV2 =
        liquidLegionsV2Details {
          elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
          elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
        }
    }

    val expectedComputationParticipant = computationParticipant {
      state = ComputationParticipant.State.REQUISITION_PARAMS_SET
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      externalMeasurementId = measurement.externalMeasurementId
      externalComputationId = measurement.externalComputationId
      externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      details = details { liquidLegionsV2 = request.liquidLegionsV2 }
      apiVersion = measurement.details.apiVersion
      duchyCertificate = duchyCertificates.get(EXTERNAL_DUCHY_IDS.get(0))!!
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
      val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val externalComputationId =
        population.createMeasurement(
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
          externalDuchyId = EXTERNAL_DUCHY_IDS[0]
          externalDuchyCertificateId =
            duchyCertificates.get(EXTERNAL_DUCHY_IDS.get(0))!!.externalCertificateId
          liquidLegionsV2 =
            liquidLegionsV2Details {
              elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
              elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
            }
        }
      )

      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          this.externalComputationId = externalComputationId
          externalDuchyId = EXTERNAL_DUCHY_IDS.get(1)
          externalDuchyCertificateId =
            duchyCertificates.get(EXTERNAL_DUCHY_IDS.get(1))!!.externalCertificateId
          liquidLegionsV2 =
            liquidLegionsV2Details {
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
    val measurement =
      population.createMeasurement(
        measurementsService,
        population.createMeasurementConsumer(measurementConsumersService),
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
            filter =
              filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
          }
        )
        .toList()

    // Step 2 - FulfillRequisitions for all Requisitions. This transitions the measurement state to
    // PENDING_PARTICIPANT_CONFIRMATION.
    val participationSignature = ByteString.copyFromUtf8("Participation signature")
    requisitionsService.fulfillRequisition(
      fulfillRequisitionRequest {
        externalComputationId = measurement.externalComputationId
        externalRequisitionId = requisitions[0].externalRequisitionId
        externalFulfillingDuchyId = EXTERNAL_DUCHY_IDS.get(0)
        dataProviderParticipationSignature = participationSignature
      }
    )

    requisitionsService.fulfillRequisition(
      fulfillRequisitionRequest {
        externalComputationId = measurement.externalComputationId
        externalRequisitionId = requisitions[1].externalRequisitionId
        externalFulfillingDuchyId = EXTERNAL_DUCHY_IDS.get(1)
        dataProviderParticipationSignature = participationSignature
      }
    )

    // Step 3 - ConfirmComputationParticipant for just 1 ComputationParticipant. This should NOT
    // transitions the measurement state.
    val computationParticipant =
      computationParticipantsService.confirmComputationParticipant(
        confirmComputationParticipantRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificates.get(EXTERNAL_DUCHY_IDS.get(0))!!.externalDuchyId
        }
      )

    val expectedComputationParticipant = computationParticipant {
      state = ComputationParticipant.State.READY
      this.externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
      externalMeasurementId = measurement.externalMeasurementId
      externalComputationId = measurement.externalComputationId
      externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      details = details { liquidLegionsV2 = setParticipantRequisitionParamsDetails }
      apiVersion = measurement.details.apiVersion
      duchyCertificate = duchyCertificates.get(EXTERNAL_DUCHY_IDS.get(0))!!
    }

    assertThat(computationParticipant)
      .ignoringFields(ComputationParticipant.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(expectedComputationParticipant)
  }

  @Test
  fun `confirmComputationParticipant succeeds for last duchy`() = runBlocking {
    val measurement =
      population.createMeasurement(
        measurementsService,
        population.createMeasurementConsumer(measurementConsumersService),
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
            filter =
              filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
          }
        )
        .toList()

    // Step 2 - FulfillRequisitions for all Requisitions. This transitions the measurement state to
    // PENDING_PARTICIPANT_CONFIRMATION.
    val participationSignature = ByteString.copyFromUtf8("Participation signature")
    requisitionsService.fulfillRequisition(
      fulfillRequisitionRequest {
        externalComputationId = measurement.externalComputationId
        externalRequisitionId = requisitions[0].externalRequisitionId
        externalFulfillingDuchyId = EXTERNAL_DUCHY_IDS.get(0)
        dataProviderParticipationSignature = participationSignature
      }
    )

    requisitionsService.fulfillRequisition(
      fulfillRequisitionRequest {
        externalComputationId = measurement.externalComputationId
        externalRequisitionId = requisitions[1].externalRequisitionId
        externalFulfillingDuchyId = EXTERNAL_DUCHY_IDS.get(1)
        dataProviderParticipationSignature = participationSignature
      }
    )

    // Step 3 - ConfirmComputationParticipant for just 1 ComputationParticipant. This transitions
    // the measurement state to PENDING_COMPUTATION.
    for (duchyCertificate in duchyCertificates.values) {
      computationParticipantsService.confirmComputationParticipant(
        confirmComputationParticipantRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = duchyCertificate.externalDuchyId
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
  fun `failComputationParticipant succeeds`() = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createMeasurement(
        measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      externalDuchyCertificateId =
        duchyCertificates.get(EXTERNAL_DUCHY_IDS.get(0))!!.externalCertificateId
      liquidLegionsV2 =
        liquidLegionsV2Details {
          elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
          elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
        }
    }

    computationParticipantsService.setParticipantRequisitionParams(request)

    val failedComputationParticipant =
      computationParticipantsService.failComputationParticipant(
        failComputationParticipantRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
        }
      )
    val expectedComputationParticipant = computationParticipant {
      this.state = ComputationParticipant.State.FAILED
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      this.externalMeasurementId = measurement.externalMeasurementId
      this.externalComputationId = measurement.externalComputationId
      this.externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      duchyCertificate = duchyCertificates.get(EXTERNAL_DUCHY_IDS.get(0))!!
      apiVersion = measurement.details.apiVersion
      this.details = details { liquidLegionsV2 = request.liquidLegionsV2 }
    }
    assertThat(failedComputationParticipant)
      .ignoringFields(ComputationParticipant.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(expectedComputationParticipant)

    val failedMeasurement =
      measurementsService.getMeasurementByComputationId(
        GetMeasurementByComputationIdRequest.newBuilder()
          .apply { externalComputationId = measurement.externalComputationId }
          .build()
      )
    assertThat(failedMeasurement.state).isEqualTo(Measurement.State.FAILED)
  }
}
