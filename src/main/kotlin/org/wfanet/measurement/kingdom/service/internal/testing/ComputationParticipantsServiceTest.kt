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
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
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
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementConsumer
import org.wfanet.measurement.internal.kingdom.setParticipantRequisitionParamsRequest
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.ProtocolConfigIds

private const val EXTERNAL_PROTOCOL_CONFIG_ID = "llv2"
private const val RANDOM_SEED = 1
private const val PROVIDED_MEASUREMENT_ID = "ProvidedMeasurementId"
private val TEST_INSTANT = Instant.ofEpochMilli(123456789L)
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a public key signature.")
private val EXTERNAL_DUCHY_IDS = listOf("duchy_1", "duchy_2")
private val DUCHY_SUBJECT_KEY_IDENTIFIERS =
  listOf(
    ByteString.copyFromUtf8("duchy_1 subject key identifier."),
    ByteString.copyFromUtf8("duchy_2 subject key identifier.")
  )

private val X509_DER = ByteString.copyFromUtf8("This is a X.509 certificate in DER format.")
private val EL_GAMAL_PUBLIC_KEY = ByteString.copyFromUtf8("This is an ElGamal Public Key.")
private val EL_GAMAL_PUBLIC_KEY_SIGNATURE =
  ByteString.copyFromUtf8("This is an ElGamal Public Key signature.")

@RunWith(JUnit4::class)
abstract class ComputationParticipantsServiceTest<T : ComputationParticipantsCoroutineImplBase> {

  protected data class Services<T>(
    val computationParticipantsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val measurementsService: MeasurementsCoroutineImplBase,
    val certificatesService: CertificatesCoroutineImplBase
  )

  private val clock: Clock = TestClockWithNamedInstants(TEST_INSTANT)
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var computationParticipantsService: T
    private set

  protected lateinit var measurementsService: MeasurementsCoroutineImplBase
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var certificatesService: CertificatesCoroutineImplBase
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
  }

  private suspend fun createDuchyCertificate(
    externalDuchyId: String,
    subjectKeyIdentifier: ByteString
  ): Certificate {
    val now = clock.instant()
    return certificatesService.createCertificate(
      certificate {
        this.externalDuchyId = externalDuchyId
        notValidBefore = now.toProtoTime()
        notValidAfter = now.plus(365L, ChronoUnit.DAYS).toProtoTime()
        this.subjectKeyIdentifier = subjectKeyIdentifier
        details = CertificateKt.details { x509Der = ByteString.copyFromUtf8("Duchy cert") }
      }
    )
  }

  @Test
  fun `confirmComputationParticipant fails for wrong externalDuchyId`() = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createMeasurement(
        measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider
      )
    val certificate =
      createDuchyCertificate(EXTERNAL_DUCHY_IDS.get(0), DUCHY_SUBJECT_KEY_IDENTIFIERS.get(0))

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = "wrong_external_duchy_id"
      externalDuchyCertificateId = certificate.externalCertificateId
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
  fun `confirmComputationParticipant fails for wrong externalComputationId`() = runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
    measurementConsumer.certificate.externalCertificateId
    val dataProvider = population.createDataProvider(dataProvidersService)

    population.createMeasurement(
      measurementsService,
      measurementConsumer,
      "measurement 1",
      dataProvider
    )
    val certificate =
      createDuchyCertificate(EXTERNAL_DUCHY_IDS.get(0), DUCHY_SUBJECT_KEY_IDENTIFIERS.get(0))

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = 12345L // Wrong ExternalComputationId
      externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      externalDuchyCertificateId = certificate.externalCertificateId
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
  fun `confirmComputationParticipant fails for wrong certificate for computationParticipant`() =
      runBlocking {
    val measurementConsumer = population.createMeasurementConsumer(measurementConsumersService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createMeasurement(
        measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider
      )
    createDuchyCertificate(EXTERNAL_DUCHY_IDS.get(0), DUCHY_SUBJECT_KEY_IDENTIFIERS.get(0))

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
  fun `setParticipantRequisitionParams succeeds for non-last duchy`() = runBlocking {
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
    val certificate =
      createDuchyCertificate(EXTERNAL_DUCHY_IDS.get(0), DUCHY_SUBJECT_KEY_IDENTIFIERS.get(0))

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      externalDuchyCertificateId = certificate.externalCertificateId
      liquidLegionsV2 =
        liquidLegionsV2Details {
          elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
          elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
        }
    }

    val expectedComputationParticipant = computationParticipant {
      this.state = ComputationParticipant.State.REQUISITION_PARAMS_SET
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      this.externalMeasurementId = measurement.externalMeasurementId
      this.externalComputationId = measurement.externalComputationId
      this.externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      this.externalDuchyCertificateId = certificate.externalCertificateId
      this.details = details { liquidLegionsV2 = request.liquidLegionsV2 }
    }

    val computationParticipant =
      computationParticipantsService.setParticipantRequisitionParams(request)
    assertThat(computationParticipant)
      .ignoringFields(ComputationParticipant.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(expectedComputationParticipant)

    val nonUpdatedMeasurement =
      measurementsService.getMeasurementByComputationId(
        GetMeasurementByComputationIdRequest.newBuilder()
          .apply {
            externalComputationId = measurement.externalComputationId
            measurementView = Measurement.View.COMPUTATION
          }
          .build()
      )
    assertThat(nonUpdatedMeasurement.state).isEqualTo(Measurement.State.PENDING_REQUISITION_PARAMS)
  }

  @Test
  fun `setParticipantRequisitionParams succeeds for last duchy`() = runBlocking {
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

    // Insert Certificate and set Participant Params for first computationParticipant.
    computationParticipantsService.setParticipantRequisitionParams(
      setParticipantRequisitionParamsRequest {
        externalComputationId = measurement.externalComputationId
        externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
        externalDuchyCertificateId =
          createDuchyCertificate(EXTERNAL_DUCHY_IDS.get(0), DUCHY_SUBJECT_KEY_IDENTIFIERS.get(0))
            .externalCertificateId
        liquidLegionsV2 =
          liquidLegionsV2Details {
            elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
            elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
          }
      }
    )

    val lastCertificate =
      createDuchyCertificate(EXTERNAL_DUCHY_IDS.get(1), DUCHY_SUBJECT_KEY_IDENTIFIERS.get(1))

    val lastRequest = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = EXTERNAL_DUCHY_IDS.get(1)
      externalDuchyCertificateId = lastCertificate.externalCertificateId
      liquidLegionsV2 =
        liquidLegionsV2Details {
          elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
          elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
        }
    }

    val lastExpectedComputationParticipant = computationParticipant {
      this.state = ComputationParticipant.State.REQUISITION_PARAMS_SET
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      this.externalMeasurementId = measurement.externalMeasurementId
      this.externalComputationId = measurement.externalComputationId
      this.externalDuchyId = EXTERNAL_DUCHY_IDS.get(1)
      this.externalDuchyCertificateId = lastCertificate.externalCertificateId
      this.details = details { liquidLegionsV2 = lastRequest.liquidLegionsV2 }
    }

    val lastComputationParticipant =
      computationParticipantsService.setParticipantRequisitionParams(lastRequest)

    assertThat(lastComputationParticipant)
      .ignoringFields(ComputationParticipant.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(lastExpectedComputationParticipant)

    val updatedMeasurement =
      measurementsService.getMeasurementByComputationId(
        GetMeasurementByComputationIdRequest.newBuilder()
          .apply {
            externalComputationId = measurement.externalComputationId
            measurementView = Measurement.View.COMPUTATION
          }
          .build()
      )
    assertThat(updatedMeasurement.state)
      .isEqualTo(Measurement.State.PENDING_REQUISITION_FULFILLMENT)
  }

  @Ignore @Test fun `confirmComputationParticipant succeeds for non-last duchy`() = runBlocking {}
  @Ignore @Test fun `confirmComputationParticipant succeeds for last duchy`() = runBlocking {}

  @Ignore @Test fun `failComputationParticipant succeeds`() = runBlocking {}

  companion object {
    protected const val API_VERSION = "v2alpha"

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      ProtocolConfigIds.setForTest(listOf(EXTERNAL_PROTOCOL_CONFIG_ID))
      DuchyIds.setForTest(EXTERNAL_DUCHY_IDS)
    }
  }
}
