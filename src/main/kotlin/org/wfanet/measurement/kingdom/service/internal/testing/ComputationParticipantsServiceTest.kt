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
import kotlin.test.assertNotNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantDetails
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt.computedRequisitionParams
import org.wfanet.measurement.internal.kingdom.HonestMajorityShareShuffleParams
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementFailure
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.TrusTeeParams
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.confirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.failComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.getComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.honestMajorityShareShuffleParams
import org.wfanet.measurement.internal.kingdom.liquidLegionsV2Params
import org.wfanet.measurement.internal.kingdom.revokeCertificateRequest
import org.wfanet.measurement.internal.kingdom.setMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.setParticipantRequisitionParamsRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.common.HmssProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.TrusTeeProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.WORKER1_DUCHY

private const val RANDOM_SEED = 1
private const val PROVIDED_MEASUREMENT_ID = "measurement"

private val EL_GAMAL_PUBLIC_KEY = ByteString.copyFromUtf8("This is an ElGamal Public Key.")
private val EL_GAMAL_PUBLIC_KEY_SIGNATURE =
  ByteString.copyFromUtf8("This is an ElGamal Public Key signature.")
private val TINK_PUBLIC_KEY = ByteString.copyFromUtf8("This is an Tink Public Key.")
private val TINK_PUBLIC_KEY_SIGNATURE =
  ByteString.copyFromUtf8("This is an Tink Public Key signature.")
private const val TINK_PUBLIC_KEY_SIGNATURE_ALGORITHM_OID = "2.9999"

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
    val accountsService: AccountsCoroutineImplBase,
  )

  private val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var computationParticipantsService: T
    private set

  private lateinit var duchyCertificates: Map<String, Certificate>

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

  private fun createDuchyCertificates(
    externalDuchyIds: List<String> = DUCHIES.map { it.externalDuchyId }
  ) {
    duchyCertificates =
      externalDuchyIds.associateWith { externalDuchyId ->
        runBlocking { population.createDuchyCertificate(certificatesService, externalDuchyId) }
      }
  }

  @Test
  fun `getComputationParticipant returns ComputationParticipant`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    val measurement: Measurement =
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider,
      )

    val request = getComputationParticipantRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES.first().externalDuchyId
    }
    val response = computationParticipantsService.getComputationParticipant(request)

    assertThat(response)
      .ignoringFields(ComputationParticipant.ETAG_FIELD_NUMBER)
      .isEqualTo(
        computationParticipant {
          externalComputationId = request.externalComputationId
          externalDuchyId = request.externalDuchyId
          externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          externalMeasurementId = measurement.externalMeasurementId
          state = ComputationParticipant.State.CREATED
          updateTime = measurement.updateTime
          apiVersion = Version.V2_ALPHA.string
          details = ComputationParticipantDetails.getDefaultInstance()
        }
      )
    assertThat(response.etag).isNotEmpty()
  }

  @Test
  fun `getComputationParticipant throws NOT_FOUND when Duchy not found`(): Unit = runBlocking {
    val request = getComputationParticipantRequest {
      externalComputationId = 1234L
      externalDuchyId = "invalid Duchy ID"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.getComputationParticipant(request)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    val errorInfo = assertNotNull(exception.errorInfo)
    assertThat(errorInfo.reason).isEqualTo(ErrorCode.DUCHY_NOT_FOUND.name)
    assertThat(errorInfo.metadataMap).containsAtLeast("external_duchy_id", request.externalDuchyId)
  }

  @Test
  fun `getComputationParticipant throws NOT_FOUND when ComputationParticipant not found`(): Unit =
    runBlocking {
      val request = getComputationParticipantRequest {
        externalComputationId = 404L
        externalDuchyId = DUCHIES.first().externalDuchyId
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          computationParticipantsService.getComputationParticipant(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      val errorInfo = assertNotNull(exception.errorInfo)
      assertThat(errorInfo.reason).isEqualTo(ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND.name)
      assertThat(errorInfo.metadataMap)
        .containsAtLeast(
          "external_computation_id",
          request.externalComputationId.toString(),
          "external_duchy_id",
          request.externalDuchyId,
        )
    }

  @Test
  fun `setParticipantRequisitionParams fails for wrong externalDuchyId`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider,
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = "wrong_external_duchy_id"
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Params {
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

    population.createLlv2Measurement(
      measurementsService,
      measurementConsumer,
      PROVIDED_MEASUREMENT_ID,
      dataProvider,
    )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = 12345L // Wrong ExternalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Params {
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
        population.createLlv2Measurement(
          measurementsService,
          measurementConsumer,
          PROVIDED_MEASUREMENT_ID,
          dataProvider,
        )

      val request = setParticipantRequisitionParamsRequest {
        externalComputationId = measurement.externalComputationId
        externalDuchyId = DUCHIES[0].externalDuchyId
        externalDuchyCertificateId = 12345L // Wrong External Duchy Certificate Id
        liquidLegionsV2 = liquidLegionsV2Params {
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
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider,
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Params {
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
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider,
      )

    val certificate =
      population.createDuchyCertificate(
        certificatesService,
        DUCHIES[0].externalDuchyId,
        clock.instant().minus(2L, ChronoUnit.DAYS),
        clock.instant().minus(1L, ChronoUnit.DAYS),
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = certificate.externalDuchyId
      externalDuchyCertificateId = certificate.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Params {
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
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider,
      )

    val certificate =
      population.createDuchyCertificate(
        certificatesService,
        DUCHIES[0].externalDuchyId,
        clock.instant().plus(1L, ChronoUnit.DAYS),
        clock.instant().plus(2L, ChronoUnit.DAYS),
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = certificate.externalDuchyId
      externalDuchyCertificateId = certificate.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Params {
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
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider,
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
      liquidLegionsV2 = liquidLegionsV2Params {
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
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    val measurement: Measurement =
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider,
      )
    val computation: Measurement =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          externalComputationId = measurement.externalComputationId
        }
      )
    val computationParticipant: ComputationParticipant =
      computation.computationParticipantsList.single {
        it.externalDuchyId == DUCHIES[0].externalDuchyId
      }

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Params {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
      etag = computationParticipant.etag
    }
    val response: ComputationParticipant =
      computationParticipantsService.setParticipantRequisitionParams(request)

    assertThat(response)
      .ignoringFields(
        ComputationParticipant.UPDATE_TIME_FIELD_NUMBER,
        ComputationParticipant.ETAG_FIELD_NUMBER,
      )
      .isEqualTo(
        computationParticipant.copy {
          state = ComputationParticipant.State.REQUISITION_PARAMS_SET
          duchyCertificate = duchyCertificates[DUCHIES[0].externalDuchyId]!!
          details = details.copy { liquidLegionsV2 = request.liquidLegionsV2 }
        }
      )
    assertThat(response.updateTime.toInstant())
      .isGreaterThan(computationParticipant.updateTime.toInstant())
    assertThat(response.etag).isNotEqualTo(computationParticipant.etag)

    val updatedComputation: Measurement =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          externalComputationId = measurement.externalComputationId
        }
      )
    assertThat(updatedComputation.state).isEqualTo(Measurement.State.PENDING_REQUISITION_PARAMS)
    assertThat(
        updatedComputation.computationParticipantsList.single {
          it.externalDuchyId == DUCHIES[0].externalDuchyId
        }
      )
      .isEqualTo(response)
  }

  @Test
  fun `setParticipantRequisitionParams throws ABORTED when etag mismatches`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    val measurement: Measurement =
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider,
      )
    val externalDuchyId = DUCHIES[0].externalDuchyId

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      this.externalDuchyId = externalDuchyId
      externalDuchyCertificateId = duchyCertificates[externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Params {
        elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
        elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
      }
      etag = "invalid ETag"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.setParticipantRequisitionParams(request)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception).hasMessageThat().ignoringCase().contains("etag")
  }

  @Test
  fun `setParticipantRequisitionParams succeeds for non-final HMSS Duchy`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)
    val measurement =
      population.createHmssMeasurement(
        measurementsService,
        measurementConsumer,
        PROVIDED_MEASUREMENT_ID,
        dataProvider,
      )
    val computation: Measurement =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          externalComputationId = measurement.externalComputationId
        }
      )
    val computationParticipant: ComputationParticipant =
      computation.computationParticipantsList.single {
        it.externalDuchyId == DUCHIES[0].externalDuchyId
      }

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      honestMajorityShareShuffle = honestMajorityShareShuffleParams {
        tinkPublicKey = TINK_PUBLIC_KEY
        tinkPublicKeySignature = TINK_PUBLIC_KEY_SIGNATURE
        tinkPublicKeySignatureAlgorithmOid = TINK_PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
      }
      etag = computationParticipant.etag
    }
    val response = computationParticipantsService.setParticipantRequisitionParams(request)

    assertThat(response)
      .ignoringFields(
        ComputationParticipant.UPDATE_TIME_FIELD_NUMBER,
        ComputationParticipant.ETAG_FIELD_NUMBER,
      )
      .isEqualTo(
        computationParticipant.copy {
          state = ComputationParticipant.State.READY
          duchyCertificate = duchyCertificates[DUCHIES[0].externalDuchyId]!!
          details = details.copy { honestMajorityShareShuffle = request.honestMajorityShareShuffle }
        }
      )

    val updatedComputation: Measurement =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          externalComputationId = measurement.externalComputationId
        }
      )
    assertThat(updatedComputation.state).isEqualTo(Measurement.State.PENDING_REQUISITION_PARAMS)
    assertThat(
        updatedComputation.computationParticipantsList.single {
          it.externalDuchyId == DUCHIES[0].externalDuchyId
        }
      )
      .isEqualTo(response)
  }

  @Test
  fun `setParticipantRequisitionParams for final HMSS Duchy updates Measurement and Requisition state`() {
    runBlocking {
      createDuchyCertificates()
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val measurement =
        population.createHmssMeasurement(
          measurementsService,
          measurementConsumer,
          PROVIDED_MEASUREMENT_ID,
          dataProvider,
        )

      // Set Participant Params for the aggregator computationParticipant.
      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          this.externalComputationId = measurement.externalComputationId
          externalDuchyId = DUCHIES[0].externalDuchyId
          externalDuchyCertificateId =
            duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
          honestMajorityShareShuffle = HonestMajorityShareShuffleParams.getDefaultInstance()
        }
      )
      // Set Participant Params for second computationParticipant.
      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          this.externalComputationId = measurement.externalComputationId
          externalDuchyId = DUCHIES[1].externalDuchyId
          externalDuchyCertificateId =
            duchyCertificates[DUCHIES[1].externalDuchyId]!!.externalCertificateId
          honestMajorityShareShuffle = honestMajorityShareShuffleParams {
            tinkPublicKey = TINK_PUBLIC_KEY
            tinkPublicKeySignature = TINK_PUBLIC_KEY_SIGNATURE
            tinkPublicKeySignatureAlgorithmOid = TINK_PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
          }
        }
      )
      // Set Participant Params for third computationParticipant.
      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          this.externalComputationId = measurement.externalComputationId
          externalDuchyId = DUCHIES[2].externalDuchyId
          externalDuchyCertificateId =
            duchyCertificates[DUCHIES[2].externalDuchyId]!!.externalCertificateId
          honestMajorityShareShuffle = honestMajorityShareShuffleParams {
            tinkPublicKey = TINK_PUBLIC_KEY
            tinkPublicKeySignature = TINK_PUBLIC_KEY_SIGNATURE
            tinkPublicKeySignatureAlgorithmOid = TINK_PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
          }
        }
      )

      val updatedMeasurement =
        measurementsService.getMeasurementByComputationId(
          getMeasurementByComputationIdRequest {
            externalComputationId = measurement.externalComputationId
          }
        )
      assertThat(updatedMeasurement.state)
        .isEqualTo(Measurement.State.PENDING_REQUISITION_FULFILLMENT)

      val requisitions: List<Requisition> =
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
      assertThat(requisitions.map { it.state }).containsExactly(Requisition.State.UNFULFILLED)
      requisitions.map {
        assertThat(it.state).isEqualTo(Requisition.State.UNFULFILLED)
        assertThat(it.externalFulfillingDuchyId).isNotEmpty()
      }
    }
  }

  @Test
  fun `setParticipantRequisitionParams for final Duchy updates Measurement and Requisition state`() {
    runBlocking {
      createDuchyCertificates()
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider = population.createDataProvider(dataProvidersService)
      val externalComputationId =
        population
          .createLlv2Measurement(
            measurementsService,
            measurementConsumer,
            PROVIDED_MEASUREMENT_ID,
            dataProvider,
          )
          .externalComputationId

      // Set Participant Params for first computationParticipant.
      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          this.externalComputationId = externalComputationId
          externalDuchyId = DUCHIES[0].externalDuchyId
          externalDuchyCertificateId =
            duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
          liquidLegionsV2 = liquidLegionsV2Params {
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
          liquidLegionsV2 = liquidLegionsV2Params {
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
          liquidLegionsV2 = liquidLegionsV2Params {
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

      val requisitions: List<Requisition> =
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
      assertThat(requisitions.map { it.state }).containsExactly(Requisition.State.UNFULFILLED)
    }
  }

  @Test
  fun `setParticipantRequisitionParams for TrusTEE updates Requisition and Measurement state`() {
    runBlocking {
      createDuchyCertificates()
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider =
        population.createDataProvider(
          dataProvidersService,
          listOf(Population.AGGREGATOR_DUCHY.externalDuchyId),
        )
      val externalComputationId =
        population
          .createTrusTeeMeasurement(
            measurementsService,
            measurementConsumer,
            PROVIDED_MEASUREMENT_ID,
            dataProvider,
          )
          .externalComputationId

      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          this.externalComputationId = externalComputationId
          externalDuchyId = DUCHIES[0].externalDuchyId
          externalDuchyCertificateId =
            duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
          trusTee = TrusTeeParams.getDefaultInstance()
        }
      )

      val measurement =
        measurementsService.getMeasurementByComputationId(
          getMeasurementByComputationIdRequest {
            this.externalComputationId = externalComputationId
          }
        )
      assertThat(measurement.state).isEqualTo(Measurement.State.PENDING_REQUISITION_FULFILLMENT)

      val requisitions: List<Requisition> =
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
      assertThat(requisitions.map { it.state }).containsExactly(Requisition.State.UNFULFILLED)
    }
  }

  @Test
  fun `confirmComputationParticipant succeeds for non-last duchy`(): Unit = runBlocking {
    createDuchyCertificates()
    val measurement =
      population.createLlv2Measurement(
        measurementsService,
        population.createMeasurementConsumer(measurementConsumersService, accountsService),
        "measurement",
        population.createDataProvider(dataProvidersService),
        population.createDataProvider(dataProvidersService),
      )

    val setParticipantRequisitionParamsDetails = liquidLegionsV2Params {
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
      population.createLlv2Measurement(
        measurementsService,
        population.createMeasurementConsumer(measurementConsumersService, accountsService),
        "measurement",
        population.createDataProvider(dataProvidersService),
        population.createDataProvider(dataProvidersService),
      )
    val setParticipantRequisitionParamsDetails = liquidLegionsV2Params {
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
  fun `confirmComputationParticipant succeeds with a subset of registered duchies`() = runBlocking {
    val duchies = DUCHIES.dropLast(1)
    createDuchyCertificates(duchies.map { it.externalDuchyId })
    val measurement =
      population.createLlv2Measurement(
        measurementsService,
        population.createMeasurementConsumer(measurementConsumersService, accountsService),
        "measurement",
        population.createDataProvider(
          dataProvidersService,
          listOf(Population.AGGREGATOR_DUCHY.externalDuchyId),
        ),
      )

    val setParticipantRequisitionParamsDetails = liquidLegionsV2Params {
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
    for ((requisition, duchy) in requisitions zip duchies) {
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
    for (duchy in duchies) {
      computationParticipantsService.confirmComputationParticipant(
        confirmComputationParticipantRequest {
          externalComputationId = measurement.externalComputationId
          this.externalDuchyId = duchy.externalDuchyId
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
  fun `confirmComputationParticipant throws ABORTED when etag mismatches`(): Unit = runBlocking {
    createDuchyCertificates()
    val measurement =
      population.createLlv2Measurement(
        measurementsService,
        population.createMeasurementConsumer(measurementConsumersService, accountsService),
        "measurement",
        population.createDataProvider(dataProvidersService),
        population.createDataProvider(dataProvidersService),
      )
    val liquidLegionsV2Details = liquidLegionsV2Params {
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
          liquidLegionsV2 = liquidLegionsV2Details
        }
      )
    }
    // Step 2 - FulfillRequisitions for all Requisitions. This transitions the measurement state to
    // PENDING_PARTICIPANT_CONFIRMATION.
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

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.confirmComputationParticipant(
          confirmComputationParticipantRequest {
            externalComputationId = measurement.externalComputationId
            externalDuchyId = DUCHIES[0].externalDuchyId
            etag = "invalid ETag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception).hasMessageThat().ignoringCase().contains("etag")
  }

  @Test
  fun `failComputationParticipant fails due to illegal measurement state`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)

    val measurement =
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider,
      )

    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = DUCHIES[0].externalDuchyId
      externalDuchyCertificateId =
        duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
      liquidLegionsV2 = liquidLegionsV2Params {
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
        publicApiVersion = Version.V2_ALPHA.string
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
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider,
      )
    val computationParticipant =
      computationParticipantsService.setParticipantRequisitionParams(
        setParticipantRequisitionParamsRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = DUCHIES[0].externalDuchyId
          externalDuchyCertificateId =
            duchyCertificates[DUCHIES[0].externalDuchyId]!!.externalCertificateId
          liquidLegionsV2 = liquidLegionsV2Params {
            elGamalPublicKey = EL_GAMAL_PUBLIC_KEY
            elGamalPublicKeySignature = EL_GAMAL_PUBLIC_KEY_SIGNATURE
          }
        }
      )

    val response =
      computationParticipantsService.failComputationParticipant(
        failComputationParticipantRequest {
          externalComputationId = measurement.externalComputationId
          externalDuchyId = DUCHIES[0].externalDuchyId
          logMessage = "Failure message."
          etag = computationParticipant.etag
        }
      )
    assertThat(response.state).isEqualTo(ComputationParticipant.State.FAILED)

    val failedMeasurement =
      measurementsService.getMeasurementByComputationId(
        getMeasurementByComputationIdRequest {
          externalComputationId = measurement.externalComputationId
        }
      )
    assertThat(failedMeasurement.state).isEqualTo(Measurement.State.FAILED)
    assertThat(failedMeasurement.details.failure.reason)
      .isEqualTo(MeasurementFailure.Reason.COMPUTATION_PARTICIPANT_FAILED)
    assertThat(failedMeasurement.details.failure.message)
      .contains("Computation Participant failed.")
    assertThat(failedMeasurement.details.failure.message).contains("Failure message.")

    assertThat(
        failedMeasurement.computationParticipantsList.singleOrNull {
          it.externalDuchyId == DUCHIES[0].externalDuchyId
        }
      )
      .ignoringFields(ComputationParticipant.FAILURE_LOG_ENTRY_FIELD_NUMBER)
      .isEqualTo(response)
  }

  @Test
  fun `failComputationParticipant throws ABORTED when etag mismatches`() = runBlocking {
    createDuchyCertificates()
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)
    val measurement =
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        "measurement 1",
        dataProvider,
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationParticipantsService.failComputationParticipant(
          failComputationParticipantRequest {
            externalComputationId = measurement.externalComputationId
            externalDuchyId = DUCHIES[0].externalDuchyId
            logMessage = "Failure message."
            etag = "invalid ETag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception).hasMessageThat().ignoringCase().contains("etag")
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
      HmssProtocolConfig.setForTest(
        ProtocolConfig.HonestMajorityShareShuffle.getDefaultInstance(),
        Population.WORKER1_DUCHY.externalDuchyId,
        Population.WORKER2_DUCHY.externalDuchyId,
        Population.AGGREGATOR_DUCHY.externalDuchyId,
      )
      TrusTeeProtocolConfig.setForTest(
        ProtocolConfig.TrusTee.getDefaultInstance(),
        Population.AGGREGATOR_DUCHY.externalDuchyId,
      )
    }
  }
}
