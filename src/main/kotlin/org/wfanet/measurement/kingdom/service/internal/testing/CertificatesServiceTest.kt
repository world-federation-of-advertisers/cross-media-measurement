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
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Duration
import java.time.Instant
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
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.GetCertificateRequestKt
import org.wfanet.measurement.internal.kingdom.LiquidLegionsV2Params
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementFailure
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamCertificatesRequestKt
import org.wfanet.measurement.internal.kingdom.StreamCertificatesRequestKt.orderedKey
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.certificateDetails
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.getCertificateRequest
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementFailure
import org.wfanet.measurement.internal.kingdom.releaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.internal.kingdom.revokeCertificateRequest
import org.wfanet.measurement.internal.kingdom.setParticipantRequisitionParamsRequest
import org.wfanet.measurement.internal.kingdom.streamCertificatesRequest
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

private const val EXTERNAL_CERTIFICATE_ID = 123L
private const val NOT_AN_ID = 13579L

private val CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")
private val X509_DER = ByteString.copyFromUtf8("This is a X.509 certificate in DER format.")

private val CERTIFICATE = certificate {
  notValidBefore = timestamp { seconds = 12345 }
  notValidAfter = timestamp { seconds = 23456 }
  subjectKeyIdentifier = ByteString.copyFromUtf8("This is an SKID")
  details = certificateDetails { x509Der = CERTIFICATE_DER }
}

@RunWith(JUnit4::class)
abstract class CertificatesServiceTest<T : CertificatesCoroutineImplBase> {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHIES)

  protected data class Services<T>(
    val certificatesService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val measurementsService: MeasurementsGrpcKt.MeasurementsCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val modelProvidersService: ModelProvidersCoroutineImplBase,
    val computationParticipantsService: ComputationParticipantsCoroutineImplBase,
    val accountsService: AccountsCoroutineImplBase,
    val requisitionsService: RequisitionsCoroutineImplBase,
  )

  private val clock: Clock = Clock.systemUTC()
  private val idGenerator = SequentialIdGenerator()
  private val population = Population(clock, idGenerator)

  protected lateinit var certificatesService: T
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var measurementsService: MeasurementsGrpcKt.MeasurementsCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var modelProvidersService: ModelProvidersCoroutineImplBase
    private set

  protected lateinit var computationParticipantsService: ComputationParticipantsCoroutineImplBase
    private set

  protected lateinit var accountsService: AccountsCoroutineImplBase
    private set

  protected lateinit var requisitionsService: RequisitionsCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(idGenerator)
    certificatesService = services.certificatesService
    measurementConsumersService = services.measurementConsumersService
    measurementsService = services.measurementsService
    dataProvidersService = services.dataProvidersService
    modelProvidersService = services.modelProvidersService
    computationParticipantsService = services.computationParticipantsService
    accountsService = services.accountsService
    requisitionsService = services.requisitionsService
  }

  @Test
  fun `getCertificate throws INVALID_ARGUMENT when parent not specified`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.getCertificate(
          getCertificateRequest { externalCertificateId = EXTERNAL_CERTIFICATE_ID }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createCertificate throws INVALID_ARGUMENT when parent not specified`() = runBlocking {
    val certificate = CERTIFICATE.copy { clearParent() }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  private fun assertGetFailsWithMissingCertificate(init: GetCertificateRequestKt.Dsl.() -> Unit) {
    val request = getCertificateRequest {
      init()
      externalCertificateId = EXTERNAL_CERTIFICATE_ID
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { certificatesService.getCertificate(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getCertificate fails for missing certificates`() = runBlocking {
    assertGetFailsWithMissingCertificate { externalDuchyId = DUCHIES[0].externalDuchyId }

    val dataProviderId = population.createDataProvider(dataProvidersService).externalDataProviderId
    assertGetFailsWithMissingCertificate { externalDataProviderId = dataProviderId }

    val measurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    assertGetFailsWithMissingCertificate { externalMeasurementConsumerId = measurementConsumerId }

    val modelProviderId =
      population.createModelProvider(modelProvidersService).externalModelProviderId
    assertGetFailsWithMissingCertificate { externalModelProviderId = modelProviderId }
  }

  private fun assertCreateFailsWithMissingOwner(
    expectedMessage: String,
    init: CertificateKt.Dsl.() -> Unit,
  ) {
    val certificate = CERTIFICATE.copy { init() }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { certificatesService.createCertificate(certificate) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains(expectedMessage)
  }

  @Test
  fun `createCertificate fails due to owner not_found`() {
    assertCreateFailsWithMissingOwner("Duchy not found") { externalDuchyId = "missing-duchy-id" }
    assertCreateFailsWithMissingOwner("Data Provider not found") {
      externalDataProviderId = NOT_AN_ID
    }
    assertCreateFailsWithMissingOwner("Measurement Consumer not found") {
      externalMeasurementConsumerId = NOT_AN_ID
    }
    assertCreateFailsWithMissingOwner("Model Provider not found") {
      externalModelProviderId = NOT_AN_ID
    }
  }

  private fun assertCreateCertificateSucceeds(init: CertificateKt.Dsl.() -> Unit) {
    val requestCertificate =
      CERTIFICATE.copy {
        init()
        subjectKeyIdentifier = ByteString.copyFromUtf8("Some unique SKID for $parentCase")
      }
    val createdCertificate = runBlocking {
      certificatesService.createCertificate(requestCertificate)
    }

    val expectedCertificate =
      requestCertificate.copy { externalCertificateId = createdCertificate.externalCertificateId }
    assertThat(createdCertificate).isEqualTo(expectedCertificate)
  }

  @Test
  fun `createCertificate succeeds`() = runBlocking {
    assertCreateCertificateSucceeds { externalDuchyId = DUCHIES[0].externalDuchyId }

    val dataProviderId = population.createDataProvider(dataProvidersService).externalDataProviderId
    assertCreateCertificateSucceeds { externalDataProviderId = dataProviderId }

    val measurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    assertCreateCertificateSucceeds { externalMeasurementConsumerId = measurementConsumerId }

    val modelProviderId =
      population.createModelProvider(modelProvidersService).externalModelProviderId
    assertCreateCertificateSucceeds { externalModelProviderId = modelProviderId }
  }

  private fun assertGetCertificateSucceeds(init: CertificateKt.Dsl.() -> Unit) = runBlocking {
    val requestCertificate =
      CERTIFICATE.copy {
        init()
        subjectKeyIdentifier = ByteString.copyFromUtf8("Some unique SKID for $parentCase")
      }
    val createdCertificate = certificatesService.createCertificate(requestCertificate)
    val getRequest = getCertificateRequest {
      externalCertificateId = createdCertificate.externalCertificateId

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (requestCertificate.parentCase) {
        Certificate.ParentCase.EXTERNAL_DATA_PROVIDER_ID ->
          externalDataProviderId = requestCertificate.externalDataProviderId
        Certificate.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID ->
          externalMeasurementConsumerId = requestCertificate.externalMeasurementConsumerId
        Certificate.ParentCase.EXTERNAL_DUCHY_ID ->
          externalDuchyId = requestCertificate.externalDuchyId
        Certificate.ParentCase.EXTERNAL_MODEL_PROVIDER_ID ->
          externalModelProviderId = requestCertificate.externalModelProviderId
        Certificate.ParentCase.PARENT_NOT_SET -> error("Invalid parentCase")
      }
    }

    assertThat(certificatesService.getCertificate(getRequest)).isEqualTo(createdCertificate)
  }

  @Test
  fun `getCertificate succeeds`() = runBlocking {
    assertGetCertificateSucceeds { externalDuchyId = DUCHIES[0].externalDuchyId }

    val dataProviderId = population.createDataProvider(dataProvidersService).externalDataProviderId
    assertGetCertificateSucceeds { externalDataProviderId = dataProviderId }

    val measurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    assertGetCertificateSucceeds { externalMeasurementConsumerId = measurementConsumerId }

    val modelProviderId =
      population.createModelProvider(modelProvidersService).externalModelProviderId
    assertGetCertificateSucceeds { externalModelProviderId = modelProviderId }
  }

  @Test
  fun `streamCertificates returns certificates in order`() = runBlocking {
    val now = clock.instant()
    val yesterday = now.minus(Duration.ofDays(1))
    val dataProvider =
      population.createDataProvider(dataProvidersService, notValidBefore = yesterday)
    val dataProviderCertificate2 =
      population.createDataProviderCertificate(
        certificatesService,
        dataProvider,
        notValidBefore = now,
      )
    val dataProviderCertificate3 =
      population.createDataProviderCertificate(
        certificatesService,
        dataProvider,
        notValidBefore = yesterday,
      )

    val responses: List<Certificate> =
      certificatesService
        .streamCertificates(
          streamCertificatesRequest {
            filter =
              StreamCertificatesRequestKt.filter {
                externalDataProviderId = dataProvider.externalDataProviderId
              }
          }
        )
        .toList()

    assertThat(responses)
      .containsExactly(dataProviderCertificate2, dataProvider.certificate, dataProviderCertificate3)
      .inOrder()
  }

  @Test
  fun `streamCertificates filters by SKID`() = runBlocking {
    val now = clock.instant()
    val yesterday = now.minus(Duration.ofDays(1))
    val dataProvider =
      population.createDataProvider(dataProvidersService, notValidBefore = yesterday)
    val dataProviderCertificate2 =
      population.createDataProviderCertificate(
        certificatesService,
        dataProvider,
        notValidBefore = now,
      )
    val dataProviderCertificate3 =
      population.createDataProviderCertificate(
        certificatesService,
        dataProvider,
        notValidBefore = yesterday,
      )

    val responses: List<Certificate> =
      certificatesService
        .streamCertificates(
          streamCertificatesRequest {
            filter =
              StreamCertificatesRequestKt.filter {
                externalDataProviderId = dataProvider.externalDataProviderId
                subjectKeyIdentifiers += dataProviderCertificate2.subjectKeyIdentifier
                subjectKeyIdentifiers += dataProviderCertificate3.subjectKeyIdentifier
              }
          }
        )
        .toList()

    assertThat(responses)
      .containsExactly(dataProviderCertificate2, dataProviderCertificate3)
      .inOrder()
  }

  @Test
  fun `streamCertificates filters by after`() = runBlocking {
    val now = clock.instant()
    val yesterday = now.minus(Duration.ofDays(1))
    val dataProvider =
      population.createDataProvider(dataProvidersService, notValidBefore = yesterday)
    val dataProviderCertificate2 =
      population.createDataProviderCertificate(
        certificatesService,
        dataProvider,
        notValidBefore = now,
      )
    val dataProviderCertificate3 =
      population.createDataProviderCertificate(
        certificatesService,
        dataProvider,
        notValidBefore = yesterday,
      )

    val responses: List<Certificate> =
      certificatesService
        .streamCertificates(
          streamCertificatesRequest {
            filter =
              StreamCertificatesRequestKt.filter {
                externalDataProviderId = dataProvider.externalDataProviderId
                after = orderedKey {
                  notValidBefore = now.toProtoTime()
                  externalCertificateId = dataProviderCertificate2.externalCertificateId
                  externalDataProviderId = dataProvider.externalDataProviderId
                }
              }
          }
        )
        .toList()

    assertThat(responses)
      .containsExactly(dataProvider.certificate, dataProviderCertificate3)
      .inOrder()
  }

  @Test
  fun `streamCertificates returns Duchy certificates in order`() = runBlocking {
    val now = clock.instant()
    val yesterday = now.minus(Duration.ofDays(1))
    val duchy = DUCHIES.first()
    val duchyCertificate1 =
      population.createDuchyCertificate(
        certificatesService,
        duchy.externalDuchyId,
        notValidBefore = yesterday,
      )
    val duchyCertificate2 =
      population.createDuchyCertificate(
        certificatesService,
        duchy.externalDuchyId,
        notValidBefore = now,
      )
    val duchyCertificate3 =
      population.createDuchyCertificate(
        certificatesService,
        duchy.externalDuchyId,
        notValidBefore = yesterday,
      )

    val responses: List<Certificate> =
      certificatesService
        .streamCertificates(
          streamCertificatesRequest {
            filter = StreamCertificatesRequestKt.filter { externalDuchyId = duchy.externalDuchyId }
          }
        )
        .toList()

    assertThat(responses)
      .containsExactly(duchyCertificate2, duchyCertificate1, duchyCertificate3)
      .inOrder()
  }

  @Test
  fun `createCertificate fails due to subjectKeyIdentifier collision`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val certificate =
      CERTIFICATE.copy { this.externalMeasurementConsumerId = externalMeasurementConsumerId }

    certificatesService.createCertificate(certificate)
    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception)
      .hasMessageThat()
      .contains("Certificate with the subject key identifier (SKID) already exists.")
  }

  @Test
  fun `revokeCertificate throws INVALID_ARGUMENT when parent not specified`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.revokeCertificate(
          revokeCertificateRequest { revocationState = Certificate.RevocationState.REVOKED }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `revokeCertificate fails due to wrong DataProviderId`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDataProviderId = externalDataProviderId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalDataProviderId = 1234L // wrong externalDataProviderId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.revokeCertificate(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Certificate not found")
  }

  @Test
  fun `revokeCertificate succeeds for DataProviderCertificate`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDataProviderId = externalDataProviderId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalDataProviderId = externalDataProviderId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val revokedCertificate = certificatesService.revokeCertificate(request)

    assertThat(revokedCertificate)
      .isEqualTo(
        certificatesService.getCertificate(
          getCertificateRequest {
            this.externalDataProviderId = externalDataProviderId
            externalCertificateId = certificate.externalCertificateId
          }
        )
      )

    assertThat(revokedCertificate.revocationState).isEqualTo(Certificate.RevocationState.REVOKED)
  }

  @Test
  fun `revokeCertificate for DataProvider fails pending Measurements`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)
    val measurementOne =
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        "measurement one",
        dataProvider,
      )
    val measurementTwo =
      population.createLlv2Measurement(
        measurementsService,
        measurementConsumer,
        "measurement two",
        dataProvider,
      )
    measurementsService.cancelMeasurement(
      cancelMeasurementRequest {
        externalMeasurementConsumerId = measurementTwo.externalMeasurementConsumerId
        externalMeasurementId = measurementTwo.externalMeasurementId
      }
    )
    val request = revokeCertificateRequest {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalCertificateId = dataProvider.certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    certificatesService.revokeCertificate(request)

    val measurements =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter =
              StreamMeasurementsRequestKt.filter {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                states += Measurement.State.FAILED
              }
          }
        )
        .toList()
    assertThat(measurements)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        measurement {
          state = Measurement.State.FAILED
          externalMeasurementId = measurementOne.externalMeasurementId
          details =
            measurementOne.details.copy {
              failure = measurementFailure {
                reason = MeasurementFailure.Reason.CERTIFICATE_REVOKED
                message = "An associated Data Provider certificate has been revoked."
              }
            }
        }
      )
    val requisitions =
      requisitionsService
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              StreamRequisitionsRequestKt.filter {
                externalMeasurementConsumerId = measurementOne.externalMeasurementConsumerId
                externalMeasurementId = measurementOne.externalMeasurementId
              }
          }
        )
        .toList()
    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalDataProviderId = dataProvider.externalDataProviderId
          state = Requisition.State.WITHDRAWN
        }
      )
  }

  @Test
  fun `revokeCertificate fails due to wrong MeasurementConsumerId`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalMeasurementConsumerId = 1234L // wrong MeasurementConsumerId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.revokeCertificate(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Certificate not found")
  }

  @Test
  fun `revokeCertificate succeeds for MeasurementConsumerCertificate`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val certificate =
      certificatesService.createCertificate(
        certificate {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val revokedCertificate = certificatesService.revokeCertificate(request)

    assertThat(revokedCertificate)
      .isEqualTo(
        certificatesService.getCertificate(
          getCertificateRequest {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalCertificateId = certificate.externalCertificateId
          }
        )
      )

    assertThat(revokedCertificate.revocationState).isEqualTo(Certificate.RevocationState.REVOKED)
  }

  @Test
  fun `revokeCertificate for MeasurementConsumer fails pending Measurements`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val measurementOne =
      population.createLlv2Measurement(measurementsService, measurementConsumer, "measurement one")
    val measurementTwo =
      population.createLlv2Measurement(measurementsService, measurementConsumer, "measurement two")
    measurementsService.cancelMeasurement(
      cancelMeasurementRequest {
        externalMeasurementConsumerId = measurementTwo.externalMeasurementConsumerId
        externalMeasurementId = measurementTwo.externalMeasurementId
      }
    )

    val request = revokeCertificateRequest {
      externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      externalCertificateId = measurementConsumer.certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    certificatesService.revokeCertificate(request)

    val measurements =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter =
              StreamMeasurementsRequestKt.filter {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                states += Measurement.State.FAILED
              }
          }
        )
        .toList()

    assertThat(measurements)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        measurement {
          state = Measurement.State.FAILED
          externalMeasurementId = measurementOne.externalMeasurementId
          details =
            measurementOne.details.copy {
              failure = measurementFailure {
                reason = MeasurementFailure.Reason.CERTIFICATE_REVOKED
                message = "The associated Measurement Consumer certificate has been revoked."
              }
            }
        }
      )
  }

  @Test
  fun `revokeCertificate fails due to wrong DuchyId`() = runBlocking {
    val certificate =
      certificatesService.createCertificate(
        certificate {
          externalDuchyId = DUCHIES[0].externalDuchyId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalDuchyId = "non-existing-duchy-id" // wrong MeasurementConsumerId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.revokeCertificate(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Duchy not found")
  }

  @Test
  fun `revokeCertificate succeeds for DuchyCertificate`() = runBlocking {
    val externalDuchyId = DUCHIES[0].externalDuchyId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDuchyId = externalDuchyId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalDuchyId = externalDuchyId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val revokedCertificate = certificatesService.revokeCertificate(request)

    assertThat(revokedCertificate)
      .isEqualTo(
        certificatesService.getCertificate(
          getCertificateRequest {
            this.externalDuchyId = externalDuchyId
            externalCertificateId = certificate.externalCertificateId
          }
        )
      )

    assertThat(revokedCertificate.revocationState).isEqualTo(Certificate.RevocationState.REVOKED)
  }

  @Test
  fun `revokeCertificate for Duchy fails pending Measurements`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val measurementOne =
      population.createLlv2Measurement(measurementsService, measurementConsumer, "measurement one")
    val measurementTwo =
      population.createLlv2Measurement(measurementsService, measurementConsumer, "measurement two")
    measurementsService.cancelMeasurement(
      cancelMeasurementRequest {
        externalMeasurementConsumerId = measurementTwo.externalMeasurementConsumerId
        externalMeasurementId = measurementTwo.externalMeasurementId
      }
    )
    val externalDuchyId = DUCHIES[0].externalDuchyId
    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDuchyId = externalDuchyId
          notValidBefore = clock.instant().minusSeconds(1000L).toProtoTime()
          notValidAfter = clock.instant().plusSeconds(1000L).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )
    computationParticipantsService.setParticipantRequisitionParams(
      setParticipantRequisitionParamsRequest {
        this.externalDuchyId = externalDuchyId
        externalDuchyCertificateId = certificate.externalCertificateId
        externalComputationId = measurementOne.externalComputationId
        liquidLegionsV2 = LiquidLegionsV2Params.getDefaultInstance()
      }
    )
    val request = revokeCertificateRequest {
      this.externalDuchyId = externalDuchyId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }
    certificatesService.revokeCertificate(request)
    val measurements =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter =
              StreamMeasurementsRequestKt.filter {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
                states += Measurement.State.FAILED
              }
          }
        )
        .toList()
    assertThat(measurements)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        measurement {
          state = Measurement.State.FAILED
          externalMeasurementId = measurementOne.externalMeasurementId
          details =
            measurementOne.details.copy {
              failure = measurementFailure {
                reason = MeasurementFailure.Reason.CERTIFICATE_REVOKED
                message = "An associated Duchy certificate has been revoked."
              }
            }
        }
      )
  }

  @Test
  fun `revokeCertificate throws exception when requested state illegal`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDataProviderId = externalDataProviderId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    certificatesService.revokeCertificate(
      revokeCertificateRequest {
        this.externalDataProviderId = externalDataProviderId
        externalCertificateId = certificate.externalCertificateId
        revocationState = Certificate.RevocationState.REVOKED
      }
    )

    val request = revokeCertificateRequest {
      this.externalDataProviderId = externalDataProviderId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.HOLD
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.revokeCertificate(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `releaseCertificateHold throws INVALID_ARGUMENT when parent not specified`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.releaseCertificateHold(releaseCertificateHoldRequest {})
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `releaseCertificateHold fails due to wrong DataProviderId`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDataProviderId = externalDataProviderId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    val request = releaseCertificateHoldRequest {
      this.externalDataProviderId = 1234L // wrong externalDataProviderId
      externalCertificateId = certificate.externalCertificateId
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.releaseCertificateHold(request)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Certificate not found")
  }

  @Test
  fun `releaseCertificateHold fails due to revoked DataProviderCertificate`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDataProviderId = externalDataProviderId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    certificatesService.revokeCertificate(
      revokeCertificateRequest {
        this.externalDataProviderId = externalDataProviderId
        externalCertificateId = certificate.externalCertificateId
        revocationState = Certificate.RevocationState.REVOKED
      }
    )

    val request = releaseCertificateHoldRequest {
      this.externalDataProviderId = externalDataProviderId
      externalCertificateId = certificate.externalCertificateId
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.releaseCertificateHold(request)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is in wrong State.")
  }

  @Test
  fun `releaseCertificateHold succeeds for DataProviderCertificate`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDataProviderId = externalDataProviderId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    certificatesService.revokeCertificate(
      revokeCertificateRequest {
        this.externalDataProviderId = externalDataProviderId
        externalCertificateId = certificate.externalCertificateId
        revocationState = Certificate.RevocationState.HOLD
      }
    )

    val request = releaseCertificateHoldRequest {
      this.externalDataProviderId = externalDataProviderId
      externalCertificateId = certificate.externalCertificateId
    }

    val releasedCertificate = certificatesService.releaseCertificateHold(request)

    assertThat(releasedCertificate)
      .isEqualTo(
        certificatesService.getCertificate(
          getCertificateRequest {
            this.externalDataProviderId = externalDataProviderId
            externalCertificateId = certificate.externalCertificateId
          }
        )
      )

    assertThat(releasedCertificate.revocationState)
      .isEqualTo(Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED)
  }

  @Test
  fun `releaseCertificateHold fails due to wrong MeasurementConsumerId`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    val request = releaseCertificateHoldRequest {
      this.externalMeasurementConsumerId = 1234L // wrong externalMeasurementConsumerId
      externalCertificateId = certificate.externalCertificateId
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.releaseCertificateHold(request)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Certificate not found")
  }

  @Test
  fun `releaseCertificateHold fails due to revoked measurementConsumersCertificate`() =
    runBlocking {
      val externalMeasurementConsumerId =
        population
          .createMeasurementConsumer(measurementConsumersService, accountsService)
          .externalMeasurementConsumerId

      val certificate =
        certificatesService.createCertificate(
          certificate {
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
            notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
            details = certificateDetails { x509Der = X509_DER }
          }
        )

      certificatesService.revokeCertificate(
        revokeCertificateRequest {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          externalCertificateId = certificate.externalCertificateId
          revocationState = Certificate.RevocationState.REVOKED
        }
      )

      val request = releaseCertificateHoldRequest {
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        externalCertificateId = certificate.externalCertificateId
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          certificatesService.releaseCertificateHold(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception).hasMessageThat().contains("Certificate is in wrong State.")
    }

  @Test
  fun `releaseCertificateHold succeeds for MeasurementConsumerCertificate`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    certificatesService.revokeCertificate(
      revokeCertificateRequest {
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        externalCertificateId = certificate.externalCertificateId
        revocationState = Certificate.RevocationState.HOLD
      }
    )

    val request = releaseCertificateHoldRequest {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      externalCertificateId = certificate.externalCertificateId
    }

    val releasedCertificate = certificatesService.releaseCertificateHold(request)

    assertThat(releasedCertificate)
      .isEqualTo(
        certificatesService.getCertificate(
          getCertificateRequest {
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            externalCertificateId = certificate.externalCertificateId
          }
        )
      )

    assertThat(releasedCertificate.revocationState)
      .isEqualTo(Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED)
  }

  @Test
  fun `releaseCertificateHold fails due to wrong DuchyId`() = runBlocking {
    val certificate =
      certificatesService.createCertificate(
        certificate {
          externalDuchyId = DUCHIES[0].externalDuchyId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    val request = releaseCertificateHoldRequest {
      this.externalDuchyId = "non-existing-duchy-id" // wrong MeasurementConsumerId
      externalCertificateId = certificate.externalCertificateId
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.releaseCertificateHold(request)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Duchy not found")
  }

  @Test
  fun `releaseCertificateHold succeeds for DuchyCertificate`() = runBlocking {
    val externalDuchyId = DUCHIES[0].externalDuchyId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDuchyId = externalDuchyId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = certificateDetails { x509Der = X509_DER }
        }
      )

    certificatesService.revokeCertificate(
      revokeCertificateRequest {
        this.externalDuchyId = externalDuchyId
        externalCertificateId = certificate.externalCertificateId
        revocationState = Certificate.RevocationState.HOLD
      }
    )

    val request = releaseCertificateHoldRequest {
      this.externalDuchyId = externalDuchyId
      externalCertificateId = certificate.externalCertificateId
    }

    val releasedCertificate = certificatesService.releaseCertificateHold(request)

    assertThat(releasedCertificate)
      .isEqualTo(
        certificatesService.getCertificate(
          getCertificateRequest {
            this.externalDuchyId = externalDuchyId
            externalCertificateId = certificate.externalCertificateId
          }
        )
      )

    assertThat(releasedCertificate.revocationState)
      .isEqualTo(Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED)
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
  }
}
