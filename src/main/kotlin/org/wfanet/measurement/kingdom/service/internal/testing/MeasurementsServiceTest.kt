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
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementKt.details
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.copy
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
private const val PROVIDED_MEASUREMENT_ID = "ProvidedMeasurementId"
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val PREFERRED_MC_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a MC certificate der.")
private val PREFERRED_DP_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a DP certificate der.")
private val PREFERRED_MC_SUBJECT_KEY_IDENTIFIER =
  ByteString.copyFromUtf8("This is a MC subject key identifier.")
private val PREFERRED_DP_SUBJECT_KEY_IDENTIFIER =
  ByteString.copyFromUtf8("This is a DP subject key identifier.")
private val EXTERNAL_DUCHY_IDS = listOf("Buck", "Rippon", "Shoaks")

@RunWith(JUnit4::class)
abstract class MeasurementsServiceTest<T : MeasurementsCoroutineImplBase> {

  @get:Rule val duchyIdSetter = DuchyIdSetter(EXTERNAL_DUCHY_IDS)

  protected data class Services<T>(
    val measurementsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val certificatesService: CertificatesGrpcKt.CertificatesCoroutineImplBase
  )

  protected val testClock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(testClock, Random(RANDOM_SEED))
  private val population = Population(testClock, idGenerator)

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

  // TODO(@uakyol) : delete these helper functions and use Population.kt
  private suspend fun insertDataProvider(notValidBefore: Long, notValidAfter: Long): DataProvider {
    return dataProvidersService.createDataProvider(
      DataProvider.newBuilder()
        .apply {
          certificateBuilder.apply {
            notValidBeforeBuilder.seconds = notValidBefore
            notValidAfterBuilder.seconds = notValidAfter
            subjectKeyIdentifier = PREFERRED_DP_SUBJECT_KEY_IDENTIFIER
            detailsBuilder.x509Der = PREFERRED_DP_CERTIFICATE_DER
          }
          detailsBuilder.apply {
            apiVersion = "v2alpha"
            publicKey = PUBLIC_KEY
            publicKeySignature = PUBLIC_KEY_SIGNATURE
          }
        }
        .build()
    )
  }

  private suspend fun insertDataProvider(): DataProvider {
    return insertDataProvider(
      testClock.instant().epochSecond - 1000L,
      testClock.instant().epochSecond + 1000L
    )
  }

  private suspend fun insertMeasurementConsumer(
    notValidBefore: Long,
    notValidAfter: Long
  ): MeasurementConsumer {
    return measurementConsumersService.createMeasurementConsumer(
      MeasurementConsumer.newBuilder()
        .apply {
          certificateBuilder.apply {
            notValidBeforeBuilder.seconds = notValidBefore
            notValidAfterBuilder.seconds = notValidAfter
            subjectKeyIdentifier = PREFERRED_MC_SUBJECT_KEY_IDENTIFIER
            detailsBuilder.x509Der = PREFERRED_MC_CERTIFICATE_DER
          }
          detailsBuilder.apply {
            apiVersion = "v2alpha"
            publicKey = PUBLIC_KEY
            publicKeySignature = PUBLIC_KEY_SIGNATURE
          }
        }
        .build()
    )
  }

  private suspend fun insertMeasurementConsumer(): MeasurementConsumer {
    return insertMeasurementConsumer(
      testClock.instant().epochSecond - 1000L,
      testClock.instant().epochSecond + 1000L
    )
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
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val externalDataProviderId = 0L
    val measurement =
      Measurement.newBuilder()
        .also {
          it.detailsBuilder.apiVersion = "v2alpha"
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
          it.putAllDataProviders(
            mapOf(
              externalDataProviderId to
                Measurement.DataProviderValue.newBuilder()
                  .apply { externalDataProviderCertificateId = 0L }
                  .build()
            )
          )
          it.providedMeasurementId = PROVIDED_MEASUREMENT_ID
        }
        .build()

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.createMeasurement(measurement) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `createMeasurement fails for missing measurement consumer`() = runBlocking {
    val externalMeasurementConsumerId = 0L
    val externalDataProviderId = insertDataProvider().externalDataProviderId
    val measurement =
      Measurement.newBuilder()
        .also {
          it.detailsBuilder.apiVersion = "v2alpha"
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.putAllDataProviders(
            mapOf(
              externalDataProviderId to
                Measurement.DataProviderValue.newBuilder()
                  .apply { externalDataProviderCertificateId = 0L }
                  .build()
            )
          )
          it.providedMeasurementId = PROVIDED_MEASUREMENT_ID
        }
        .build()

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.createMeasurement(measurement) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `createMeasurement fails for revoked Measurement Consumer Certificate`() = runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
    val measurement = measurement {
      details = details { apiVersion = "v2alpha" }
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
      this.dataProviders[externalDataProviderId] =
        dataProviderValue {
          this.externalDataProviderCertificateId = externalDataProviderCertificateId
        }
      this.providedMeasurementId = PROVIDED_MEASUREMENT_ID
    }

    certificatesService.revokeCertificate(
      revokeCertificateRequest {
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        externalCertificateId = externalMeasurementConsumerCertificateId
        revocationState = Certificate.RevocationState.REVOKED
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.createMeasurement(measurement) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `createMeasurement fails when current time is before mc certificate is valid`() =
      runBlocking {
    val measurementConsumer =
      insertMeasurementConsumer(
        testClock.instant().epochSecond + 1000L,
        testClock.instant().epochSecond + 2000L
      )
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
    val measurement = measurement {
      details = details { apiVersion = "v2alpha" }
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
      this.dataProviders[externalDataProviderId] =
        dataProviderValue {
          this.externalDataProviderCertificateId = externalDataProviderCertificateId
        }
      this.providedMeasurementId = PROVIDED_MEASUREMENT_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.createMeasurement(measurement) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `createMeasurement fails when current time is after mc certificate is valid`() = runBlocking {
    val measurementConsumer =
      insertMeasurementConsumer(
        testClock.instant().epochSecond - 2000L,
        testClock.instant().epochSecond - 1000L
      )
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
    val measurement = measurement {
      details = details { apiVersion = "v2alpha" }
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
      this.dataProviders[externalDataProviderId] =
        dataProviderValue {
          this.externalDataProviderCertificateId = externalDataProviderCertificateId
        }
      this.providedMeasurementId = PROVIDED_MEASUREMENT_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.createMeasurement(measurement) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `createMeasurement fails for revoked Data Provider Certificate`() = runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
    val measurement = measurement {
      details = details { apiVersion = "v2alpha" }
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
      this.dataProviders[externalDataProviderId] =
        dataProviderValue {
          this.externalDataProviderCertificateId = externalDataProviderCertificateId
        }
      this.providedMeasurementId = PROVIDED_MEASUREMENT_ID
    }

    certificatesService.revokeCertificate(
      revokeCertificateRequest {
        this.externalDataProviderId = externalDataProviderId
        externalCertificateId = externalDataProviderCertificateId
        revocationState = Certificate.RevocationState.REVOKED
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.createMeasurement(measurement) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `createMeasurement fails when current time is before edp certificate is valid`() =
      runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider =
      insertDataProvider(
        testClock.instant().epochSecond + 1000L,
        testClock.instant().epochSecond + 2000L
      )
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
    val measurement = measurement {
      details = details { apiVersion = "v2alpha" }
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
      this.dataProviders[externalDataProviderId] =
        dataProviderValue {
          this.externalDataProviderCertificateId = externalDataProviderCertificateId
        }
      this.providedMeasurementId = PROVIDED_MEASUREMENT_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.createMeasurement(measurement) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `createMeasurement fails when current time is after edp certificate is valid`() =
      runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider =
      insertDataProvider(
        testClock.instant().epochSecond - 2000L,
        testClock.instant().epochSecond - 1000L
      )
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
    val measurement = measurement {
      details = details { apiVersion = "v2alpha" }
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
      this.dataProviders[externalDataProviderId] =
        dataProviderValue {
          this.externalDataProviderCertificateId = externalDataProviderCertificateId
        }
      this.providedMeasurementId = PROVIDED_MEASUREMENT_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { measurementsService.createMeasurement(measurement) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Certificate is invalid")
  }

  @Test
  fun `createMeasurement succeeds`() = runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
    val measurement = measurement {
      details = details { apiVersion = "v2alpha" }
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
      this.dataProviders[externalDataProviderId] =
        dataProviderValue {
          this.externalDataProviderCertificateId = externalDataProviderCertificateId
        }
      this.providedMeasurementId = PROVIDED_MEASUREMENT_ID
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
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    insertDataProvider()
    val measurement =
      Measurement.newBuilder()
        .also {
          it.detailsBuilder.apiVersion = "v2alpha"
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
          it.providedMeasurementId = PROVIDED_MEASUREMENT_ID
        }
        .build()

    val createdMeasurement = measurementsService.createMeasurement(measurement)
    val secondCreateMeasurementAttempt = measurementsService.createMeasurement(measurement)
    assertThat(secondCreateMeasurementAttempt).isEqualTo(createdMeasurement)
  }

  @Test
  fun `createMeasurement returns new measurement when called without providedMeasurementId`() =
      runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    insertDataProvider()
    val measurement =
      Measurement.newBuilder()
        .also {
          it.detailsBuilder.apiVersion = "v2alpha"
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
        }
        .build()

    val otherMeasurement =
      measurement.copy {
        details =
          details { measurementSpec = ByteString.copyFromUtf8("This is a MeasurementSpec.") }
      }

    measurementsService.createMeasurement(measurement)
    val secondCreateMeasurementAttempt = measurementsService.createMeasurement(otherMeasurement)
    assertThat(secondCreateMeasurementAttempt)
      .ignoringFields(
        Measurement.EXTERNAL_MEASUREMENT_ID_FIELD_NUMBER,
        Measurement.EXTERNAL_COMPUTATION_ID_FIELD_NUMBER,
        Measurement.CREATE_TIME_FIELD_NUMBER,
        Measurement.UPDATE_TIME_FIELD_NUMBER,
      )
      .isEqualTo(otherMeasurement.copy { state = Measurement.State.PENDING_REQUISITION_PARAMS })
  }

  @Test
  fun `getMeasurementByComputationId returns created measurement`() = runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val dataProvider = insertDataProvider()
    val createdMeasurement =
      measurementsService.createMeasurement(
        measurement {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
          providedMeasurementId = PROVIDED_MEASUREMENT_ID
          details = MeasurementKt.details { apiVersion = "v2alpha" }
          dataProviders[dataProvider.externalDataProviderId] =
            dataProviderValue {
              externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
              dataProviderPublicKey = dataProvider.details.publicKey
              dataProviderPublicKeySignature = dataProvider.details.publicKeySignature
              encryptedRequisitionSpec = ByteString.copyFromUtf8("encrypted RequisitionSpec")
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
      .isEqualTo(createdMeasurement.copy { this.dataProviders.clear() })
  }

  @Test
  fun `getMeasurement succeeds`() =
    runBlocking<Unit> {
      val measurementConsumer = insertMeasurementConsumer()
      val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      val externalMeasurementConsumerCertificateId =
        measurementConsumer.certificate.externalCertificateId
      val dataProvider = insertDataProvider()
      val externalDataProviderId = dataProvider.externalDataProviderId
      val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
      val createdMeasurement =
        measurementsService.createMeasurement(
          measurement {
            details = MeasurementKt.details { apiVersion = "v2alpha" }
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
            dataProviders[externalDataProviderId] =
              MeasurementKt.dataProviderValue {
                this.externalDataProviderCertificateId = externalDataProviderCertificateId
              }
          }
        )

      val measurement =
        measurementsService.getMeasurement(
          getMeasurementRequest {
            this.externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
            externalMeasurementId = createdMeasurement.externalMeasurementId
          }
        )
      assertThat(measurement).isEqualTo(createdMeasurement)
    }

  @Test
  fun `getMeasurementByComputationId succeeds`() =
    runBlocking<Unit> {
      val measurementConsumer = insertMeasurementConsumer()
      val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      val externalMeasurementConsumerCertificateId =
        measurementConsumer.certificate.externalCertificateId
      val dataProvider = insertDataProvider()
      val externalDataProviderId = dataProvider.externalDataProviderId
      val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
      val createdMeasurement =
        measurementsService.createMeasurement(
          measurement {
            details =
              MeasurementKt.details {
                apiVersion = "v2alpha"
                protocolConfig =
                  protocolConfig {
                    measurementType = ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
                  }
              }
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
            dataProviders[externalDataProviderId] =
              MeasurementKt.dataProviderValue {
                this.externalDataProviderCertificateId = externalDataProviderCertificateId
              }
            providedMeasurementId = PROVIDED_MEASUREMENT_ID
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
        .isEqualTo(createdMeasurement.copy { this.dataProviders.clear() })
      assertThat(measurement.requisitionsList)
        .ignoringFields(Requisition.EXTERNAL_REQUISITION_ID_FIELD_NUMBER)
        .containsExactly(
          requisition {
            externalMeasurementId = createdMeasurement.externalMeasurementId
            this.externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
            externalComputationId = measurement.externalComputationId
            this.externalDataProviderId = externalDataProviderId
            updateTime = createdMeasurement.createTime
            state = Requisition.State.UNFULFILLED
            dataProviderCertificate = dataProvider.certificate
            parentMeasurement =
              parentMeasurement {
                apiVersion = createdMeasurement.details.apiVersion
                this.externalMeasurementConsumerCertificateId =
                  externalMeasurementConsumerCertificateId
                state = createdMeasurement.state
                protocolConfig =
                  protocolConfig {
                    measurementType = ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
                  }
              }
            details = Requisition.Details.getDefaultInstance()
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
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
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
  fun `setMeasurementResult fails for wrong externalComputationId`() =
    runBlocking<Unit> {
      val request = setMeasurementResultRequest {
        externalComputationId = 1234L // externalComputationId for Measurement that doesn't exist
        aggregatorCertificate = ByteString.copyFromUtf8("aggregatorCertificate")
        resultPublicKey = ByteString.copyFromUtf8("resultPublicKey")
        encryptedResult = ByteString.copyFromUtf8("encryptedResult")
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          measurementsService.setMeasurementResult(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception).hasMessageThat().contains("Measurement not found")
    }

  @Test
  fun `setMeasurementResult succeeds`() =
    runBlocking<Unit> {
      val measurementConsumer = insertMeasurementConsumer()
      val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      val externalMeasurementConsumerCertificateId =
        measurementConsumer.certificate.externalCertificateId
      val dataProvider = insertDataProvider()
      val externalDataProviderId = dataProvider.externalDataProviderId
      val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId

      val createdMeasurement =
        measurementsService.createMeasurement(
          measurement {
            details = MeasurementKt.details { apiVersion = "v2alpha" }
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
            dataProviders[externalDataProviderId] =
              MeasurementKt.dataProviderValue {
                this.externalDataProviderCertificateId = externalDataProviderCertificateId
              }
            providedMeasurementId = PROVIDED_MEASUREMENT_ID
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
          this.aggregatorCertificate = request.aggregatorCertificate
          this.resultPublicKey = request.resultPublicKey
          this.encryptedResult = request.encryptedResult
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
    val measurement =
      population.createMeasurement(
        measurementsService,
        population.createMeasurementConsumer(measurementConsumersService),
        "measurement",
        population.createDataProvider(dataProvidersService)
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
    val measurement =
      population.createMeasurement(
        measurementsService,
        population.createMeasurementConsumer(measurementConsumersService),
        "measurement",
        population.createDataProvider(dataProvidersService)
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
  fun `streamMeasuerements returns all measurements in order`(): Unit = runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId

    val measurement1 =
      measurementsService.createMeasurement(
        measurement {
          details = MeasurementKt.details { apiVersion = "v2alpha" }
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
          dataProviders[externalDataProviderId] =
            MeasurementKt.dataProviderValue {
              this.externalDataProviderCertificateId = externalDataProviderCertificateId
            }
          providedMeasurementId = "measurement1"
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        measurement {
          details = MeasurementKt.details { apiVersion = "v2alpha" }
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
          dataProviders[externalDataProviderId] =
            MeasurementKt.dataProviderValue {
              this.externalDataProviderCertificateId = externalDataProviderCertificateId
            }
          providedMeasurementId = "measurement2"
        }
      )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter { this.externalMeasurementConsumerId = externalMeasurementConsumerId }
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
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId

    val measurement1 =
      measurementsService.createMeasurement(
        measurement {
          details = MeasurementKt.details { apiVersion = "v2alpha" }
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
          dataProviders[externalDataProviderId] =
            MeasurementKt.dataProviderValue {
              this.externalDataProviderCertificateId = externalDataProviderCertificateId
            }
          providedMeasurementId = "measurement1"
        }
      )

    val measurement2 =
      measurementsService.createMeasurement(
        measurement {
          details = MeasurementKt.details { apiVersion = "v2alpha" }
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
          dataProviders[externalDataProviderId] =
            MeasurementKt.dataProviderValue {
              this.externalDataProviderCertificateId = externalDataProviderCertificateId
            }
          providedMeasurementId = "measurement2"
        }
      )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter { this.updatedAfter = measurement1.updateTime }
          }
        )
        .toList()

    assertThat(measurements).comparingExpectedFieldsOnly().containsExactly(measurement2)
  }

  @Test
  fun `streamMeasurements respects limit`(): Unit = runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId

    val measurement1 =
      measurementsService.createMeasurement(
        measurement {
          details = MeasurementKt.details { apiVersion = "v2alpha" }
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
          dataProviders[externalDataProviderId] =
            MeasurementKt.dataProviderValue {
              this.externalDataProviderCertificateId = externalDataProviderCertificateId
            }
          providedMeasurementId = "measurement1"
        }
      )

    measurementsService.createMeasurement(
      measurement {
        details = MeasurementKt.details { apiVersion = "v2alpha" }
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId
        dataProviders[externalDataProviderId] =
          MeasurementKt.dataProviderValue {
            this.externalDataProviderCertificateId = externalDataProviderCertificateId
          }
        providedMeasurementId = "measurement2"
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
    val externalMeasurementConsumerId1 = measurementConsumer1.externalMeasurementConsumerId
    val externalMeasurementConsumerId2 = measurementConsumer2.externalMeasurementConsumerId

    val externalMeasurementConsumerCertificateId1 =
      measurementConsumer1.certificate.externalCertificateId
    val externalMeasurementConsumerCertificateId2 =
      measurementConsumer2.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId

    measurementsService.createMeasurement(
      measurement {
        details = MeasurementKt.details { apiVersion = "v2alpha" }
        this.externalMeasurementConsumerId = externalMeasurementConsumerId1
        this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId1
        dataProviders[externalDataProviderId] =
          MeasurementKt.dataProviderValue {
            this.externalDataProviderCertificateId = externalDataProviderCertificateId
          }
        providedMeasurementId = "measurement1"
      }
    )

    val measurement2 =
      measurementsService.createMeasurement(
        measurement {
          details = MeasurementKt.details { apiVersion = "v2alpha" }
          this.externalMeasurementConsumerId = externalMeasurementConsumerId2
          this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId2
          dataProviders[externalDataProviderId] =
            MeasurementKt.dataProviderValue {
              this.externalDataProviderCertificateId = externalDataProviderCertificateId
            }
          providedMeasurementId = "measurement2"
        }
      )

    val measurements: List<Measurement> =
      measurementsService
        .streamMeasurements(
          streamMeasurementsRequest {
            filter = filter { this.externalMeasurementConsumerId = externalMeasurementConsumerId2 }
          }
        )
        .toList()

    assertThat(measurements).comparingExpectedFieldsOnly().containsExactly(measurement2)
  }

  @Test
  fun `streamMeasurements respects states`(): Unit = runBlocking {
    val measurementConsumer1 = population.createMeasurementConsumer(measurementConsumersService)
    val measurementConsumer2 = population.createMeasurementConsumer(measurementConsumersService)
    val externalMeasurementConsumerId1 = measurementConsumer1.externalMeasurementConsumerId
    val externalMeasurementConsumerId2 = measurementConsumer2.externalMeasurementConsumerId

    val externalMeasurementConsumerCertificateId1 =
      measurementConsumer1.certificate.externalCertificateId
    val externalMeasurementConsumerCertificateId2 =
      measurementConsumer2.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId

    measurementsService.createMeasurement(
      measurement {
        details = MeasurementKt.details { apiVersion = "v2alpha" }
        this.externalMeasurementConsumerId = externalMeasurementConsumerId1
        this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId1
        dataProviders[externalDataProviderId] =
          MeasurementKt.dataProviderValue {
            this.externalDataProviderCertificateId = externalDataProviderCertificateId
          }
        providedMeasurementId = "measurement1"
      }
    )

    val measurement2 =
      measurementsService.createMeasurement(
        measurement {
          details = MeasurementKt.details { apiVersion = "v2alpha" }
          this.externalMeasurementConsumerId = externalMeasurementConsumerId2
          this.externalMeasurementConsumerCertificateId = externalMeasurementConsumerCertificateId2
          dataProviders[externalDataProviderId] =
            MeasurementKt.dataProviderValue {
              this.externalDataProviderCertificateId = externalDataProviderCertificateId
            }
          providedMeasurementId = "measurement2"
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
          streamMeasurementsRequest {
            filter = filter { this.states += Measurement.State.SUCCEEDED }
          }
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
