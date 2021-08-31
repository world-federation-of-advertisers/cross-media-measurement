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
import java.time.Instant
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.getMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private val TEST_INSTANT = Instant.ofEpochMilli(123456789L)
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
    val dataProvidersService: DataProvidersCoroutineImplBase
  )

  protected val idGenerator =
    RandomIdGenerator(TestClockWithNamedInstants(TEST_INSTANT), Random(RANDOM_SEED))

  protected lateinit var measurementsService: T
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    measurementConsumersService = services.measurementConsumersService
    measurementsService = services.measurementsService
    dataProvidersService = services.dataProvidersService
  }

  private suspend fun insertDataProvider(): DataProvider {
    return dataProvidersService.createDataProvider(
      DataProvider.newBuilder()
        .apply {
          certificateBuilder.apply {
            notValidBeforeBuilder.seconds = 12345
            notValidAfterBuilder.seconds = 23456
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

  private suspend fun insertMeasurementConsumer(): MeasurementConsumer {
    return measurementConsumersService.createMeasurementConsumer(
      MeasurementConsumer.newBuilder()
        .apply {
          certificateBuilder.apply {
            notValidBeforeBuilder.seconds = 12345
            notValidAfterBuilder.seconds = 23456
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

  @Test
  fun `getMeasurementByComputationId fails for missing Measurement`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementsService.getMeasurementByComputationId(
          GetMeasurementByComputationIdRequest.newBuilder().setExternalComputationId(1L).build()
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
  fun `createMeasurement succeeds`() = runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
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
                  .also { it.externalDataProviderCertificateId = externalDataProviderCertificateId }
                  .build()
            )
          )
          it.providedMeasurementId = PROVIDED_MEASUREMENT_ID
        }
        .build()

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

    assertThat(measurement).isEqualTo(createdMeasurement)
  }

  @Test
  fun `getMeasurementByComputationId COMPUTATION View succeeds`() =
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
            measurementView = Measurement.View.COMPUTATION
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
            this.externalDataProviderCertificateId = externalDataProviderCertificateId
            updateTime = createdMeasurement.createTime
            state = Requisition.State.UNFULFILLED
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
}
