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
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
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
private val EXTERNAL_DUCHY_IDS = listOf("duchy_1", "duchy_2", "duchy_3")

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
    assertThat(createdMeasurement.state).isEqualTo(Measurement.State.PENDING_REQUISITION_PARAMS)
    assertThat(createdMeasurement)
      .ignoringFields(
        Measurement.EXTERNAL_MEASUREMENT_ID_FIELD_NUMBER,
        Measurement.EXTERNAL_COMPUTATION_ID_FIELD_NUMBER,
        Measurement.CREATE_TIME_FIELD_NUMBER,
        Measurement.UPDATE_TIME_FIELD_NUMBER,
        Measurement.STATE_FIELD_NUMBER
      )
      .isEqualTo(measurement)
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
  fun `getMeasurementByComputationId COMPUTATION View succeeds`() = runBlocking {
    val measurementConsumer = insertMeasurementConsumer()
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalMeasurementConsumerCertificateId =
      measurementConsumer.certificate.externalCertificateId
    val dataProvider = insertDataProvider()
    val externalDataProviderId = dataProvider.externalDataProviderId
    val externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId

    val request =
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

    val createdMeasurement = measurementsService.createMeasurement(request)

    val measurement =
      measurementsService.getMeasurementByComputationId(
        GetMeasurementByComputationIdRequest.newBuilder()
          .apply {
            externalComputationId = createdMeasurement.externalComputationId
            measurementView = Measurement.View.COMPUTATION
          }
          .build()
      )

    // TODO(@uakyol) : Populate the expected proto fields once the field popultaion is implemented.

    val expectedMeasurement =
      createdMeasurement
        .toBuilder()
        .apply {
          clearDataProviders()
          addRequisitionsBuilder().also {
            it.externalMeasurementId = createdMeasurement.externalMeasurementId
            it.externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
          }
          addComputationParticipantsBuilder().also {
            it.externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
            it.externalMeasurementId = createdMeasurement.externalMeasurementId
            it.externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
            it.externalComputationId = createdMeasurement.externalComputationId
            it.state = ComputationParticipant.State.CREATED
          }
          addComputationParticipantsBuilder().also {
            it.externalDuchyId = EXTERNAL_DUCHY_IDS.get(1)
            it.externalMeasurementId = createdMeasurement.externalMeasurementId
            it.externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
            it.externalComputationId = createdMeasurement.externalComputationId
            it.state = ComputationParticipant.State.CREATED
          }
          addComputationParticipantsBuilder().also {
            it.externalDuchyId = EXTERNAL_DUCHY_IDS.get(2)
            it.externalMeasurementId = createdMeasurement.externalMeasurementId
            it.externalMeasurementConsumerId = createdMeasurement.externalMeasurementConsumerId
            it.externalComputationId = createdMeasurement.externalComputationId
            it.state = ComputationParticipant.State.CREATED
          }
        }
        .build()
    assertThat(measurement)
      .ignoringFields(Measurement.CREATE_TIME_FIELD_NUMBER, Measurement.UPDATE_TIME_FIELD_NUMBER)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedMeasurement)
  }
}
