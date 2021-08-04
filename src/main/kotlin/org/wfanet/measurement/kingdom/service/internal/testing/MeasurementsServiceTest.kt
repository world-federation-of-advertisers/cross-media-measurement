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
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private val TEST_INSTANT = Instant.ofEpochMilli(123456789L)
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 123L
private const val FIXED_INTERNAL_ID = 2345L
private const val FIXED_EXTERNAL_ID = 6789L
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

  private suspend fun insertMeasurementConsumer(): Long {
    return measurementConsumersService.createMeasurementConsumer(
      MeasurementConsumer.newBuilder()
        .apply {
              preferredCertificateBuilder.apply {
                notValidBeforeBuilder.seconds = 12345
            notValidAfterBuilder.seconds = 23456
            subjectKeyIdentifier = PREFERRED_MC_SUBJECT_KEY_IDENTIFIER
            detailsBuilder.setX509Der(PREFERRED_MC_CERTIFICATE_DER)
              }
          detailsBuilder.apply {
                apiVersion = "2"
            publicKey = PUBLIC_KEY
            publicKeySignature = PUBLIC_KEY_SIGNATURE
              }
            }
        .build()
    )
      .externalMeasurementConsumerId
  }

  private suspend fun insertDataProvider(): Long {
    return dataProvidersService.createDataProvider(
      DataProvider.newBuilder()
        .apply {
              preferredCertificateBuilder.apply {
                notValidBeforeBuilder.seconds = 12345
            notValidAfterBuilder.seconds = 23456
            subjectKeyIdentifier = PREFERRED_DP_SUBJECT_KEY_IDENTIFIER
            detailsBuilder.setX509Der(PREFERRED_DP_CERTIFICATE_DER)
              }
          detailsBuilder.apply {
                apiVersion = "2"
            publicKey = PUBLIC_KEY
            publicKeySignature = PUBLIC_KEY_SIGNATURE
              }
            }
        .build()
    )
      .externalDataProviderId
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
    val externalMeasurementConsumerId = insertMeasurementConsumer()
    val externalDataProviderId = 0L
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `createMeasurement fails for missing measurement consumer`() = runBlocking {
    val externalMeasurementConsumerId = 0L
    val externalDataProviderId = insertDataProvider()
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
    val externalMeasurementConsumerId = insertMeasurementConsumer()
    val externalDataProviderId = insertDataProvider()
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

    val createdMeasurement = measurementsService.createMeasurement(measurement)

    assertThat(createdMeasurement)
      .isEqualTo(
        measurement
          .toBuilder()
          .apply {
            externalMeasurementId = createdMeasurement.externalMeasurementId
            externalComputationId = createdMeasurement.externalComputationId
            createTime = createdMeasurement.createTime
          }
          .build()
      )
  }

  @Test
  fun `createMeasurement returns already created measurement for the same ProvidedMeasurementId`() =
      runBlocking {
    val externalMeasurementConsumerId = insertMeasurementConsumer()
    val measurement =
      Measurement.newBuilder()
        .also {
          it.detailsBuilder.apiVersion = "v2alpha"
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.providedMeasurementId = PROVIDED_MEASUREMENT_ID
        }
        .build()

    val createdMeasurement = measurementsService.createMeasurement(measurement)
    val secondCreateMeasurementAttempt = measurementsService.createMeasurement(measurement)
    assertThat(secondCreateMeasurementAttempt).isEqualTo(createdMeasurement)
  }

  @Test fun `getMeasurementByComputationId succeeds`() = runBlocking {}
}
