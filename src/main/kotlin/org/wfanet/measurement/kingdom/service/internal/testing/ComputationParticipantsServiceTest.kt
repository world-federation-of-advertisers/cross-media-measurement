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
import java.time.Instant
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.Rule
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.setParticipantRequisitionParamsRequest
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
abstract class ComputationParticipantsServiceTest<T : ComputationParticipantsCoroutineImplBase> {

  @get:Rule val duchyIdSetter = DuchyIdSetter(EXTERNAL_DUCHY_IDS)

  protected data class Services<T>(
    val computationParticipantsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val measurementsService: MeasurementsCoroutineImplBase,
  )

  protected val idGenerator =
    RandomIdGenerator(TestClockWithNamedInstants(TEST_INSTANT), Random(RANDOM_SEED))

  protected lateinit var computationParticipantsService: T
    private set

  protected lateinit var measurementsService: MeasurementsCoroutineImplBase
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    computationParticipantsService = services.computationParticipantsService
    measurementConsumersService = services.measurementConsumersService
    dataProvidersService = services.dataProvidersService
    measurementsService = services.measurementsService
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

  private suspend fun insertMeasurement(): Measurement {
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

    return measurementsService.createMeasurement(measurement)
  }

  @Test
  fun `setParticipantRequisitionParams succeeds for non-last duchy`() = runBlocking {
    val measurement = insertMeasurement()
    val request = setParticipantRequisitionParamsRequest {
      externalComputationId = measurement.externalComputationId
      externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
      externalDuchyCertificateId = 5L
      // protocol.liquidLegionsV2Builder.apply {
      //   elGamalPublicKey = PUBLIC_KEY_SIGNATURE
      //   elGamalPublicKeySignature = PUBLIC_KEY_SIGNATURE
      // }
    }
    val computationParticipant =
      computationParticipantsService.setParticipantRequisitionParams(request)
    assertThat(computationParticipant)
      .ignoringFields(ComputationParticipant.EXTERNAL_DUCHY_CERTIFICATE_ID_FIELD_NUMBER)
      .isNotEqualTo(computationParticipant)
  }

  //   @Ignore @Test fun `setParticipantRequisitionParams succeeds for last duchy`() = runBlocking
  // {}

  //   @Ignore @Test fun `confirmComputationParticipant succeeds for non-last duchy`() = runBlocking
  // {}
  //   @Ignore @Test fun `confirmComputationParticipant succeeds for last duchy`() = runBlocking {}

  //   @Ignore @Test fun `failComputationParticipant succeeds`() = runBlocking {}
}
