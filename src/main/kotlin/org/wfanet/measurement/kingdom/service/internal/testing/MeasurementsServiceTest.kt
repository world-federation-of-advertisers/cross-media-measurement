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
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase

private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 123L
private const val FIXED_INTERNAL_ID = 2345L
private const val FIXED_EXTERNAL_ID = 6789L
private const val PROVIDED_MEASUREMENT_ID = "ProvidedMeasurementId"
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val PREFERRED_MC_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a MC certificate der.")
private val PREFERRED_MC_SUBJECT_KEY_IDENTIFIER =
  ByteString.copyFromUtf8("This is a MC subject key identifier.")

@RunWith(JUnit4::class)
abstract class MeasurementsServiceTest<T : MeasurementsCoroutineImplBase> {

  protected data class Services<T>(
    val measurementsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase
  )

  protected val idGenerator =
    FixedIdGenerator(InternalId(FIXED_INTERNAL_ID), ExternalId(FIXED_EXTERNAL_ID))

  protected lateinit var measurementsService: T
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    measurementConsumersService = services.measurementConsumersService
    measurementsService = services.measurementsService
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

  @Test fun `getMeasurement fails for missing Measurement`() = runBlocking {}

  @Test
  fun `createMeasurement fails for missing fields`() = runBlocking {
    val externalMeasurementConsumerId = insertMeasurementConsumer()
    val measurement =
      Measurement.newBuilder()
        .also {
          it.detailsBuilder.apiVersion = "v2alpha"
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.providedMeasurementId = PROVIDED_MEASUREMENT_ID
          it.putAllDataProviders(
            mapOf(
              1L to
                Measurement.DataProviderValue.newBuilder()
                  .apply { externalDataProviderCertificateId = 0L }
                  .build(),
              2L to
                Measurement.DataProviderValue.newBuilder()
                  .apply { externalDataProviderCertificateId = 1L }
                  .build()
            )
          )
        }
        .build()

    val createdMeasurement = measurementsService.createMeasurement(measurement)
  }

  @Test fun `createMeasurement succeeds`() = runBlocking {}

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

  @Test fun `getMeasurement succeeds`() = runBlocking {}
}
