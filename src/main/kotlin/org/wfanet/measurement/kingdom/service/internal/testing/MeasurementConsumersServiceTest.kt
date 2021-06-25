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
<<<<<<< HEAD
import com.google.protobuf.ByteString
=======
>>>>>>> 47e4ba8d (initial commit)
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
<<<<<<< HEAD
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
=======
>>>>>>> 47e4ba8d (initial commit)
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase

private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 123L
<<<<<<< HEAD
private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val PREFERRED_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")

@RunWith(JUnit4::class)
abstract class MeasurementConsumersServiceTest<T : MeasurementConsumersCoroutineImplBase> {

  protected val idGenerator =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID)
    )

  protected lateinit var measurementConsumersService: T
    private set

  protected abstract fun newService(idGenerator: IdGenerator): T

  @Before
  fun initService() {
    measurementConsumersService = newService(idGenerator)
  }
=======

@RunWith(JUnit4::class)
abstract class MeasurementConsumersServiceTest {
  abstract val service: MeasurementConsumersCoroutineImplBase
>>>>>>> 47e4ba8d (initial commit)

  @Test
  fun `getMeasurementConsumer fails for missing MeasurementConsumer`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
<<<<<<< HEAD
        measurementConsumersService.getMeasurementConsumer(
=======
        service.getMeasurementConsumer(
>>>>>>> 47e4ba8d (initial commit)
          GetMeasurementConsumerRequest.newBuilder()
            .setExternalMeasurementConsumerId(EXTERNAL_MEASUREMENT_CONSUMER_ID)
            .build()
        )
      }

<<<<<<< HEAD
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createMeasurementConsumer fails for missing fields`() = runBlocking {
    val measurementConsumer =
      MeasurementConsumer.newBuilder()
        .apply {
          preferredCertificateBuilder.apply {
            notValidBeforeBuilder.seconds = 12345
            notValidAfterBuilder.seconds = 23456
            detailsBuilder.setX509Der(PREFERRED_CERTIFICATE_DER)
          }
          detailsBuilder.apply {
            apiVersion = "2"
            publicKey = PUBLIC_KEY
          }
        }
        .build()
    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.createMeasurementConsumer(measurementConsumer)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
=======
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
>>>>>>> 47e4ba8d (initial commit)
  }

  @Test
  fun `createMeasurementConsumer succeeds`() = runBlocking {
<<<<<<< HEAD
    val measurementConsumer =
<<<<<<< HEAD
      MeasurementConsumer.newBuilder()
        .apply {
          preferredCertificateBuilder.apply {
            notValidBeforeBuilder.seconds = 12345
            notValidAfterBuilder.seconds = 23456
            detailsBuilder.setX509Der(PREFERRED_CERTIFICATE_DER)
          }
          detailsBuilder.apply {
            apiVersion = "2"
            publicKey = PUBLIC_KEY
            publicKeySignature = PUBLIC_KEY_SIGNATURE
          }
        }
        .build()
    val createdMeasurementConsumer =
      measurementConsumersService.createMeasurementConsumer(measurementConsumer)

    assertThat(createdMeasurementConsumer)
      .isEqualTo(
        measurementConsumer
          .toBuilder()
          .apply {
            externalMeasurementConsumerId = FIXED_GENERATED_EXTERNAL_ID
            externalPublicKeyCertificateId = FIXED_GENERATED_EXTERNAL_ID
            preferredCertificateBuilder.apply {
              externalMeasurementConsumerId = FIXED_GENERATED_EXTERNAL_ID
              externalCertificateId = FIXED_GENERATED_EXTERNAL_ID
            }
          }
          .build()
      )
  }

  @Test
  fun `getMeasurementConsumer succeeds`() = runBlocking {
    val measurementConsumer =
      MeasurementConsumer.newBuilder()
        .apply {
          preferredCertificateBuilder.apply {
            notValidBeforeBuilder.seconds = 12345
            notValidAfterBuilder.seconds = 23456
            detailsBuilder.setX509Der(PREFERRED_CERTIFICATE_DER)
          }
          detailsBuilder.apply {
            apiVersion = "2"
            publicKey = PUBLIC_KEY
            publicKeySignature = PUBLIC_KEY_SIGNATURE
          }
        }
        .build()
    val createdMeasurementConsumer =
      measurementConsumersService.createMeasurementConsumer(measurementConsumer)

    val measurementConsumerRead =
      measurementConsumersService.getMeasurementConsumer(
        GetMeasurementConsumerRequest.newBuilder()
          .setExternalMeasurementConsumerId(
            createdMeasurementConsumer.externalMeasurementConsumerId
          )
          .build()
      )

    assertThat(measurementConsumerRead).isEqualTo(createdMeasurementConsumer)
=======
=======
    val createdMeasurementConsumer =
>>>>>>> 1df833ae (testing)
      service.createMeasurementConsumer(
        MeasurementConsumer.newBuilder().apply { detailsBuilder.apply { apiVersion = "2" } }.build()
      )

<<<<<<< HEAD
    assertThat(measurementConsumer).isEqualTo(measurementConsumer)
>>>>>>> 47e4ba8d (initial commit)
=======
    val measurementConsumerRead =
      service.getMeasurementConsumer(
        GetMeasurementConsumerRequest.newBuilder()
          .setExternalMeasurementConsumerId(
            createdMeasurementConsumer.externalMeasurementConsumerId
          )
          .build()
      )

    assertThat(measurementConsumerRead).isEqualTo(createdMeasurementConsumer)
>>>>>>> 1df833ae (testing)
  }
}
