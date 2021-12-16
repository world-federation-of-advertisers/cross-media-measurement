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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerKt.details
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.measurementConsumer

private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 123L
private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")

private val MEASUREMENT_CONSUMER = measurementConsumer {
  certificate =
    certificate {
      notValidBefore = timestamp { seconds = 12345 }
      notValidAfter = timestamp { seconds = 23456 }
      details = CertificateKt.details { x509Der = CERTIFICATE_DER }
    }
  details =
    details {
      apiVersion = "v2alpha"
      publicKey = PUBLIC_KEY
      publicKeySignature = PUBLIC_KEY_SIGNATURE
    }
}

@RunWith(JUnit4::class)
abstract class MeasurementConsumersServiceTest<T : MeasurementConsumersCoroutineImplBase> {

  protected data class Services<T>(
    val measurementConsumersService: T,
    val accountsService: AccountsGrpcKt.AccountsCoroutineImplBase
  )

  protected val clock: Clock = Clock.systemUTC()

  protected val idGenerator =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID)
    )

  private val population = Population(clock, idGenerator)

  protected lateinit var measurementConsumersService: T
    private set

  protected lateinit var accountsService: AccountsGrpcKt.AccountsCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    measurementConsumersService = services.measurementConsumersService
    accountsService = services.accountsService
  }

  @Test
  fun `getMeasurementConsumer fails for missing MeasurementConsumer`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.getMeasurementConsumer(
          GetMeasurementConsumerRequest.newBuilder()
            .setExternalMeasurementConsumerId(EXTERNAL_MEASUREMENT_CONSUMER_ID)
            .build()
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("NOT_FOUND: MeasurementConsumer not found")
  }

  @Test
  fun `createMeasurementConsumer fails for missing fields`() = runBlocking {
    val measurementConsumer = MEASUREMENT_CONSUMER.copy { clearDetails() }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.createMeasurementConsumer(
          createMeasurementConsumerRequest {
            this.measurementConsumer = measurementConsumer
            externalAccountId = 5L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("Details field of MeasurementConsumer is missing fields.")
  }

  @Test
  fun `createMeasurementConsumer fails when creation token has already been used`() = runBlocking {
    val account = population.createActivatedAccount(accountsService)
    val measurementConsumerCreationTokenHash =
      hashSha256(population.createMeasurementConsumerCreationToken(accountsService))
    measurementConsumersService.createMeasurementConsumer(
      createMeasurementConsumerRequest {
        this.measurementConsumer = MEASUREMENT_CONSUMER
        externalAccountId = account.externalAccountId
        this.measurementConsumerCreationTokenHash = measurementConsumerCreationTokenHash
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.createMeasurementConsumer(
          createMeasurementConsumerRequest {
            this.measurementConsumer = MEASUREMENT_CONSUMER
            externalAccountId = account.externalAccountId
            this.measurementConsumerCreationTokenHash = measurementConsumerCreationTokenHash
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception)
      .hasMessageThat()
      .contains("Measurement Consumer creation token is not valid")
  }

  @Test
  fun `createMeasurementConsumer fails when account to be owner has not been activated`() =
      runBlocking {
    val account = accountsService.createAccount(account {})
    val measurementConsumerCreationTokenHash =
      hashSha256(population.createMeasurementConsumerCreationToken(accountsService))

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.createMeasurementConsumer(
          createMeasurementConsumerRequest {
            this.measurementConsumer = MEASUREMENT_CONSUMER
            externalAccountId = account.externalAccountId
            this.measurementConsumerCreationTokenHash = measurementConsumerCreationTokenHash
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Account has not been activated yet")
  }

  @Test
  fun `createMeasurementConsumer fails when account to be owner doesn't exist`() = runBlocking {
    val measurementConsumerCreationTokenHash =
      hashSha256(population.createMeasurementConsumerCreationToken(accountsService))

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.createMeasurementConsumer(
          createMeasurementConsumerRequest {
            this.measurementConsumer = MEASUREMENT_CONSUMER
            externalAccountId = 1L
            this.measurementConsumerCreationTokenHash = measurementConsumerCreationTokenHash
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Account not found")
  }

  @Test
  fun `createMeasurementConsumer succeeds`() = runBlocking {
    val account = population.createActivatedAccount(accountsService)
    val measurementConsumerCreationTokenHash =
      hashSha256(population.createMeasurementConsumerCreationToken(accountsService))

    val createdMeasurementConsumer =
      measurementConsumersService.createMeasurementConsumer(
        createMeasurementConsumerRequest {
          this.measurementConsumer = MEASUREMENT_CONSUMER
          externalAccountId = account.externalAccountId
          this.measurementConsumerCreationTokenHash = measurementConsumerCreationTokenHash
        }
      )

    assertThat(createdMeasurementConsumer)
      .isEqualTo(
        MEASUREMENT_CONSUMER.copy {
          externalMeasurementConsumerId = FIXED_GENERATED_EXTERNAL_ID
          certificate =
            certificate.copy {
              externalMeasurementConsumerId = FIXED_GENERATED_EXTERNAL_ID
              externalCertificateId = FIXED_GENERATED_EXTERNAL_ID
            }
        }
      )
  }

  @Test
  fun `getMeasurementConsumer succeeds`() = runBlocking {
    val account = population.createActivatedAccount(accountsService)
    val measurementConsumerCreationHash =
      hashSha256(population.createMeasurementConsumerCreationToken(accountsService))

    val createdMeasurementConsumer =
      measurementConsumersService.createMeasurementConsumer(
        createMeasurementConsumerRequest {
          this.measurementConsumer = MEASUREMENT_CONSUMER
          externalAccountId = account.externalAccountId
          this.measurementConsumerCreationTokenHash = measurementConsumerCreationHash
        }
      )

    val measurementConsumerRead =
      measurementConsumersService.getMeasurementConsumer(
        GetMeasurementConsumerRequest.newBuilder()
          .setExternalMeasurementConsumerId(
            createdMeasurementConsumer.externalMeasurementConsumerId
          )
          .build()
      )

    assertThat(measurementConsumerRead).isEqualTo(createdMeasurementConsumer)
  }
}
