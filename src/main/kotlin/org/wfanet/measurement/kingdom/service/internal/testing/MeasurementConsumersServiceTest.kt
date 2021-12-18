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
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerKt.details
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.internal.kingdom.addMeasurementConsumerOwnerRequest
import org.wfanet.measurement.internal.kingdom.authenticateAccountRequest
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.getMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.measurementConsumer
import org.wfanet.measurement.internal.kingdom.removeMeasurementConsumerOwnerRequest
import org.wfanet.measurement.kingdom.deploy.common.service.withIdToken

private const val RANDOM_SEED = 1
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")
private const val MEASUREMENT_CONSUMER_CREATION_TOKEN = 12345673L

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
  measurementConsumerCreationToken = MEASUREMENT_CONSUMER_CREATION_TOKEN
}

@RunWith(JUnit4::class)
abstract class MeasurementConsumersServiceTest<T : MeasurementConsumersCoroutineImplBase> {

  protected data class Services<T>(
    val measurementConsumersService: T,
    val accountsService: AccountsGrpcKt.AccountsCoroutineImplBase
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  protected val population = Population(clock, idGenerator)

  protected lateinit var measurementConsumersService: T
    private set

  private lateinit var accountsService: AccountsGrpcKt.AccountsCoroutineImplBase

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
          getMeasurementConsumerRequest { externalMeasurementConsumerId = 1L }
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
        measurementConsumersService.createMeasurementConsumer(measurementConsumer)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("Details field of MeasurementConsumer is missing fields.")
  }

  @Test
  fun `createMeasurementConsumer succeeds`() = runBlocking {
    val createdMeasurementConsumer =
      measurementConsumersService.createMeasurementConsumer(MEASUREMENT_CONSUMER)

    assertThat(MEASUREMENT_CONSUMER)
      .isEqualTo(
        createdMeasurementConsumer.copy {
          clearExternalMeasurementConsumerId()
          certificate =
            certificate.copy {
              clearExternalMeasurementConsumerId()
              clearExternalCertificateId()
            }
        }
      )
  }

  @Test
  fun `getMeasurementConsumer succeeds`() = runBlocking {
    val createdMeasurementConsumer =
      measurementConsumersService.createMeasurementConsumer(MEASUREMENT_CONSUMER)

    val measurementConsumerRead =
      measurementConsumersService.getMeasurementConsumer(
        GetMeasurementConsumerRequest.newBuilder()
          .setExternalMeasurementConsumerId(
            createdMeasurementConsumer.externalMeasurementConsumerId
          )
          .build()
      )

    assertThat(measurementConsumerRead)
      .isEqualTo(createdMeasurementConsumer.copy { clearMeasurementConsumerCreationToken() })
  }

  @Test
  fun `addMeasurementConsumerOwner throws NOT_FOUND when MC doesn't exist`() = runBlocking {
    val account = accountsService.createAccount(account {})

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.addMeasurementConsumerOwner(
          addMeasurementConsumerOwnerRequest {
            externalAccountId = account.externalAccountId
            externalMeasurementConsumerId = 1L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `addMeasurementConsumerOwner throws NOT_FOUND when Account doesn't exist`() = runBlocking {
    val measurementConsumer =
      measurementConsumersService.createMeasurementConsumer(MEASUREMENT_CONSUMER)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.addMeasurementConsumerOwner(
          addMeasurementConsumerOwnerRequest {
            externalAccountId = 1L
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Account not found")
  }

  @Test
  fun `addMeasurementConsumerOwner adds Account as new owner of MC`() = runBlocking {
    val (account, _) = population.createActivatedAccount(accountsService)
    val measurementConsumer =
      measurementConsumersService.createMeasurementConsumer(
        MEASUREMENT_CONSUMER.copy {
          measurementConsumerCreationToken = account.measurementConsumerCreationToken
        }
      )

    val (account2, idToken2) = population.createActivatedAccount(accountsService)

    measurementConsumersService.addMeasurementConsumerOwner(
      addMeasurementConsumerOwnerRequest {
        externalAccountId = account2.externalAccountId
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      }
    )

    val updatedAccount =
      withIdToken(idToken2) {
        runBlocking { accountsService.authenticateAccount(authenticateAccountRequest {}) }
      }

    assertThat(updatedAccount.externalOwnedMeasurementConsumerIdsList)
      .contains(measurementConsumer.externalMeasurementConsumerId)
  }

  @Test
  fun `removeMeasurementConsumerOwner throws NOT_FOUND when MC doesn't exist`() = runBlocking {
    val (account, _) = population.createActivatedAccount(accountsService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.removeMeasurementConsumerOwner(
          removeMeasurementConsumerOwnerRequest {
            externalAccountId = account.externalAccountId
            externalMeasurementConsumerId = 1L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `removeMeasurementConsumerOwner throws NOT_FOUND when Account doesn't exist`() = runBlocking {
    val measurementConsumer =
      measurementConsumersService.createMeasurementConsumer(MEASUREMENT_CONSUMER)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.removeMeasurementConsumerOwner(
          removeMeasurementConsumerOwnerRequest {
            externalAccountId = 1L
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Account not found")
  }

  @Test
  fun `removeMeasurementConsumerOwner throws FAILED_PRECONDITION when Account doesn't own MC`() =
      runBlocking {
    val account = accountsService.createAccount(account {})
    val measurementConsumer =
      measurementConsumersService.createMeasurementConsumer(MEASUREMENT_CONSUMER)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        measurementConsumersService.removeMeasurementConsumerOwner(
          removeMeasurementConsumerOwnerRequest {
            externalAccountId = account.externalAccountId
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("Account doesn't own MeasurementConsumer")
  }

  /**
   * TODO(https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/404/):
   * Remove @Ignore.
   */
  @Ignore
  @Test
  fun `removeMeasurementConsumerOwner removes Account as owner of MC`() = runBlocking {
    val (account, idToken) = population.createActivatedAccount(accountsService)

    val measurementConsumer =
      measurementConsumersService.createMeasurementConsumer(
        MEASUREMENT_CONSUMER.copy {
          measurementConsumerCreationToken = account.measurementConsumerCreationToken
        }
      )

    var updatedAccount =
      withIdToken(idToken) {
        runBlocking { accountsService.authenticateAccount(authenticateAccountRequest {}) }
      }

    assertThat(updatedAccount.externalOwnedMeasurementConsumerIdsList)
      .contains(measurementConsumer.externalMeasurementConsumerId)

    measurementConsumersService.removeMeasurementConsumerOwner(
      removeMeasurementConsumerOwnerRequest {
        externalAccountId = account.externalAccountId
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      }
    )

    updatedAccount =
      withIdToken(idToken) {
        runBlocking { accountsService.authenticateAccount(authenticateAccountRequest {}) }
      }

    assertThat(updatedAccount.externalOwnedMeasurementConsumerIdsList)
      .doesNotContain(measurementConsumer.externalMeasurementConsumerId)
  }
}
