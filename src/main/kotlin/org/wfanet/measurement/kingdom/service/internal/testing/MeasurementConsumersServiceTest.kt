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
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerKt.details
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.internal.kingdom.addMeasurementConsumerOwnerRequest
import org.wfanet.measurement.internal.kingdom.authenticateAccountRequest
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.measurementConsumer
import org.wfanet.measurement.internal.kingdom.removeMeasurementConsumerOwnerRequest

private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")

private val MEASUREMENT_CONSUMER = measurementConsumer {
  certificate = certificate {
    notValidBefore = timestamp { seconds = 12345 }
    notValidAfter = timestamp { seconds = 23456 }
    details = CertificateKt.details { x509Der = CERTIFICATE_DER }
  }
  details = details {
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
          getMeasurementConsumerRequest { externalMeasurementConsumerId = 1L }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
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
  }

  @Test
  fun `createMeasurementConsumer fails when creation token has already been used`() = runBlocking {
    val account = population.createAccount(accountsService)
    population.activateAccount(accountsService, account)
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

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `createMeasurementConsumer succeeds`() = runBlocking {
    val account = population.createAccount(accountsService)
    population.activateAccount(accountsService, account)
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
      .ignoringFieldDescriptors(
        MeasurementConsumer.getDescriptor()
          .findFieldByNumber(MeasurementConsumer.EXTERNAL_MEASUREMENT_CONSUMER_ID_FIELD_NUMBER),
        Certificate.getDescriptor()
          .findFieldByNumber(Certificate.EXTERNAL_MEASUREMENT_CONSUMER_ID_FIELD_NUMBER),
        Certificate.getDescriptor()
          .findFieldByNumber(Certificate.EXTERNAL_CERTIFICATE_ID_FIELD_NUMBER)
      )
      .isEqualTo(MEASUREMENT_CONSUMER)
    assertThat(createdMeasurementConsumer.externalMeasurementConsumerId).isNotEqualTo(0L)
    assertThat(createdMeasurementConsumer.externalMeasurementConsumerId)
      .isEqualTo(createdMeasurementConsumer.certificate.externalMeasurementConsumerId)
  }

  @Test
  fun `getMeasurementConsumer succeeds`() = runBlocking {
    val account = population.createAccount(accountsService)
    population.activateAccount(accountsService, account)
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

    val readMeasurementConsumer =
      measurementConsumersService.getMeasurementConsumer(
        GetMeasurementConsumerRequest.newBuilder()
          .setExternalMeasurementConsumerId(
            createdMeasurementConsumer.externalMeasurementConsumerId
          )
          .build()
      )

    assertThat(readMeasurementConsumer).isEqualTo(createdMeasurementConsumer)
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
  }

  @Test
  fun `addMeasurementConsumerOwner throws FAILED_PRECONDITION when Account doesn't exist`() =
    runBlocking {
      val account = population.createAccount(accountsService)
      population.activateAccount(accountsService, account)
      val measurementConsumer =
        measurementConsumersService.createMeasurementConsumer(
          createMeasurementConsumerRequest {
            measurementConsumer = MEASUREMENT_CONSUMER
            externalAccountId = account.externalAccountId
            measurementConsumerCreationTokenHash =
              hashSha256(population.createMeasurementConsumerCreationToken(accountsService))
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          measurementConsumersService.addMeasurementConsumerOwner(
            addMeasurementConsumerOwnerRequest {
              externalAccountId = 1L
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `addMeasurementConsumerOwner adds Account as new owner of MC`() = runBlocking {
    val account = population.createAccount(accountsService)
    population.activateAccount(accountsService, account)
    val measurementConsumer =
      measurementConsumersService.createMeasurementConsumer(
        createMeasurementConsumerRequest {
          measurementConsumer = MEASUREMENT_CONSUMER
          externalAccountId = account.externalAccountId
          measurementConsumerCreationTokenHash =
            hashSha256(population.createMeasurementConsumerCreationToken(accountsService))
        }
      )

    idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID + 1L)
    idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID + 1L)
    val account2 = population.createAccount(accountsService)
    val idToken2 = population.activateAccount(accountsService, account2)

    measurementConsumersService.addMeasurementConsumerOwner(
      addMeasurementConsumerOwnerRequest {
        externalAccountId = account2.externalAccountId
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      }
    )

    val openIdConnectIdentity = population.parseIdToken(idToken = idToken2)
    val updatedAccount =
      accountsService.authenticateAccount(
        authenticateAccountRequest { identity = openIdConnectIdentity }
      )

    assertThat(updatedAccount.externalOwnedMeasurementConsumerIdsList)
      .contains(measurementConsumer.externalMeasurementConsumerId)
  }

  @Test
  fun `removeMeasurementConsumerOwner throws NOT_FOUND when MC doesn't exist`() = runBlocking {
    val account = population.createAccount(accountsService)
    population.activateAccount(accountsService, account)

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
  }

  @Test
  fun `removeMeasurementConsumerOwner throws FAILED_PRECONDITION when Account doesn't exist`() =
    runBlocking {
      val account = population.createAccount(accountsService)
      population.activateAccount(accountsService, account)
      val measurementConsumer =
        measurementConsumersService.createMeasurementConsumer(
          createMeasurementConsumerRequest {
            measurementConsumer = MEASUREMENT_CONSUMER
            externalAccountId = account.externalAccountId
            measurementConsumerCreationTokenHash =
              hashSha256(population.createMeasurementConsumerCreationToken(accountsService))
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          measurementConsumersService.removeMeasurementConsumerOwner(
            removeMeasurementConsumerOwnerRequest {
              externalAccountId = 1L
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `removeMeasurementConsumerOwner throws FAILED_PRECONDITION when Account doesn't own MC`() =
    runBlocking {
      val account = population.createAccount(accountsService)
      population.activateAccount(accountsService, account)
      val measurementConsumer =
        measurementConsumersService.createMeasurementConsumer(
          createMeasurementConsumerRequest {
            measurementConsumer = MEASUREMENT_CONSUMER
            externalAccountId = account.externalAccountId
            measurementConsumerCreationTokenHash =
              hashSha256(population.createMeasurementConsumerCreationToken(accountsService))
          }
        )

      idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID + 1L)
      idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID + 1L)
      val account2 = population.createAccount(accountsService)
      population.activateAccount(accountsService, account2)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          measurementConsumersService.removeMeasurementConsumerOwner(
            removeMeasurementConsumerOwnerRequest {
              externalAccountId = account2.externalAccountId
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `removeMeasurementConsumerOwner removes Account as owner of MC`() = runBlocking {
    val account = population.createAccount(accountsService)
    val idToken = population.activateAccount(accountsService, account)

    val measurementConsumer =
      measurementConsumersService.createMeasurementConsumer(
        createMeasurementConsumerRequest {
          measurementConsumer = MEASUREMENT_CONSUMER
          externalAccountId = account.externalAccountId
          measurementConsumerCreationTokenHash =
            hashSha256(population.createMeasurementConsumerCreationToken(accountsService))
        }
      )

    val openIdConnectIdentity = population.parseIdToken(idToken = idToken)
    val authenticateAccountRequest = authenticateAccountRequest { identity = openIdConnectIdentity }
    var updatedAccount = accountsService.authenticateAccount(authenticateAccountRequest)

    assertThat(updatedAccount.externalOwnedMeasurementConsumerIdsList)
      .contains(measurementConsumer.externalMeasurementConsumerId)

    measurementConsumersService.removeMeasurementConsumerOwner(
      removeMeasurementConsumerOwnerRequest {
        externalAccountId = account.externalAccountId
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      }
    )

    updatedAccount = accountsService.authenticateAccount(authenticateAccountRequest)

    assertThat(updatedAccount.externalOwnedMeasurementConsumerIdsList)
      .doesNotContain(measurementConsumer.externalMeasurementConsumerId)
  }
}
