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
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.apiKey
import org.wfanet.measurement.internal.kingdom.authenticateApiKeyRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.deleteApiKeyRequest

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ApiKeysServiceTest<T : ApiKeysCoroutineImplBase> {
  protected data class Services<T>(
    val apiKeysService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val accountsService: AccountsCoroutineImplBase,
  )

  private val clock: Clock = Clock.systemUTC()
  private val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  private lateinit var apiKeysService: T

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var accountsService: AccountsCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(idGenerator)
    apiKeysService = services.apiKeysService
    measurementConsumersService = services.measurementConsumersService
    accountsService = services.accountsService
  }

  @Test
  fun `createApiKey with no description returns an api key`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val apiKey = apiKey {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      nickname = "nickname"
    }

    val result = apiKeysService.createApiKey(apiKey)

    assertThat(result).comparingExpectedFieldsOnly().isEqualTo(apiKey)
    assertThat(result.externalApiKeyId).isGreaterThan(0L)
  }

  @Test
  fun `createApiKey with description returns an api key()`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val apiKey = apiKey {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      nickname = "nickname"
      description = "description"
    }

    val result = apiKeysService.createApiKey(apiKey)

    assertThat(result).comparingExpectedFieldsOnly().isEqualTo(apiKey)
    assertThat(result.externalApiKeyId).isGreaterThan(0L)
  }

  @Test
  fun `createApiKey throws NOT_FOUND when measurement consumer doesn't exist`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        apiKeysService.createApiKey(
          apiKey {
            externalMeasurementConsumerId = 1L
            nickname = "nickname"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `deleteApiKey returns the api key`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val apiKey =
      apiKeysService.createApiKey(
        apiKey {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          nickname = "nickname"
        }
      )

    val result =
      apiKeysService.deleteApiKey(
        deleteApiKeyRequest {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          externalApiKeyId = apiKey.externalApiKeyId
        }
      )

    assertThat(result)
      .comparingExpectedFieldsOnly()
      .isEqualTo(apiKey.copy { clearAuthenticationKey() })
  }

  @Test
  fun `deleteApiKey throws NOT_FOUND when measurement consumer doesn't exist`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val apiKey =
      apiKeysService.createApiKey(
        apiKey {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          nickname = "nickname"
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        apiKeysService.deleteApiKey(
          deleteApiKeyRequest {
            if (externalMeasurementConsumerId == 1L) {
              this.externalMeasurementConsumerId = 2L
            } else {
              this.externalMeasurementConsumerId = 1L
            }
            externalApiKeyId = apiKey.externalApiKeyId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `deleteApiKey throws NOT FOUND when api key doesn't exist`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val apiKey = apiKey {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      nickname = "nickname"
    }

    val createdApiKey = apiKeysService.createApiKey(apiKey)

    apiKeysService.deleteApiKey(
      deleteApiKeyRequest {
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        externalApiKeyId = createdApiKey.externalApiKeyId
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        apiKeysService.deleteApiKey(
          deleteApiKeyRequest {
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            externalApiKeyId = createdApiKey.externalApiKeyId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Api Key not found")
  }

  @Test
  fun `authenticateApiKey returns a measurement consumer`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val apiKey =
      apiKeysService.createApiKey(
        apiKey {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          nickname = "nickname"
        }
      )

    val result =
      apiKeysService.authenticateApiKey(
        authenticateApiKeyRequest { authenticationKeyHash = hashSha256(apiKey.authenticationKey) }
      )

    assertThat(result).comparingExpectedFieldsOnly().isEqualTo(measurementConsumer)
  }

  @Test
  fun `authenticateApiKey throws INVALID_ARGUMENT when authentication key hash is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          apiKeysService.authenticateApiKey(authenticateApiKeyRequest {})
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("authentication_key_hash")
    }

  @Test
  fun `authenticateApiKey throws NOT_FOUND when authentication key hash doesn't match`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          apiKeysService.authenticateApiKey(
            authenticateApiKeyRequest { authenticationKeyHash = hashSha256(1L) }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception).hasMessageThat().contains("ApiKey")
    }

  @Test
  fun `authenticateApiKey throws NOT_FOUND when api key has been deleted`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val apiKey =
      apiKeysService.createApiKey(
        apiKey {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          nickname = "nickname"
        }
      )

    apiKeysService.deleteApiKey(
      deleteApiKeyRequest {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalApiKeyId = apiKey.externalApiKeyId
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        apiKeysService.authenticateApiKey(
          authenticateApiKeyRequest { authenticationKeyHash = hashSha256(apiKey.authenticationKey) }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ApiKey")
  }
}
