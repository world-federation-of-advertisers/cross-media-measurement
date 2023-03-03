// Copyright 2022 The Cross-Media Measurement Authors
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
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
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
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt.PublicKeysCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.updatePublicKeyRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private const val API_VERSION = "v2alpha"

private val PUBLIC_KEY = ByteString.copyFromUtf8("public key")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("public key signature")

@RunWith(JUnit4::class)
abstract class PublicKeysServiceTest<T : PublicKeysCoroutineImplBase> {

  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)

  protected data class Services<T>(
    val publicKeysService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val certificatesService: CertificatesCoroutineImplBase,
    val accountsService: AccountsCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  private lateinit var publicKeysService: T

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var certificatesService: CertificatesCoroutineImplBase
    private set

  protected lateinit var accountsService: AccountsCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    publicKeysService = services.publicKeysService
    measurementConsumersService = services.measurementConsumersService
    dataProvidersService = services.dataProvidersService
    certificatesService = services.certificatesService
    accountsService = services.accountsService
  }

  @Test
  fun `updatePublicKey updates the public key info for a data provider`() = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)

    publicKeysService.updatePublicKey(
      updatePublicKeyRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalCertificateId = dataProvider.certificate.externalCertificateId
        apiVersion = API_VERSION
        publicKey = dataProvider.details.publicKey.concat(PUBLIC_KEY)
        publicKeySignature = dataProvider.details.publicKeySignature.concat(PUBLIC_KEY_SIGNATURE)
      }
    )

    val updatedDataProvider =
      dataProvidersService.getDataProvider(
        getDataProviderRequest { externalDataProviderId = dataProvider.externalDataProviderId }
      )

    assertThat(dataProvider.details.publicKey).isNotEqualTo(updatedDataProvider.details.publicKey)
    assertThat(dataProvider.details.publicKeySignature)
      .isNotEqualTo(updatedDataProvider.details.publicKeySignature)
  }

  @Test
  fun `updatePublicKey updates the public key info for a measurement consumer`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    publicKeysService.updatePublicKey(
      updatePublicKeyRequest {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        externalCertificateId = measurementConsumer.certificate.externalCertificateId
        apiVersion = API_VERSION
        publicKey = measurementConsumer.details.publicKey.concat(PUBLIC_KEY)
        publicKeySignature =
          measurementConsumer.details.publicKeySignature.concat(PUBLIC_KEY_SIGNATURE)
      }
    )

    val updatedMeasurementConsumer =
      measurementConsumersService.getMeasurementConsumer(
        getMeasurementConsumerRequest {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        }
      )

    assertThat(measurementConsumer.details.publicKey)
      .isNotEqualTo(updatedMeasurementConsumer.details.publicKey)
    assertThat(measurementConsumer.details.publicKeySignature)
      .isNotEqualTo(updatedMeasurementConsumer.details.publicKeySignature)
  }

  @Test
  fun `updatePublicKey throws NOT_FOUND when mc certificate not found`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        publicKeysService.updatePublicKey(
          updatePublicKeyRequest {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalCertificateId = measurementConsumer.certificate.externalCertificateId + 1L
            apiVersion = API_VERSION
            publicKey = measurementConsumer.details.publicKey.concat(PUBLIC_KEY)
            publicKeySignature =
              measurementConsumer.details.publicKeySignature.concat(PUBLIC_KEY_SIGNATURE)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `updatePublicKey throws NOT_FOUND when data provider certificate not found`() = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        publicKeysService.updatePublicKey(
          updatePublicKeyRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalCertificateId = dataProvider.certificate.externalCertificateId + 1L
            apiVersion = API_VERSION
            publicKey = dataProvider.details.publicKey.concat(PUBLIC_KEY)
            publicKeySignature =
              dataProvider.details.publicKeySignature.concat(PUBLIC_KEY_SIGNATURE)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `updatePublicKey throws NOT_FOUND when measurement consumer not found`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        publicKeysService.updatePublicKey(
          updatePublicKeyRequest {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId + 1L
            externalCertificateId = measurementConsumer.certificate.externalCertificateId
            apiVersion = API_VERSION
            publicKey = measurementConsumer.details.publicKey.concat(PUBLIC_KEY)
            publicKeySignature =
              measurementConsumer.details.publicKeySignature.concat(PUBLIC_KEY_SIGNATURE)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `updatePublicKey throws NOT_FOUND when data provider not found`() = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        publicKeysService.updatePublicKey(
          updatePublicKeyRequest {
            externalDataProviderId = dataProvider.externalDataProviderId + 1L
            externalCertificateId = dataProvider.certificate.externalCertificateId
            apiVersion = API_VERSION
            publicKey = dataProvider.details.publicKey.concat(PUBLIC_KEY)
            publicKeySignature =
              dataProvider.details.publicKeySignature.concat(PUBLIC_KEY_SIGNATURE)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }
}
