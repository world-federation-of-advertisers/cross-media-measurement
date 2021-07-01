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
<<<<<<< HEAD
import com.google.protobuf.ByteString
=======
>>>>>>> d519ecd3 (setting up)
=======
import com.google.protobuf.ByteString
>>>>>>> cc3034cf (rebased and fixed)
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
<<<<<<< HEAD
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
=======
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
>>>>>>> d519ecd3 (setting up)
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest

<<<<<<< HEAD
<<<<<<< HEAD
private const val EXTERNAL_DATA_PROVIDER_ID = 123L
private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val PREFERRED_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")

@RunWith(JUnit4::class)
abstract class DataProvidersServiceTest<T : DataProvidersCoroutineImplBase> {

  protected val idGenerator =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID)
    )

  protected lateinit var dataProvidersService: T
    private set

  protected abstract fun newService(idGenerator: IdGenerator): T

  @Before
  fun initService() {
    dataProvidersService = newService(idGenerator)
  }
=======
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 123L
=======
private const val EXTERNAL_DATA_PROVIDER_ID = 123L
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val PREFERRED_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")
>>>>>>> cc3034cf (rebased and fixed)

@RunWith(JUnit4::class)
abstract class DataProvidersServiceTest {
<<<<<<< HEAD
  abstract val measurementConsumersService: DataProvidersCoroutineImplBase
>>>>>>> d519ecd3 (setting up)
=======
  abstract val dataProvidersService: DataProvidersCoroutineImplBase
>>>>>>> 030d4904 (ready)

  @Test
  fun `getDataProvider fails for missing DataProvider`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
<<<<<<< HEAD
<<<<<<< HEAD
        dataProvidersService.getDataProvider(
          GetDataProviderRequest.newBuilder()
            .setExternalDataProviderId(EXTERNAL_DATA_PROVIDER_ID)
=======
        measurementConsumersService.getDataProvider(
=======
        dataProvidersService.getDataProvider(
>>>>>>> 030d4904 (ready)
          GetDataProviderRequest.newBuilder()
<<<<<<< HEAD
            .setExternalDataProviderId(EXTERNAL_MEASUREMENT_CONSUMER_ID)
>>>>>>> d519ecd3 (setting up)
=======
            .setExternalDataProviderId(EXTERNAL_DATA_PROVIDER_ID)
>>>>>>> cc3034cf (rebased and fixed)
            .build()
        )
      }

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc3034cf (rebased and fixed)
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createDataProvider fails for missing fields`() = runBlocking {
    val dataProvider =
      DataProvider.newBuilder()
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
        dataProvidersService.createDataProvider(dataProvider)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
<<<<<<< HEAD
=======
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
>>>>>>> d519ecd3 (setting up)
=======
>>>>>>> cc3034cf (rebased and fixed)
  }

  @Test
  fun `createDataProvider succeeds`() = runBlocking {
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc3034cf (rebased and fixed)
    val dataProvider =
      DataProvider.newBuilder()
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
<<<<<<< HEAD
    val createdDataProvider = dataProvidersService.createDataProvider(dataProvider)

    assertThat(createdDataProvider)
      .isEqualTo(
        dataProvider
          .toBuilder()
          .apply {
            externalDataProviderId = FIXED_GENERATED_EXTERNAL_ID
            externalPublicKeyCertificateId = FIXED_GENERATED_EXTERNAL_ID
            preferredCertificateBuilder.apply {
              externalDataProviderId = FIXED_GENERATED_EXTERNAL_ID
              externalCertificateId = FIXED_GENERATED_EXTERNAL_ID
            }
          }
          .build()
      )
  }

  @Test
  fun `getDataProvider succeeds`() = runBlocking {
    val dataProvider =
      DataProvider.newBuilder()
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
    val createdDataProvider = dataProvidersService.createDataProvider(dataProvider)

    val dataProviderRead =
      dataProvidersService.getDataProvider(
        GetDataProviderRequest.newBuilder()
          .setExternalDataProviderId(createdDataProvider.externalDataProviderId)
          .build()
      )

    assertThat(dataProviderRead).isEqualTo(createdDataProvider)
=======
=======
>>>>>>> cc3034cf (rebased and fixed)
    val createdDataProvider =
      dataProvidersService.createDataProvider(dataProvider)
    assertThat(createdDataProvider.externalDataProviderId).isNotEqualTo(0L)
    assertThat(createdDataProvider.preferredCertificate.externalDataProviderId)
      .isEqualTo(createdDataProvider.externalDataProviderId)
    assertThat(createdDataProvider)
      .comparingExpectedFieldsOnly()
      .isEqualTo(dataProvider)
  }

  @Test
  fun `getDataProvider succeeds`() = runBlocking {
    val dataProvider =
      DataProvider.newBuilder()
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
    val createdDataProvider =
      dataProvidersService.createDataProvider(dataProvider)

    val dataProviderRead =
      dataProvidersService.getDataProvider(
        GetDataProviderRequest.newBuilder()
          .setExternalDataProviderId(
            createdDataProvider.externalDataProviderId
          )
          .build()
      )

<<<<<<< HEAD
    assertThat(measurementConsumerRead).isEqualTo(createdDataProvider)
>>>>>>> d519ecd3 (setting up)
=======
    assertThat(dataProviderRead).isEqualTo(createdDataProvider)
>>>>>>> 030d4904 (ready)
  }
}
