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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest

private const val EXTERNAL_DATA_PROVIDER_ID = 123L
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val PREFERRED_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")

@RunWith(JUnit4::class)
abstract class DataProvidersServiceTest {
  abstract val dataProvidersService: DataProvidersCoroutineImplBase

  @Test
  fun `getDataProvider fails for missing DataProvider`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.getDataProvider(
          GetDataProviderRequest.newBuilder()
            .setExternalDataProviderId(EXTERNAL_DATA_PROVIDER_ID)
            .build()
        )
      }

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
  }

  @Test
  fun `createDataProvider succeeds`() = runBlocking {
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

    assertThat(dataProviderRead).isEqualTo(createdDataProvider)
  }
}
