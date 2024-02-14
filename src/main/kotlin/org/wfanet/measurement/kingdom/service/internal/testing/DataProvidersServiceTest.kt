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
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.internal.kingdom.replaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.internal.kingdom.replaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

private const val EXTERNAL_DATA_PROVIDER_ID = 123L
private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private const val PUBLIC_KEY_SIGNATURE_ALGORITHM_OID = "2.9999"
private val CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")

@RunWith(JUnit4::class)
abstract class DataProvidersServiceTest<T : DataProvidersCoroutineImplBase> {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHIES)

  protected val idGenerator =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID),
    )

  protected lateinit var dataProvidersService: T
    private set

  protected abstract fun newService(idGenerator: IdGenerator): T

  @Before
  fun initService() {
    dataProvidersService = newService(idGenerator)
  }

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
    assertThat(exception).hasMessageThat().contains("NOT_FOUND: DataProvider not found")
  }

  @Test
  fun `createDataProvider fails for missing fields`() = runBlocking {
    val dataProvider =
      DataProvider.newBuilder()
        .apply {
          certificateBuilder.apply {
            notValidBeforeBuilder.seconds = 12345
            notValidAfterBuilder.seconds = 23456
            detailsBuilder.x509Der = CERTIFICATE_DER
          }
          detailsBuilder.apply {
            apiVersion = "v2alpha"
            publicKey = PUBLIC_KEY
          }
        }
        .build()
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.createDataProvider(dataProvider)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("Details field of DataProvider is missing fields.")
  }

  @Test
  fun `createDataProvider returns created DataProvider`() = runBlocking {
    val request =
      DATA_PROVIDER.copy {
        clearExternalDataProviderId()
        certificate =
          certificate.copy {
            clearExternalDataProviderId()
            clearExternalCertificateId()
          }
      }

    val response: DataProvider = dataProvidersService.createDataProvider(request)

    assertThat(response).isEqualTo(DATA_PROVIDER)
  }

  @Test
  fun `createDataProvider returns created DataProvider when requiredExternalDuchyIds is empty`() =
    runBlocking {
      val request = DATA_PROVIDER.copy { requiredExternalDuchyIds.clear() }

      val response: DataProvider = dataProvidersService.createDataProvider(request)

      assertThat(response).isEqualTo(DATA_PROVIDER.copy { requiredExternalDuchyIds.clear() })
    }

  @Test
  fun `getDataProvider returns created DataProvider`() = runBlocking {
    val dataProvider: DataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)

    val response =
      dataProvidersService.getDataProvider(
        GetDataProviderRequest.newBuilder()
          .setExternalDataProviderId(dataProvider.externalDataProviderId)
          .build()
      )

    assertThat(response).isEqualTo(dataProvider)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies succeeds`() = runBlocking {
    val dataProvider =
      dataProvidersService.createDataProvider(
        dataProvider {
          certificate {
            notValidBefore = timestamp { seconds = 12345 }
            notValidAfter = timestamp { seconds = 23456 }
            details = CertificateKt.details { x509Der = CERTIFICATE_DER }
          }
          details =
            DataProviderKt.details {
              apiVersion = "v2alpha"
              publicKey = PUBLIC_KEY
              publicKeySignature = PUBLIC_KEY_SIGNATURE
              publicKeySignatureAlgorithmOid = PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
            }
          requiredExternalDuchyIds += DUCHIES.map { it.externalDuchyId }
        }
      )
    val desiredDuchyList = listOf(Population.AGGREGATOR_DUCHY.externalDuchyId)

    val updatedDataProvider =
      dataProvidersService.replaceDataProviderRequiredDuchies(
        replaceDataProviderRequiredDuchiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          requiredExternalDuchyIds += desiredDuchyList
        }
      )

    // Ensure DataProvider with updated duchy list is returned from function.
    assertThat(updatedDataProvider.requiredExternalDuchyIdsList).isEqualTo(desiredDuchyList)
    // Ensure changes were persisted.
    assertThat(
        dataProvidersService.getDataProvider(
          getDataProviderRequest { externalDataProviderId = dataProvider.externalDataProviderId }
        )
      )
      .isEqualTo(updatedDataProvider)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws NOT_FOUND when edp not found`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.replaceDataProviderRequiredDuchies(
          replaceDataProviderRequiredDuchiesRequest {
            externalDataProviderId = 123
            requiredExternalDuchyIds += listOf(Population.AGGREGATOR_DUCHY.externalDuchyId)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws INVALID_ARGUMENT when no edp id`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.replaceDataProviderRequiredDuchies(
          replaceDataProviderRequiredDuchiesRequest {
            requiredExternalDuchyIds += listOf(Population.AGGREGATOR_DUCHY.externalDuchyId)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceDataAvailabilityInterval modifies DataProvider`() = runBlocking {
    val dataAvailabilityInterval = interval {
      startTime = timestamp { seconds = 200 }
      endTime = timestamp { seconds = 300 }
    }
    val dataProvider =
      dataProvidersService.createDataProvider(
        dataProvider {
          certificate {
            notValidBefore = timestamp { seconds = 12345 }
            notValidAfter = timestamp { seconds = 23456 }
            details = CertificateKt.details { x509Der = CERTIFICATE_DER }
          }
          details =
            DataProviderKt.details {
              apiVersion = "v2alpha"
              publicKey = PUBLIC_KEY
              publicKeySignature = PUBLIC_KEY_SIGNATURE
              publicKeySignatureAlgorithmOid = PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
              this.dataAvailabilityInterval = dataAvailabilityInterval
            }
          requiredExternalDuchyIds += DUCHIES.map { it.externalDuchyId }
        }
      )

    val updatedDataProvider =
      dataProvidersService.replaceDataAvailabilityInterval(
        replaceDataAvailabilityIntervalRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          this.dataAvailabilityInterval = dataAvailabilityInterval
        }
      )

    assertThat(updatedDataProvider.details.dataAvailabilityInterval)
      .isEqualTo(dataAvailabilityInterval)
    // Ensure changes were persisted.
    assertThat(
        dataProvidersService.getDataProvider(
          getDataProviderRequest { externalDataProviderId = dataProvider.externalDataProviderId }
        )
      )
      .isEqualTo(updatedDataProvider)
  }

  @Test
  fun `replaceDataAvailabilityInterval throws NOT_FOUND when edp not found`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.replaceDataAvailabilityInterval(
          replaceDataAvailabilityIntervalRequest {
            externalDataProviderId = 123
            dataAvailabilityInterval = interval {
              startTime = timestamp { seconds = 200 }
              endTime = timestamp { seconds = 300 }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `replaceDataAvailabilityInterval throws NOT_FOUND when no edp id`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        dataProvidersService.replaceDataAvailabilityInterval(
          replaceDataAvailabilityIntervalRequest {
            dataAvailabilityInterval = interval {
              startTime = timestamp { seconds = 200 }
              endTime = timestamp { seconds = 300 }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceDataProviderCapabilites updates DataProvider`() = runBlocking {
    val dataProvider: DataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)
    val capabilities = DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true }

    val response: DataProvider =
      dataProvidersService.replaceDataProviderCapabilities(
        replaceDataProviderCapabilitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          this.capabilities = capabilities
        }
      )

    assertThat(response.details.capabilities).isEqualTo(capabilities)
    // Ensure changes were persisted.
    assertThat(
        dataProvidersService.getDataProvider(
          getDataProviderRequest { externalDataProviderId = dataProvider.externalDataProviderId }
        )
      )
      .isEqualTo(response)
  }

  companion object {
    private val DATA_PROVIDER = dataProvider {
      externalDataProviderId = FIXED_GENERATED_EXTERNAL_ID
      certificate = certificate {
        externalDataProviderId = FIXED_GENERATED_EXTERNAL_ID
        externalCertificateId = FIXED_GENERATED_EXTERNAL_ID
        notValidBefore = timestamp { seconds = 12345 }
        notValidAfter = timestamp { seconds = 23456 }
        details = CertificateKt.details { x509Der = CERTIFICATE_DER }
      }
      details =
        DataProviderKt.details {
          apiVersion = "v2alpha"
          publicKey = PUBLIC_KEY
          publicKeySignature = PUBLIC_KEY_SIGNATURE
          publicKeySignatureAlgorithmOid = PUBLIC_KEY_SIGNATURE_ALGORITHM_OID
        }
      requiredExternalDuchyIds += DUCHIES.map { it.externalDuchyId }
    }
  }
}
