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
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest
import org.wfanet.measurement.internal.kingdom.batchGetDataProvidersRequest
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.internal.kingdom.replaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

@RunWith(JUnit4::class)
abstract class DataProvidersServiceTest<T : DataProvidersCoroutineImplBase> {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHIES)

  private val recordingIdGenerator = RecordingIdGenerator()
  protected val idGenerator: IdGenerator
    get() = recordingIdGenerator

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
          getDataProviderRequest { externalDataProviderId = 404L }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider")
  }

  @Test
  fun `createDataProvider fails for missing fields`() = runBlocking {
    val request =
      CREATE_DATA_PROVIDER_REQUEST.copy { details = details.copy { clearPublicKeySignature() } }
    val exception =
      assertFailsWith<StatusRuntimeException> { dataProvidersService.createDataProvider(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("Details field of DataProvider is missing fields.")
  }

  @Test
  fun `createDataProvider succeeds`() = runBlocking {
    val request = CREATE_DATA_PROVIDER_REQUEST

    val response: DataProvider = dataProvidersService.createDataProvider(request)

    assertThat(recordingIdGenerator.externalIds).hasSize(2)
    val remainingExternalIds = recordingIdGenerator.externalIds.toMutableSet()
    assertThat(response).ignoringFieldDescriptors(EXTERNAL_ID_FIELD_DESCRIPTORS).isEqualTo(request)
    val externalDataProviderId = ExternalId(response.externalDataProviderId)
    assertThat(externalDataProviderId).isIn(remainingExternalIds)
    remainingExternalIds.remove(externalDataProviderId)
    assertThat(ExternalId(response.certificate.externalDataProviderId))
      .isEqualTo(externalDataProviderId)
    assertThat(ExternalId(response.certificate.externalCertificateId)).isIn(remainingExternalIds)
  }

  @Test
  fun `createDataProvider succeeds when requiredExternalDuchyIds is empty`() = runBlocking {
    val request = CREATE_DATA_PROVIDER_REQUEST.copy { requiredExternalDuchyIds.clear() }

    val response = dataProvidersService.createDataProvider(request)

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
      .ignoringFieldDescriptors(EXTERNAL_ID_FIELD_DESCRIPTORS)
      .isEqualTo(request)
  }

  @Test
  fun `getDataProvider succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(CREATE_DATA_PROVIDER_REQUEST)

    val response =
      dataProvidersService.getDataProvider(
        GetDataProviderRequest.newBuilder()
          .setExternalDataProviderId(dataProvider.externalDataProviderId)
          .build()
      )

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
      .isEqualTo(dataProvider)
  }

  @Test
  fun `batchGetDataProviders returns DataProviders in request order`() {
    val dataProviders = runBlocking {
      listOf(
        dataProvidersService.createDataProvider(CREATE_DATA_PROVIDER_REQUEST),
        dataProvidersService.createDataProvider(
          CREATE_DATA_PROVIDER_REQUEST.copy {
            certificate =
              certificate.copy {
                subjectKeyIdentifier = subjectKeyIdentifier.concat(ByteString.copyFromUtf8("2"))
              }
          }
        ),
        dataProvidersService.createDataProvider(
          CREATE_DATA_PROVIDER_REQUEST.copy {
            certificate =
              certificate.copy {
                subjectKeyIdentifier = subjectKeyIdentifier.concat(ByteString.copyFromUtf8("3"))
              }
          }
        ),
      )
    }
    val shuffledDataProviders = dataProviders.shuffled()
    val request = batchGetDataProvidersRequest {
      externalDataProviderIds += shuffledDataProviders.map { it.externalDataProviderId }
    }

    val response = runBlocking { dataProvidersService.batchGetDataProviders(request) }

    assertThat(response.dataProvidersList)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
      .containsExactlyElementsIn(shuffledDataProviders)
      .inOrder()
  }

  @Test
  fun `replaceDataProviderRequiredDuchies succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(CREATE_DATA_PROVIDER_REQUEST)
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
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
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
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELD_DESCRIPTORS)
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

  /** Random [IdGenerator] which records generated IDs. */
  private class RecordingIdGenerator : IdGenerator {
    private val delegate = RandomIdGenerator()
    private val mutableInternalIds: MutableList<InternalId> = mutableListOf()
    private val mutableExternalIds: MutableList<ExternalId> = mutableListOf()

    val internalIds: List<InternalId>
      get() = mutableInternalIds

    val externalIds: List<ExternalId>
      get() = mutableExternalIds

    override fun generateExternalId(): ExternalId {
      return delegate.generateExternalId().also { mutableExternalIds.add(it) }
    }

    override fun generateInternalId(): InternalId {
      return delegate.generateInternalId().also { mutableInternalIds.add(it) }
    }
  }

  companion object {
    private val EXTERNAL_ID_FIELD_DESCRIPTORS =
      listOf(
        DataProvider.getDescriptor()
          .findFieldByNumber(DataProvider.EXTERNAL_DATA_PROVIDER_ID_FIELD_NUMBER),
        Certificate.getDescriptor()
          .findFieldByNumber(Certificate.EXTERNAL_DATA_PROVIDER_ID_FIELD_NUMBER),
        Certificate.getDescriptor()
          .findFieldByNumber(Certificate.EXTERNAL_CERTIFICATE_ID_FIELD_NUMBER),
      )
    private val UNORDERED_FIELD_DESCRIPTORS =
      listOf(
        DataProvider.getDescriptor()
          .findFieldByNumber(DataProvider.REQUIRED_EXTERNAL_DUCHY_IDS_FIELD_NUMBER)
      )

    private const val PUBLIC_KEY_SIGNATURE_ALGORITHM_OID = "2.9999"
    private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
    private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
    private val CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")
    private val CERTIFICATE_SKID = ByteString.copyFromUtf8("Certificate SKID")
    private val CREATE_DATA_PROVIDER_REQUEST = dataProvider {
      certificate = certificate {
        notValidBefore = timestamp { seconds = 12345 }
        notValidAfter = timestamp { seconds = 23456 }
        subjectKeyIdentifier = CERTIFICATE_SKID
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
