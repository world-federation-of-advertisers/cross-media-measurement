// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.security.cert.X509Certificate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.DuchyPrincipal
import org.wfanet.measurement.api.v2alpha.ListCertificatesPageToken
import org.wfanet.measurement.api.v2alpha.ListCertificatesPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListCertificatesRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ListCertificatesResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.listCertificatesPageToken
import org.wfanet.measurement.api.v2alpha.listCertificatesRequest
import org.wfanet.measurement.api.v2alpha.listCertificatesResponse
import org.wfanet.measurement.api.v2alpha.releaseCertificateHoldRequest
import org.wfanet.measurement.api.v2alpha.revokeCertificateRequest
import org.wfanet.measurement.api.v2alpha.testing.makeModelProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withDuchyPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate as InternalCertificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase as InternalCertificatesCoroutineService
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub as InternalCertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest as InternalGetCertificateRequest
import org.wfanet.measurement.internal.kingdom.ReleaseCertificateHoldRequest as InternalReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.RevokeCertificateRequest as InternalRevokeCertificateRequest
import org.wfanet.measurement.internal.kingdom.StreamCertificatesRequestKt
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.certificateDetails
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.getCertificateRequest as internalGetCertificateRequest
import org.wfanet.measurement.internal.kingdom.releaseCertificateHoldRequest as internalReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.revokeCertificateRequest as internalRevokeCertificateRequest
import org.wfanet.measurement.internal.kingdom.streamCertificatesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertSubjectKeyIdAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateRevocationStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelProviderNotFoundException

private const val EXTERNAL_MODEL_PROVIDER_ID = 23456L
private const val EXTERNAL_MODEL_PROVIDER_ID_2 = 23457L
private val MODEL_PROVIDER_NAME = makeModelProvider(EXTERNAL_MODEL_PROVIDER_ID)
private val MODEL_PROVIDER_NAME_2 = makeModelProvider(EXTERNAL_MODEL_PROVIDER_ID_2)
private val MODEL_PROVIDER_CERTIFICATE_NAME = "$MODEL_PROVIDER_NAME/certificates/AAAAAAAAAcg"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME_2 = "measurementConsumers/BBBBBBBBBHs"
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
  "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME_2 =
  "$MEASUREMENT_CONSUMER_NAME_2/certificates/AAAAAAAAAcg"
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DUCHY_NAME_2 = "duchies/BBBBBBBBBHs"
private const val DUCHY_CERTIFICATE_NAME = "$DUCHY_NAME/certificates/AAAAAAAAAcg"

@RunWith(JUnit4::class)
class CertificatesServiceTest {
  private val internalCertificatesMock: InternalCertificatesCoroutineService = mockService {
    onBlocking { getCertificate(any()) }
      .thenAnswer {
        val request = it.getArgument<InternalGetCertificateRequest>(0)
        INTERNAL_CERTIFICATE.copy {
          externalCertificateId = request.externalCertificateId

          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
          when (request.parentCase) {
            InternalGetCertificateRequest.ParentCase.EXTERNAL_DATA_PROVIDER_ID ->
              externalDataProviderId = request.externalDataProviderId
            InternalGetCertificateRequest.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID ->
              externalMeasurementConsumerId = request.externalMeasurementConsumerId
            InternalGetCertificateRequest.ParentCase.EXTERNAL_DUCHY_ID ->
              externalDuchyId = request.externalDuchyId
            InternalGetCertificateRequest.ParentCase.EXTERNAL_MODEL_PROVIDER_ID ->
              externalModelProviderId = request.externalModelProviderId
            InternalGetCertificateRequest.ParentCase.PARENT_NOT_SET -> error("Invalid case")
          }
        }
      }

    onBlocking { createCertificate(any()) }.thenReturn(INTERNAL_CERTIFICATE)
    onBlocking { revokeCertificate(any()) }.thenReturn(INTERNAL_CERTIFICATE)
    onBlocking { releaseCertificateHold(any()) }.thenReturn(INTERNAL_CERTIFICATE)
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalCertificatesMock) }

  private lateinit var service: CertificatesService

  @Before
  fun initService() {
    service = CertificatesService(InternalCertificatesCoroutineStub(grpcTestServerRule.channel))
  }

  private fun assertGetCertificateRequestSucceeds(
    caller: MeasurementPrincipal,
    certificateName: String,
    expectedInternalRequest: InternalGetCertificateRequest,
  ) {
    val request = getCertificateRequest { name = certificateName }
    val result = withPrincipal(caller) { runBlocking { service.getCertificate(request) } }

    verifyProtoArgument(
        internalCertificatesMock,
        InternalCertificatesCoroutineService::getCertificate,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedInternalRequest)

    assertThat(result)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(CERTIFICATE.copy { name = certificateName })
  }

  private fun assertCreateCertificateRequestSucceeds(
    mockedInternalResponse: InternalCertificate,
    caller: MeasurementPrincipal,
    parentName: String,
    certificate: Certificate,
    expectedInternalCertificate: InternalCertificate,
  ) {
    runBlocking {
      whenever(internalCertificatesMock.createCertificate(any())).thenReturn(mockedInternalResponse)
    }

    val request = createCertificateRequest {
      parent = parentName
      this.certificate = certificate
    }
    val result = withPrincipal(caller) { runBlocking { service.createCertificate(request) } }

    verifyProtoArgument(
        internalCertificatesMock,
        InternalCertificatesCoroutineService::createCertificate,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedInternalCertificate)

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(certificate)
  }

  private fun assertRevokeCertificateRequestSucceeds(
    mockedInternalResponse: InternalCertificate,
    caller: MeasurementPrincipal,
    certificateName: String,
    expectedInternalRequest: InternalRevokeCertificateRequest,
    expectedCertificate: Certificate,
  ) {
    runBlocking {
      whenever(internalCertificatesMock.revokeCertificate(any())).thenReturn(mockedInternalResponse)
    }

    val request = revokeCertificateRequest {
      name = certificateName
      revocationState = Certificate.RevocationState.REVOKED
    }

    val result = withPrincipal(caller) { runBlocking { service.revokeCertificate(request) } }

    verifyProtoArgument(
        internalCertificatesMock,
        InternalCertificatesCoroutineService::revokeCertificate,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedInternalRequest)

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expectedCertificate)
  }

  private fun assertReleaseCertificateHoldRequestSucceeds(
    mockedInternalResponse: InternalCertificate,
    caller: MeasurementPrincipal,
    certificateName: String,
    expectedInternalRequest: InternalReleaseCertificateHoldRequest,
    expectedCertificate: Certificate,
  ) {
    runBlocking {
      whenever(internalCertificatesMock.releaseCertificateHold(any()))
        .thenReturn(mockedInternalResponse)
    }

    val request = releaseCertificateHoldRequest { name = certificateName }

    val result = withPrincipal(caller) { runBlocking { service.releaseCertificateHold(request) } }

    verifyProtoArgument(
        internalCertificatesMock,
        InternalCertificatesCoroutineService::releaseCertificateHold,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedInternalRequest)

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expectedCertificate)
  }

  @Test
  fun `getCertificate succeeds for EDP caller when getting EDP certificate`() {
    assertGetCertificateRequestSucceeds(
      DataProviderPrincipal(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!),
      DATA_PROVIDER_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key = DataProviderCertificateKey.fromName(DATA_PROVIDER_CERTIFICATE_NAME)!!
        externalDataProviderId = apiIdToExternalId(key.dataProviderId)
        externalCertificateId = apiIdToExternalId(key.certificateId)
      },
    )
  }

  @Test
  fun `getCertificate succeeds for EDP caller when getting MC certificate`() {
    assertGetCertificateRequestSucceeds(
      DataProviderPrincipal(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!),
      MEASUREMENT_CONSUMER_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key =
          MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)!!
        externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
        externalCertificateId = apiIdToExternalId(key.certificateId)
      },
    )
  }

  @Test
  fun `getCertificate succeeds for EDP caller when getting Duchy certificate`() {
    assertGetCertificateRequestSucceeds(
      DataProviderPrincipal(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!),
      DUCHY_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!
        externalDuchyId = key.duchyId
        externalCertificateId = apiIdToExternalId(key.certificateId)
      },
    )
  }

  @Test
  fun `getCertificate succeeds for MC caller when getting MC certificate`() {
    assertGetCertificateRequestSucceeds(
      MeasurementConsumerPrincipal(MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!),
      MEASUREMENT_CONSUMER_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key =
          MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)!!
        externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
        externalCertificateId = apiIdToExternalId(key.certificateId)
      },
    )
  }

  @Test
  fun `getCertificate succeeds for MC caller when getting EDP certificate`() {
    assertGetCertificateRequestSucceeds(
      MeasurementConsumerPrincipal(MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!),
      DATA_PROVIDER_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key = DataProviderCertificateKey.fromName(DATA_PROVIDER_CERTIFICATE_NAME)!!
        externalDataProviderId = apiIdToExternalId(key.dataProviderId)
        externalCertificateId = apiIdToExternalId(key.certificateId)
      },
    )
  }

  @Test
  fun `getCertificate succeeds for MC caller when getting Duchy certificate`() {
    assertGetCertificateRequestSucceeds(
      MeasurementConsumerPrincipal(MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!),
      DUCHY_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!
        externalDuchyId = key.duchyId
        externalCertificateId = apiIdToExternalId(key.certificateId)
      },
    )
  }

  @Test
  fun `getCertificate succeeds for Duchy caller when getting Duchy certificate`() {
    assertGetCertificateRequestSucceeds(
      DuchyPrincipal(DuchyKey.fromName(DUCHY_NAME)!!),
      DUCHY_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!
        externalDuchyId = key.duchyId
        externalCertificateId = apiIdToExternalId(key.certificateId)
      },
    )
  }

  @Test
  fun `getCertificate succeeds for MP caller when getting MP certificate`() {
    assertGetCertificateRequestSucceeds(
      ModelProviderPrincipal(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!),
      MODEL_PROVIDER_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key = ModelProviderCertificateKey.fromName(MODEL_PROVIDER_CERTIFICATE_NAME)!!
        externalModelProviderId = apiIdToExternalId(key.modelProviderId)
        externalCertificateId = apiIdToExternalId(key.certificateId)
      },
    )
  }

  @Test
  fun `getCertificate succeeds for MP caller when getting Duchy certificate`() {
    assertGetCertificateRequestSucceeds(
      ModelProviderPrincipal(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!),
      DUCHY_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!
        externalDuchyId = key.duchyId
        externalCertificateId = apiIdToExternalId(key.certificateId)
      },
    )
  }

  @Test
  fun `getCertificate throws UNAUTHENTICATED when no principal is found`() {
    val request = getCertificateRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.getCertificate(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getCertificate throws PERMISSION_DENIED when MC caller doesn't match parent MC`() {
    val request = getCertificateRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME_2 }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getCertificate throws PERMISSION_DENIED when EDP caller doesn't match parent EDP`() {
    val request = getCertificateRequest { name = DATA_PROVIDER_CERTIFICATE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_2_NAME) {
          runBlocking { service.getCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getCertificate throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.getCertificate(getCertificateRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listCertificates returns response with Certificates`() {
    val externalCertificate2Id = ExternalId(INTERNAL_CERTIFICATE.externalCertificateId + 1)
    val internalCertificates =
      listOf(
        INTERNAL_CERTIFICATE,
        INTERNAL_CERTIFICATE.copy { externalCertificateId = externalCertificate2Id.value },
      )
    internalCertificatesMock.stub {
      onBlocking { streamCertificates(any()) }.thenReturn(internalCertificates.asFlow())
    }
    val request = listCertificatesRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter { subjectKeyIdentifiers += INTERNAL_CERTIFICATE.subjectKeyIdentifier }
    }

    val response =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listCertificates(request) }
      }

    verifyProtoArgument(
        internalCertificatesMock,
        InternalCertificatesCoroutineService::streamCertificates,
      )
      .isEqualTo(
        streamCertificatesRequest {
          filter =
            StreamCertificatesRequestKt.filter {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value
              subjectKeyIdentifiers += request.filter.subjectKeyIdentifiersList
            }
          limit = 51
        }
      )
    assertThat(response)
      .isEqualTo(
        listCertificatesResponse {
          certificates += CERTIFICATE
          certificates +=
            CERTIFICATE.copy {
              name =
                DataProviderCertificateKey(
                    DATA_PROVIDER_KEY.dataProviderId,
                    externalCertificate2Id.apiId.value,
                  )
                  .toName()
            }
        }
      )
  }

  @Test
  fun `listCertificates returns response with next page token`() {
    val externalCertificate2Id = ExternalId(INTERNAL_CERTIFICATE.externalCertificateId + 1)
    val internalCertificates =
      listOf(
        INTERNAL_CERTIFICATE,
        INTERNAL_CERTIFICATE.copy { externalCertificateId = externalCertificate2Id.value },
      )
    internalCertificatesMock.stub {
      onBlocking { streamCertificates(any()) }.thenReturn(internalCertificates.asFlow())
    }
    val request = listCertificatesRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 1
    }

    val response =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listCertificates(request) }
      }

    verifyProtoArgument(
        internalCertificatesMock,
        InternalCertificatesCoroutineService::streamCertificates,
      )
      .isEqualTo(
        streamCertificatesRequest {
          filter =
            StreamCertificatesRequestKt.filter {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value
            }
          limit = 2
        }
      )
    assertThat(response)
      .ignoringFields(ListCertificatesResponse.NEXT_PAGE_TOKEN_FIELD_NUMBER)
      .isEqualTo(listCertificatesResponse { certificates += CERTIFICATE })
    val nextPageToken =
      ListCertificatesPageToken.parseFrom(response.nextPageToken.base64UrlDecode())
    assertThat(nextPageToken)
      .isEqualTo(
        listCertificatesPageToken {
          parentKey =
            ListCertificatesPageTokenKt.parentKey {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value
            }
          lastCertificate =
            ListCertificatesPageTokenKt.previousPageEnd {
              parentKey =
                ListCertificatesPageTokenKt.parentKey {
                  externalDataProviderId = INTERNAL_CERTIFICATE.externalDataProviderId
                }
              notValidBefore = INTERNAL_CERTIFICATE.notValidBefore
              externalCertificateId = INTERNAL_CERTIFICATE.externalCertificateId
            }
        }
      )
  }

  @Test
  fun `listCertificates calls internal method with page token`() {
    val externalCertificate2Id = ExternalId(INTERNAL_CERTIFICATE.externalCertificateId + 1)
    val internalCertificates =
      listOf(INTERNAL_CERTIFICATE.copy { externalCertificateId = externalCertificate2Id.value })
    internalCertificatesMock.stub {
      onBlocking { streamCertificates(any()) }.thenReturn(internalCertificates.asFlow())
    }
    val pageToken = listCertificatesPageToken {
      parentKey =
        ListCertificatesPageTokenKt.parentKey {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value
        }
      lastCertificate =
        ListCertificatesPageTokenKt.previousPageEnd {
          parentKey =
            ListCertificatesPageTokenKt.parentKey {
              externalDataProviderId = INTERNAL_CERTIFICATE.externalDataProviderId
            }
          notValidBefore = INTERNAL_CERTIFICATE.notValidBefore
          externalCertificateId = INTERNAL_CERTIFICATE.externalCertificateId
        }
    }
    val request = listCertificatesRequest {
      parent = DATA_PROVIDER_NAME
      this.pageToken = pageToken.toByteString().base64UrlEncode()
    }

    val response =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listCertificates(request) }
      }

    verifyProtoArgument(
        internalCertificatesMock,
        InternalCertificatesCoroutineService::streamCertificates,
      )
      .isEqualTo(
        streamCertificatesRequest {
          filter =
            StreamCertificatesRequestKt.filter {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value
              after =
                StreamCertificatesRequestKt.orderedKey {
                  notValidBefore = pageToken.lastCertificate.notValidBefore
                  externalCertificateId = pageToken.lastCertificate.externalCertificateId
                  externalDataProviderId =
                    pageToken.lastCertificate.parentKey.externalDataProviderId
                }
            }
          limit = 51
        }
      )
    assertThat(response)
      .isEqualTo(
        listCertificatesResponse {
          certificates +=
            CERTIFICATE.copy {
              name =
                DataProviderCertificateKey(
                    DATA_PROVIDER_KEY.dataProviderId,
                    externalCertificate2Id.apiId.value,
                  )
                  .toName()
            }
        }
      )
  }

  @Test
  fun `createCertificate returns certificate when EDP caller is found`() {
    assertCreateCertificateRequestSucceeds(
      INTERNAL_CERTIFICATE,
      DataProviderPrincipal(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!),
      DATA_PROVIDER_NAME,
      CERTIFICATE,
      INTERNAL_CERTIFICATE.copy { clearExternalCertificateId() },
    )
  }

  @Test
  fun `createCertificate returns certificate when MC caller is found`() {
    val key = MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)!!
    val externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
    val externalCertificateId = apiIdToExternalId(key.certificateId)

    assertCreateCertificateRequestSucceeds(
      INTERNAL_CERTIFICATE.copy {
        clearExternalDataProviderId()
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        this.externalCertificateId = externalCertificateId
      },
      MeasurementConsumerPrincipal(MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!),
      MEASUREMENT_CONSUMER_NAME,
      CERTIFICATE.copy { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME },
      INTERNAL_CERTIFICATE.copy {
        clearExternalDataProviderId()
        clearExternalCertificateId()
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
      },
    )
  }

  @Test
  fun `createCertificate returns certificate when duchy caller is found`() {
    val key = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!
    val externalCertificateId = apiIdToExternalId(key.certificateId)

    assertCreateCertificateRequestSucceeds(
      INTERNAL_CERTIFICATE.copy {
        clearExternalDataProviderId()
        externalDuchyId = key.duchyId
        this.externalCertificateId = externalCertificateId
      },
      DuchyPrincipal(DuchyKey.fromName(DUCHY_NAME)!!),
      DUCHY_NAME,
      CERTIFICATE.copy { name = DUCHY_CERTIFICATE_NAME },
      INTERNAL_CERTIFICATE.copy {
        clearExternalDataProviderId()
        clearExternalCertificateId()
        externalDuchyId = key.duchyId
      },
    )
  }

  @Test
  fun `createCertificate returns certificate when MP caller is found`() {
    val key = ModelProviderCertificateKey.fromName(MODEL_PROVIDER_CERTIFICATE_NAME)!!
    val externalModelProviderId = apiIdToExternalId(key.modelProviderId)
    val externalCertificateId = apiIdToExternalId(key.certificateId)

    assertCreateCertificateRequestSucceeds(
      INTERNAL_CERTIFICATE.copy {
        clearExternalDataProviderId()
        this.externalModelProviderId = externalModelProviderId
        this.externalCertificateId = externalCertificateId
      },
      ModelProviderPrincipal(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!),
      MODEL_PROVIDER_NAME,
      CERTIFICATE.copy { name = MODEL_PROVIDER_CERTIFICATE_NAME },
      INTERNAL_CERTIFICATE.copy {
        clearExternalDataProviderId()
        clearExternalCertificateId()
        this.externalModelProviderId = externalModelProviderId
      },
    )
  }

  @Test
  fun `createCertificate throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createCertificate(createCertificateRequest { certificate = CERTIFICATE })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createCertificate throws INVALID_ARGUMENT when certificate is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createCertificate(createCertificateRequest { parent = DATA_PROVIDER_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createCertificate throws UNAUTHENTICATED when no principal is found`() {
    val request = createCertificateRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      certificate = CERTIFICATE.copy { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createCertificate(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createCertificate throws PERMISSION_DENIED when no authorization for MP certificate`() {
    val request = createCertificateRequest {
      parent = MODEL_PROVIDER_NAME
      certificate = CERTIFICATE.copy { name = MODEL_PROVIDER_CERTIFICATE_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createCertificate throws PERMISSION_DENIED when no authorization for duchy certificate`() {
    val request = createCertificateRequest {
      parent = DUCHY_NAME
      certificate = CERTIFICATE.copy { name = DUCHY_CERTIFICATE_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createCertificate throws PERMISSION_DENIED when no authorization for EDP certificate`() {
    val request = createCertificateRequest {
      parent = DATA_PROVIDER_NAME
      certificate = CERTIFICATE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createCertificate throws PERMISSION_DENIED when no authorization for MC certificate`() {
    val request = createCertificateRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      certificate = CERTIFICATE.copy { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createCertificate throws PERMISSION_DENIED when EDP caller doesn't match parent`() {
    val request = createCertificateRequest {
      parent = DATA_PROVIDER_NAME
      certificate = CERTIFICATE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_2_NAME) {
          runBlocking { service.createCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createCertificate throws PERMISSION_DENIED when MC caller doesn't match parent`() {
    val request = createCertificateRequest {
      parent = MEASUREMENT_CONSUMER_NAME_2
      certificate = CERTIFICATE.copy { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createCertificate throws PERMISSION_DENIED when Duchy caller doesn't match parent`() {
    val request = createCertificateRequest {
      parent = DUCHY_NAME_2
      certificate = CERTIFICATE.copy { name = DUCHY_CERTIFICATE_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.createCertificate(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createCertificate throws PERMISSION_DENIED when MP caller doesn't match parent`() {
    val request = createCertificateRequest {
      parent = MODEL_PROVIDER_NAME
      certificate = CERTIFICATE.copy { name = MODEL_PROVIDER_CERTIFICATE_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `revokeCertificate returns certificate with RevocationState set when EDP caller`() {
    assertRevokeCertificateRequestSucceeds(
      INTERNAL_CERTIFICATE.copy { revocationState = InternalCertificate.RevocationState.REVOKED },
      DataProviderPrincipal(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!),
      DATA_PROVIDER_CERTIFICATE_NAME,
      internalRevokeCertificateRequest {
        val key = DataProviderCertificateKey.fromName(CERTIFICATE.name)!!
        externalDataProviderId = apiIdToExternalId(key.dataProviderId)
        externalCertificateId = apiIdToExternalId(key.certificateId)
        revocationState = InternalCertificate.RevocationState.REVOKED
      },
      CERTIFICATE.copy { revocationState = Certificate.RevocationState.REVOKED },
    )
  }

  @Test
  fun `revokeCertificate returns certificate with RevocationState set when MC caller`() {
    val key = MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)!!
    val externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
    val externalCertificateId = apiIdToExternalId(key.certificateId)

    assertRevokeCertificateRequestSucceeds(
      INTERNAL_CERTIFICATE.copy {
        revocationState = InternalCertificate.RevocationState.REVOKED
        clearExternalDataProviderId()
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        this.externalCertificateId = externalCertificateId
      },
      MeasurementConsumerPrincipal(MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!),
      MEASUREMENT_CONSUMER_CERTIFICATE_NAME,
      internalRevokeCertificateRequest {
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        this.externalCertificateId = externalCertificateId
        revocationState = InternalCertificate.RevocationState.REVOKED
      },
      CERTIFICATE.copy {
        name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
        revocationState = Certificate.RevocationState.REVOKED
      },
    )
  }

  @Test
  fun `revokeCertificate returns certificate with RevocationState set when duchy caller`() {
    val key = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!
    val externalCertificateId = apiIdToExternalId(key.certificateId)

    assertRevokeCertificateRequestSucceeds(
      INTERNAL_CERTIFICATE.copy {
        revocationState = InternalCertificate.RevocationState.REVOKED
        clearExternalDataProviderId()
        externalDuchyId = key.duchyId
        this.externalCertificateId = externalCertificateId
      },
      DuchyPrincipal(DuchyKey.fromName(DUCHY_NAME)!!),
      DUCHY_CERTIFICATE_NAME,
      internalRevokeCertificateRequest {
        externalDuchyId = key.duchyId
        this.externalCertificateId = externalCertificateId
        revocationState = InternalCertificate.RevocationState.REVOKED
      },
      CERTIFICATE.copy {
        name = DUCHY_CERTIFICATE_NAME
        revocationState = Certificate.RevocationState.REVOKED
      },
    )
  }

  @Test
  fun `revokeCertificate returns certificate with RevocationState set when MP caller`() {
    val key = ModelProviderCertificateKey.fromName(MODEL_PROVIDER_CERTIFICATE_NAME)!!
    val externalModelProviderId = apiIdToExternalId(key.modelProviderId)
    val externalCertificateId = apiIdToExternalId(key.certificateId)

    assertRevokeCertificateRequestSucceeds(
      INTERNAL_CERTIFICATE.copy {
        revocationState = InternalCertificate.RevocationState.REVOKED
        clearExternalDataProviderId()
        this.externalModelProviderId = externalModelProviderId
        this.externalCertificateId = externalCertificateId
      },
      ModelProviderPrincipal(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!),
      MODEL_PROVIDER_CERTIFICATE_NAME,
      internalRevokeCertificateRequest {
        this.externalModelProviderId = externalModelProviderId
        this.externalCertificateId = externalCertificateId
        revocationState = InternalCertificate.RevocationState.REVOKED
      },
      CERTIFICATE.copy {
        name = MODEL_PROVIDER_CERTIFICATE_NAME
        revocationState = Certificate.RevocationState.REVOKED
      },
    )
  }

  @Test
  fun `revokeCertificate throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        assertFailsWith<StatusRuntimeException> {
          runBlocking { service.revokeCertificate(revokeCertificateRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `revokeCertificate throws INVALID_ARGUMENT when revocation state is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.revokeCertificate(
              revokeCertificateRequest { name = DATA_PROVIDER_CERTIFICATE_NAME }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `revokeCertificate throws UNAUTHENTICATED when no principal is found`() {
    val request = revokeCertificateRequest {
      name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.revokeCertificate(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `revokeCertificate throws PERMISSION_DENIED when no authorization for MP certificate`() {
    val request = revokeCertificateRequest {
      name = MODEL_PROVIDER_CERTIFICATE_NAME
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.revokeCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `revokeCertificate throws PERMISSION_DENIED when no authorization for duchy certificate`() {
    val request = revokeCertificateRequest {
      name = DUCHY_CERTIFICATE_NAME
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.revokeCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `revokeCertificate throws PERMISSION_DENIED when no authorization for EDP certificate`() {
    val request = revokeCertificateRequest {
      name = DATA_PROVIDER_CERTIFICATE_NAME
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.revokeCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `revokeCertificate throws PERMISSION_DENIED when no authorization for MC certificate`() {
    val request = revokeCertificateRequest {
      name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.revokeCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `revokeCertificate throws PERMISSION_DENIED when duchy caller doesn't match parent`() {
    val request = revokeCertificateRequest {
      name = DUCHY_CERTIFICATE_NAME
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME_2) { runBlocking { service.revokeCertificate(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `revokeCertificate throws PERMISSION_DENIED when MC caller doesn't match parent`() {
    val request = revokeCertificateRequest {
      name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME_2
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.revokeCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `revokeCertificate throws PERMISSION_DENIED when EDP caller doesn't match parent`() {
    val request = revokeCertificateRequest {
      name = DATA_PROVIDER_CERTIFICATE_NAME
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_2_NAME) {
          runBlocking { service.revokeCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `revokeCertificate throws PERMISSION_DENIED when MP caller doesn't match parent`() {
    val request = revokeCertificateRequest {
      name = MODEL_PROVIDER_CERTIFICATE_NAME
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.revokeCertificate(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `releaseCertificateHold returns EDP certificate when EDP caller`() {
    assertReleaseCertificateHoldRequestSucceeds(
      INTERNAL_CERTIFICATE,
      DataProviderPrincipal(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!),
      DATA_PROVIDER_CERTIFICATE_NAME,
      internalReleaseCertificateHoldRequest {
        val key = DataProviderCertificateKey.fromName(CERTIFICATE.name)!!
        externalDataProviderId = apiIdToExternalId(key.dataProviderId)
        externalCertificateId = apiIdToExternalId(key.certificateId)
      },
      CERTIFICATE,
    )
  }

  @Test
  fun `releaseCertificateHold returns MC certificate when MC caller`() {
    val key = MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)!!
    val externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
    val externalCertificateId = apiIdToExternalId(key.certificateId)

    assertReleaseCertificateHoldRequestSucceeds(
      INTERNAL_CERTIFICATE.copy {
        clearExternalDataProviderId()
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        this.externalCertificateId = externalCertificateId
      },
      MeasurementConsumerPrincipal(MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!),
      MEASUREMENT_CONSUMER_CERTIFICATE_NAME,
      internalReleaseCertificateHoldRequest {
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        this.externalCertificateId = externalCertificateId
      },
      CERTIFICATE.copy { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME },
    )
  }

  @Test
  fun `releaseCertificateHold returns MP certificate when MP caller`() {
    val key = ModelProviderCertificateKey.fromName(MODEL_PROVIDER_CERTIFICATE_NAME)!!
    val externalModelProviderId = apiIdToExternalId(key.modelProviderId)
    val externalCertificateId = apiIdToExternalId(key.certificateId)

    assertReleaseCertificateHoldRequestSucceeds(
      INTERNAL_CERTIFICATE.copy {
        clearExternalDataProviderId()
        this.externalModelProviderId = externalModelProviderId
        this.externalCertificateId = externalCertificateId
      },
      ModelProviderPrincipal(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!),
      MODEL_PROVIDER_CERTIFICATE_NAME,
      internalReleaseCertificateHoldRequest {
        this.externalModelProviderId = externalModelProviderId
        this.externalCertificateId = externalCertificateId
      },
      CERTIFICATE.copy { name = MODEL_PROVIDER_CERTIFICATE_NAME },
    )
  }

  @Test
  fun `releaseCertificateHold returns duchy certificate when duchy caller`() {
    val key = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!
    val externalCertificateId = apiIdToExternalId(key.certificateId)

    assertReleaseCertificateHoldRequestSucceeds(
      INTERNAL_CERTIFICATE.copy {
        clearExternalDataProviderId()
        externalDuchyId = key.duchyId
        this.externalCertificateId = externalCertificateId
      },
      DuchyPrincipal(DuchyKey.fromName(DUCHY_NAME)!!),
      DUCHY_CERTIFICATE_NAME,
      internalReleaseCertificateHoldRequest {
        externalDuchyId = key.duchyId
        this.externalCertificateId = externalCertificateId
      },
      CERTIFICATE.copy { name = DUCHY_CERTIFICATE_NAME },
    )
  }

  @Test
  fun `releaseCertificateHold throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.releaseCertificateHold(releaseCertificateHoldRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `releaseCertificate throws UNAUTHENTICATED when no principal found`() {
    val request = releaseCertificateHoldRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.releaseCertificateHold(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `releaseCertificate throws PERMISSION_DENIED when EDP caller doesn't match parent`() {
    val request = releaseCertificateHoldRequest { name = DATA_PROVIDER_CERTIFICATE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_2_NAME) {
          runBlocking { service.releaseCertificateHold(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `releaseCertificate throws PERMISSION_DENIED when MC caller doesn't match parent`() {
    val request = releaseCertificateHoldRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME_2 }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.releaseCertificateHold(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `releaseCertificate throws PERMISSION_DENIED when MP caller doesn't match parent`() {
    val request = releaseCertificateHoldRequest { name = MODEL_PROVIDER_CERTIFICATE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.releaseCertificateHold(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `releaseCertificate throws PERMISSION_DENIED when duchy caller doesn't match parent`() {
    val request = releaseCertificateHoldRequest { name = DUCHY_CERTIFICATE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME_2) { runBlocking { service.releaseCertificateHold(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `releaseCertificate throws PERMISSION_DENIED when no authorization to release MP cert`() {
    val request = releaseCertificateHoldRequest { name = MODEL_PROVIDER_CERTIFICATE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.releaseCertificateHold(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `releaseCertificate throws PERMISSION_DENIED when no authorization to release duchy cert`() {
    val request = releaseCertificateHoldRequest { name = DUCHY_CERTIFICATE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.releaseCertificateHold(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `releaseCertificate throws PERMISSION_DENIED when no authorization to release MC cert`() {
    val request = releaseCertificateHoldRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.releaseCertificateHold(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `releaseCertificate throws PERMISSION_DENIED when no authorization to release EDP cert`() {
    val request = releaseCertificateHoldRequest { name = DATA_PROVIDER_CERTIFICATE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.releaseCertificateHold(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getCertificate throws INVALID_ARGUMENT when parent not specified`() {
    internalCertificatesMock.stub {
      onBlocking { getCertificate(any()) }
        .thenThrow(
          Status.INVALID_ARGUMENT.withDescription("Parent not specified").asRuntimeException()
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(
          MeasurementConsumerPrincipal(MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!)
        ) {
          runBlocking {
            service.getCertificate(
              getCertificateRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createCertificate throws NOT_FOUND with model provider name when model provider not found`() {
    internalCertificatesMock.stub {
      onBlocking { createCertificate(any()) }
        .thenThrow(
          ModelProviderNotFoundException(ExternalId(EXTERNAL_MODEL_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "Model provider not found")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(ModelProviderPrincipal(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!)) {
          runBlocking {
            service.createCertificate(
              createCertificateRequest {
                parent = MODEL_PROVIDER_NAME
                certificate = CERTIFICATE
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelProvider", MODEL_PROVIDER_NAME)
  }

  @Test
  fun `createCertificate throws ALREADY_EXISTS when certificate with subject key identifier already exists`() {
    internalCertificatesMock.stub {
      onBlocking { createCertificate(any()) }
        .thenThrow(
          CertSubjectKeyIdAlreadyExistsException()
            .asStatusRuntimeException(
              Status.Code.ALREADY_EXISTS,
              "Certificate with the subject key identifier (SKID) already exists.",
            )
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(ModelProviderPrincipal(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!)) {
          runBlocking {
            service.createCertificate(
              createCertificateRequest {
                parent = MODEL_PROVIDER_NAME
                certificate = CERTIFICATE
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `revokeCertificate throws NOT_FOUND when certificate is not found`() {
    internalCertificatesMock.stub {
      onBlocking { revokeCertificate(any()) }
        .thenThrow(
          CertificateNotFoundException(EXTERNAL_CERTIFICATE_ID)
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "Certificate not found")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(ModelProviderPrincipal(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!)) {
          runBlocking {
            service.revokeCertificate(
              revokeCertificateRequest {
                name = MODEL_PROVIDER_CERTIFICATE_NAME
                revocationState = Certificate.RevocationState.REVOKED
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `revokeCertificate throws FAILED_PRECONDITION with certificate revocation state when certificate state illegal`() {
    internalCertificatesMock.stub {
      onBlocking { revokeCertificate(any()) }
        .thenThrow(
          CertificateRevocationStateIllegalException(
              EXTERNAL_CERTIFICATE_ID,
              InternalCertificate.RevocationState.REVOCATION_STATE_UNSPECIFIED,
            )
            .asStatusRuntimeException(
              Status.Code.FAILED_PRECONDITION,
              "Certificate in illegal state",
            )
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(ModelProviderPrincipal(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!)) {
          runBlocking {
            service.revokeCertificate(
              revokeCertificateRequest {
                name = MODEL_PROVIDER_CERTIFICATE_NAME
                revocationState = Certificate.RevocationState.REVOKED
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry(
        "certificationRevocationState",
        Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED.toString(),
      )
  }

  @Test
  fun `releaseCertificateHold throws NOT_FOUND with duchy api id when duchy not found`() {
    internalCertificatesMock.stub {
      onBlocking { releaseCertificateHold(any()) }
        .thenThrow(
          DuchyNotFoundException(DuchyKey.fromName(DUCHY_NAME)!!.duchyId)
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "Duchy not found")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(DuchyPrincipal(DuchyKey.fromName(DUCHY_NAME)!!)) {
          runBlocking {
            service.releaseCertificateHold(
              releaseCertificateHoldRequest { name = DUCHY_CERTIFICATE_NAME }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("duchy", DUCHY_NAME)
  }

  @Test
  fun `releaseCertificateHold throws FAILED_PRECONDITION with certificate revocation state when certificate state illegal`() {
    internalCertificatesMock.stub {
      onBlocking { releaseCertificateHold(any()) }
        .thenThrow(
          CertificateRevocationStateIllegalException(
              EXTERNAL_CERTIFICATE_ID,
              InternalCertificate.RevocationState.REVOCATION_STATE_UNSPECIFIED,
            )
            .asStatusRuntimeException(
              Status.Code.FAILED_PRECONDITION,
              "Certificate in illegal state",
            )
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(DuchyPrincipal(DuchyKey.fromName(DUCHY_NAME)!!)) {
          runBlocking {
            service.releaseCertificateHold(
              releaseCertificateHoldRequest { name = DUCHY_CERTIFICATE_NAME }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry(
        "certificationRevocationState",
        Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED.toString(),
      )
  }

  companion object {
    private val EXTERNAL_DATA_PROVIDER_ID = ExternalId(12345L)
    private val DATA_PROVIDER_KEY = DataProviderKey(EXTERNAL_DATA_PROVIDER_ID.apiId.value)
    private val DATA_PROVIDER_NAME = DATA_PROVIDER_KEY.toName()

    private val EXTERNAL_DATA_PROVIDER_2_ID = ExternalId(12346L)
    private val DATA_PROVIDER_2_KEY = DataProviderKey(EXTERNAL_DATA_PROVIDER_2_ID.apiId.value)
    private val DATA_PROVIDER_2_NAME = DATA_PROVIDER_2_KEY.toName()

    private val EXTERNAL_CERTIFICATE_ID = ExternalId(456L)
    private val DATA_PROVIDER_CERTIFICATE_KEY =
      DataProviderCertificateKey(
        DATA_PROVIDER_KEY.dataProviderId,
        EXTERNAL_CERTIFICATE_ID.apiId.value,
      )
    private val DATA_PROVIDER_CERTIFICATE_NAME = DATA_PROVIDER_CERTIFICATE_KEY.toName()

    private val SERVER_CERTIFICATE: X509Certificate =
      readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE)
    private val SERVER_CERTIFICATE_DER = ByteString.copyFrom(SERVER_CERTIFICATE.encoded)

    private val INTERNAL_CERTIFICATE = internalCertificate {
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value
      externalCertificateId = EXTERNAL_CERTIFICATE_ID.value
      subjectKeyIdentifier = SERVER_CERTIFICATE.subjectKeyIdentifier!!
      notValidBefore = SERVER_CERTIFICATE.notBefore.toInstant().toProtoTime()
      notValidAfter = SERVER_CERTIFICATE.notAfter.toInstant().toProtoTime()
      details = certificateDetails { x509Der = SERVER_CERTIFICATE_DER }
    }
    private val CERTIFICATE: Certificate = certificate {
      name = DATA_PROVIDER_CERTIFICATE_NAME
      x509Der = SERVER_CERTIFICATE_DER
      subjectKeyIdentifier = INTERNAL_CERTIFICATE.subjectKeyIdentifier
    }
  }
}
