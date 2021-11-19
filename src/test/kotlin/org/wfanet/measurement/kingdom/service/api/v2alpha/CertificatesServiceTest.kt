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
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.GetCertificateRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.ModelProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.ReleaseCertificateHoldRequest
import org.wfanet.measurement.api.v2alpha.RevokeCertificateRequest
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.releaseCertificateHoldRequest
import org.wfanet.measurement.api.v2alpha.revokeCertificateRequest
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate as InternalCertificate
import org.wfanet.measurement.internal.kingdom.CertificateKt.details
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest as InternalGetCertificateRequest
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.getCertificateRequest as internalGetCertificateRequest
import org.wfanet.measurement.internal.kingdom.releaseCertificateHoldRequest as internalReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.revokeCertificateRequest as internalRevokeCertificateRequest
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.testing.makeModelProvider

private val DATA_PROVIDER_NAME = makeDataProvider(12345L)
private val DATA_PROVIDER_CERTIFICATE_NAME = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAcg"
private val MODEL_PROVIDER_NAME = makeModelProvider(23456L)
private val MODEL_PROVIDER_CERTIFICATE_NAME = "$MODEL_PROVIDER_NAME/certificates/AAAAAAAAAcg"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
  "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DUCHY_CERTIFICATE_NAME = "$DUCHY_NAME/certificates/AAAAAAAAAcg"

@RunWith(JUnit4::class)
class CertificatesServiceTest {
  private val internalCertificatesMock: CertificatesCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { getCertificate(any()) }.thenAnswer {
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
    service = CertificatesService(CertificatesCoroutineStub(grpcTestServerRule.channel))
  }

  private fun assertGetCertificateRequestSucceeds(
    name: String,
    expectedInternalRequest: InternalGetCertificateRequest
  ) {
    val request = getCertificateRequest { this.name = name }
    val result = runBlocking { service.getCertificate(request) }

    verifyProtoArgument(internalCertificatesMock, CertificatesCoroutineImplBase::getCertificate)
      .comparingExpectedFieldsOnly()
      .isEqualTo(expectedInternalRequest)

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(CERTIFICATE.copy { this.name = name })
  }

  @Test
  fun `getCertificate succeeds for DataProvider`() {
    assertGetCertificateRequestSucceeds(
      DATA_PROVIDER_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key = DataProviderCertificateKey.fromName(DATA_PROVIDER_CERTIFICATE_NAME)!!
        externalDataProviderId = apiIdToExternalId(key.dataProviderId)
        externalCertificateId = apiIdToExternalId(key.certificateId)
      }
    )
  }

  @Test
  fun `getCertificate succeeds for MeasurementConsumer`() {
    assertGetCertificateRequestSucceeds(
      MEASUREMENT_CONSUMER_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key =
          MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)!!
        externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
        externalCertificateId = apiIdToExternalId(key.certificateId)
      }
    )
  }

  @Test
  fun `getCertificate succeeds for Duchy`() {
    assertGetCertificateRequestSucceeds(
      DUCHY_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!
        externalDuchyId = key.duchyId
        externalCertificateId = apiIdToExternalId(key.certificateId)
      }
    )
  }

  @Test
  fun `getCertificate succeeds for ModelProvider`() {
    assertGetCertificateRequestSucceeds(
      MODEL_PROVIDER_CERTIFICATE_NAME,
      internalGetCertificateRequest {
        val key = ModelProviderCertificateKey.fromName(MODEL_PROVIDER_CERTIFICATE_NAME)!!
        externalModelProviderId = apiIdToExternalId(key.modelProviderId)
        externalCertificateId = apiIdToExternalId(key.certificateId)
      }
    )
  }

  @Test
  fun `getCertificate throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.getCertificate(GetCertificateRequest.getDefaultInstance()) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }

  @Test
  fun `createCertificate returns certificate`() {

    val request = createCertificateRequest {
      parent = DATA_PROVIDER_NAME
      certificate = CERTIFICATE
    }

    val result = runBlocking { service.createCertificate(request) }

    val expected = CERTIFICATE

    verifyProtoArgument(internalCertificatesMock, CertificatesCoroutineImplBase::createCertificate)
      .isEqualTo(INTERNAL_CERTIFICATE.copy { clearExternalCertificateId() })

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createCertificate throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.createCertificate(createCertificateRequest { certificate = CERTIFICATE })
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Parent unspecified or invalid")
  }

  @Test
  fun `createCertificate throws INVALID_ARGUMENT when certificate is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.createCertificate(createCertificateRequest { parent = DATA_PROVIDER_NAME })
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("certificate_der is not specified")
  }

  @Test
  fun `revokeCertificate returns certificate with RevocationState set`() {
    runBlocking {
      whenever(internalCertificatesMock.revokeCertificate(any()))
        .thenReturn(
          INTERNAL_CERTIFICATE.copy {
            revocationState = InternalCertificate.RevocationState.REVOKED
          }
        )
    }

    val request = revokeCertificateRequest {
      name = DATA_PROVIDER_CERTIFICATE_NAME
      revocationState = Certificate.RevocationState.REVOKED
    }

    val result = runBlocking { service.revokeCertificate(request) }

    val expected = CERTIFICATE.copy { revocationState = Certificate.RevocationState.REVOKED }

    verifyProtoArgument(internalCertificatesMock, CertificatesCoroutineImplBase::revokeCertificate)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        internalRevokeCertificateRequest {
          val key = DataProviderCertificateKey.fromName(CERTIFICATE.name)!!
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)
          externalCertificateId = apiIdToExternalId(key.certificateId)
          revocationState = InternalCertificate.RevocationState.REVOKED
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `revokeCertificate throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.revokeCertificate(RevokeCertificateRequest.getDefaultInstance()) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }

  @Test
  fun `revokeCertificate throws INVALID_ARGUMENT when revocation state is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.revokeCertificate(
            revokeCertificateRequest { name = DATA_PROVIDER_CERTIFICATE_NAME }
          )
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Revocation State unspecified")
  }

  @Test
  fun `releaseCertificateHold returns certificate with hold released`() {

    val request = releaseCertificateHoldRequest { name = DATA_PROVIDER_CERTIFICATE_NAME }

    val result = runBlocking { service.releaseCertificateHold(request) }

    val expected = CERTIFICATE

    verifyProtoArgument(
        internalCertificatesMock,
        CertificatesCoroutineImplBase::releaseCertificateHold
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        internalReleaseCertificateHoldRequest {
          val key = DataProviderCertificateKey.fromName(CERTIFICATE.name)!!
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `releaseCertificateHold throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.releaseCertificateHold(ReleaseCertificateHoldRequest.getDefaultInstance())
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }
}

private val SERVER_CERTIFICATE: X509Certificate = readCertificate(FIXED_SERVER_CERT_PEM_FILE)
private val SERVER_CERTIFICATE_DER = ByteString.copyFrom(SERVER_CERTIFICATE.encoded)

private val CERTIFICATE: Certificate = certificate {
  name = DATA_PROVIDER_CERTIFICATE_NAME
  x509Der = SERVER_CERTIFICATE_DER
}

private val INTERNAL_CERTIFICATE = internalCertificate {
  val key = DataProviderCertificateKey.fromName(CERTIFICATE.name)!!
  externalDataProviderId = apiIdToExternalId(key.dataProviderId)
  externalCertificateId = apiIdToExternalId(key.certificateId)
  subjectKeyIdentifier = SERVER_CERTIFICATE.subjectKeyIdentifier!!
  notValidBefore = SERVER_CERTIFICATE.notBefore.toInstant().toProtoTime()
  notValidAfter = SERVER_CERTIFICATE.notAfter.toInstant().toProtoTime()
  details = details { x509Der = SERVER_CERTIFICATE_DER }
}
