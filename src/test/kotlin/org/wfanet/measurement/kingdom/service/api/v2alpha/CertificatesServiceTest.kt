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
import org.wfanet.measurement.api.v2alpha.CreateCertificateRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.GetCertificateRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.ReleaseCertificateHoldRequest
import org.wfanet.measurement.api.v2alpha.RevokeCertificateRequest
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate as InternalCertificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub

private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val DATA_PROVIDER_CERTIFICATE_NAME = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAcg"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
  "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DUCHY_CERTIFICATE_NAME = "$DUCHY_NAME/certificates/AAAAAAAAAcg"

@RunWith(JUnit4::class)
class CertificatesServiceTest {
  private val internalCertificatesMock: CertificatesCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { getCertificate(any()) }.thenReturn(INTERNAL_CERTIFICATE)
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

  @Test
  fun `getCertificate with data provider certificate name returns certificate`() {

    val request = buildGetCertificateRequest { name = DATA_PROVIDER_CERTIFICATE_NAME }

    val result = runBlocking { service.getCertificate(request) }

    val expected = CERTIFICATE

    verifyProtoArgument(internalCertificatesMock, CertificatesCoroutineImplBase::getCertificate)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        buildInternalGetCertificateRequest {
          val key = DataProviderCertificateKey.fromName(CERTIFICATE.name)
          externalDataProviderId = apiIdToExternalId(key!!.dataProviderId)
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `getCertificate with measurement consumer certificate name returns certificate`() {
    runBlocking {
      whenever(internalCertificatesMock.getCertificate(any()))
        .thenReturn(
          INTERNAL_CERTIFICATE.change {
            val key =
              MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)
            externalMeasurementConsumerId = apiIdToExternalId(key!!.measurementConsumerId)
            externalCertificateId = apiIdToExternalId(key.certificateId)
          }
        )
    }

    val request = buildGetCertificateRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }

    val result = runBlocking { service.getCertificate(request) }

    val expected = CERTIFICATE.rebuild { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }

    verifyProtoArgument(internalCertificatesMock, CertificatesCoroutineImplBase::getCertificate)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        buildInternalGetCertificateRequest {
          val key =
            MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)
          externalMeasurementConsumerId = apiIdToExternalId(key!!.measurementConsumerId)
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `getCertificate with duchy certificate name returns certificate`() {
    runBlocking {
      whenever(internalCertificatesMock.getCertificate(any()))
        .thenReturn(
          INTERNAL_CERTIFICATE.change {
            clearExternalDataProviderId()
            val key = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)
            externalDuchyId = key!!.duchyId
            externalCertificateId = apiIdToExternalId(key.certificateId)
          }
        )
    }

    val request = buildGetCertificateRequest { name = DUCHY_CERTIFICATE_NAME }

    val result = runBlocking { service.getCertificate(request) }

    val expected = CERTIFICATE.rebuild { name = DUCHY_CERTIFICATE_NAME }

    verifyProtoArgument(internalCertificatesMock, CertificatesCoroutineImplBase::getCertificate)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        buildInternalGetCertificateRequest {
          val key = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)
          externalDuchyId = key!!.duchyId
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
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

    val request = buildCreateCertificateRequest {
      parent = DATA_PROVIDER_NAME
      certificate = CERTIFICATE
    }

    val result = runBlocking { service.createCertificate(request) }

    val expected = CERTIFICATE

    verifyProtoArgument(internalCertificatesMock, CertificatesCoroutineImplBase::createCertificate)
      .comparingExpectedFieldsOnly()
      .isEqualTo(INTERNAL_CERTIFICATE.change { clearExternalCertificateId() })

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `createCertificate throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createCertificate(CreateCertificateRequest.getDefaultInstance()) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Parent unspecified or invalid")
  }

  @Test
  fun `createCertificate throws INVALID_ARGUMENT when certificate is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.createCertificate(
            buildCreateCertificateRequest {
              parent = org.wfanet.measurement.kingdom.service.api.v2alpha.DATA_PROVIDER_NAME
            }
          )
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
          INTERNAL_CERTIFICATE.change {
            revocationState = InternalCertificate.RevocationState.REVOKED
          }
        )
    }

    val request = buildRevokeCertificateRequest {
      name = DATA_PROVIDER_CERTIFICATE_NAME
      revocationState = Certificate.RevocationState.REVOKED
    }

    val result = runBlocking { service.revokeCertificate(request) }

    val expected = CERTIFICATE.rebuild { revocationState = Certificate.RevocationState.REVOKED }

    verifyProtoArgument(internalCertificatesMock, CertificatesCoroutineImplBase::revokeCertificate)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        buildInternalRevokeCertificateRequest {
          val key = DataProviderCertificateKey.fromName(CERTIFICATE.name)
          externalDataProviderId = apiIdToExternalId(key!!.dataProviderId)
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
            buildRevokeCertificateRequest { name = DATA_PROVIDER_CERTIFICATE_NAME }
          )
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Revocation State unspecified")
  }

  @Test
  fun `releaseCertificateHold returns certificate with hold released`() {

    val request = buildReleaseCertificateHoldRequest { name = DATA_PROVIDER_CERTIFICATE_NAME }

    val result = runBlocking { service.releaseCertificateHold(request) }

    val expected = CERTIFICATE

    verifyProtoArgument(
        internalCertificatesMock,
        CertificatesCoroutineImplBase::releaseCertificateHold
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        buildInternalReleaseCertificateHoldRequest {
          val key = DataProviderCertificateKey.fromName(CERTIFICATE.name)
          externalDataProviderId = apiIdToExternalId(key!!.dataProviderId)
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

private val CERTIFICATE: Certificate = buildCertificate {
  name = DATA_PROVIDER_CERTIFICATE_NAME
  x509Der = SERVER_CERTIFICATE_DER
}

private val INTERNAL_CERTIFICATE =
  InternalCertificate.newBuilder()
    .apply {
      val key = DataProviderCertificateKey.fromName(CERTIFICATE.name)
      externalDataProviderId = apiIdToExternalId(key!!.dataProviderId)
      externalCertificateId = apiIdToExternalId(key.certificateId)
      subjectKeyIdentifier = SERVER_CERTIFICATE.subjectKeyIdentifier
      notValidBefore = SERVER_CERTIFICATE.notBefore.toInstant().toProtoTime()
      notValidAfter = SERVER_CERTIFICATE.notAfter.toInstant().toProtoTime()
      detailsBuilder.x509Der = SERVER_CERTIFICATE_DER
    }
    .build()

internal inline fun InternalCertificate.change(
  fill: (@Builder InternalCertificate.Builder).() -> Unit
) = toBuilder().apply(fill).build()

internal inline fun Certificate.rebuild(fill: (@Builder Certificate.Builder).() -> Unit) =
  toBuilder().apply(fill).build()

internal inline fun buildGetCertificateRequest(
  fill: (@Builder GetCertificateRequest.Builder).() -> Unit
) = GetCertificateRequest.newBuilder().apply(fill).build()

internal inline fun buildCreateCertificateRequest(
  fill: (@Builder CreateCertificateRequest.Builder).() -> Unit
) = CreateCertificateRequest.newBuilder().apply(fill).build()

internal inline fun buildRevokeCertificateRequest(
  fill: (@Builder RevokeCertificateRequest.Builder).() -> Unit
) = RevokeCertificateRequest.newBuilder().apply(fill).build()

internal inline fun buildReleaseCertificateHoldRequest(
  fill: (@Builder ReleaseCertificateHoldRequest.Builder).() -> Unit
) = ReleaseCertificateHoldRequest.newBuilder().apply(fill).build()
