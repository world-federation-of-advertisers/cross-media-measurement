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
import java.time.Clock
import java.time.Instant
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
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt.details
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.getCertificateRequest
import org.wfanet.measurement.internal.kingdom.revokeCertificateRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private const val EXTERNAL_CERTIFICATE_ID = 123L
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 234L
private const val EXTERNAL_DATA_PROVIDER_ID = 345L
private val EXTERNAL_DUCHY_IDS = listOf("duchy_1", "duchy_2", "duchy_3")

private val TEST_INSTANT = Instant.ofEpochMilli(123456789L)
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val MC_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a MC certificate der.")
private val DP_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a DP certificate der.")

private val MC_SUBJECT_KEY_IDENTIFIER =
  ByteString.copyFromUtf8("This is a MC subject key identifier.")
private val DP_SUBJECT_KEY_IDENTIFIER =
  ByteString.copyFromUtf8("This is a DP subject key identifier.")

private val X509_DER = ByteString.copyFromUtf8("This is a X.509 certificate in DER format.")

@RunWith(JUnit4::class)
abstract class CertificatesServiceTest<T : CertificatesCoroutineImplBase> {
  @get:Rule val duchyIdSetter = DuchyIdSetter(EXTERNAL_DUCHY_IDS)

  private val clock: Clock = TestClockWithNamedInstants(TEST_INSTANT)
  private val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected data class Services<T>(
    val certificatesService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase
  )

  protected lateinit var certificatesService: T
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(idGenerator)
    certificatesService = services.certificatesService
    measurementConsumersService = services.measurementConsumersService
    dataProvidersService = services.dataProvidersService
  }

  @Test
  fun `getCertificate throws INVALID_ARGUMENT when parent not specified`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.getCertificate(
          getCertificateRequest { externalCertificateId = EXTERNAL_CERTIFICATE_ID }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createCertificate throws INVALID_ARGUMENT when parent not specified`() = runBlocking {
    val certificate = certificate {
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getCertificate fails for missing DuchyCertificate`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.getCertificate(
          getCertificateRequest {
            externalDuchyId = EXTERNAL_DUCHY_IDS[0]
            externalCertificateId = EXTERNAL_CERTIFICATE_ID
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createCertificate fails due to Duchy owner not_found `() = runBlocking {
    val certificate = certificate {
      externalDuchyId = "non-existing-duchy-id"
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Duchy not found")
  }

  @Test
  fun `createCertificate suceeds for DuchyCertificate`() = runBlocking {
    val certificate = certificate {
      externalDuchyId = EXTERNAL_DUCHY_IDS[0]
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    val createdCertificate = certificatesService.createCertificate(certificate)

    assertThat(createdCertificate)
      .isEqualTo(
        certificate
          .toBuilder()
          .also { it.externalCertificateId = createdCertificate.externalCertificateId }
          .build()
      )
  }

  @Test
  fun `getCertificate succeeds for DuchyCertificate`() = runBlocking {
    val request = certificate {
      externalDuchyId = EXTERNAL_DUCHY_IDS[0]
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    val createdCertificate = certificatesService.createCertificate(request)

    val certificate =
      certificatesService.getCertificate(
        getCertificateRequest {
          externalDuchyId = EXTERNAL_DUCHY_IDS[0]
          externalCertificateId = createdCertificate.externalCertificateId
        }
      )

    assertThat(certificate).isEqualTo(createdCertificate)
  }

  @Test
  fun `getCertificate fails for missing MeasurementConsumerCertificate`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.getCertificate(
          getCertificateRequest {
            externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
            externalCertificateId = EXTERNAL_CERTIFICATE_ID
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createCertificate fails due to MeasurementConsumer owner not_found `() = runBlocking {
    val certificate = certificate {
      externalMeasurementConsumerId = 5678
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `createCertificate fails due to subjectKeyIdentifier collision`() = runBlocking {
    val externalMeasurementConsumerId =
      population.createMeasurementConsumer(measurementConsumersService)
        .externalMeasurementConsumerId

    val certificate = certificate {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    certificatesService.createCertificate(certificate)
    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception)
      .hasMessageThat()
      .contains("Certificate with the same subject key identifier (SKID) already exists.")
  }

  @Test
  fun `createCertificate suceeds for MeasurementConsumerCertificate`() = runBlocking {
    val externalMeasurementConsumerId =
      population.createMeasurementConsumer(measurementConsumersService)
        .externalMeasurementConsumerId

    val certificate = certificate {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    val createdCertificate = certificatesService.createCertificate(certificate)

    assertThat(createdCertificate)
      .isEqualTo(
        certificate
          .toBuilder()
          .also { it.externalCertificateId = createdCertificate.externalCertificateId }
          .build()
      )
  }

  @Test
  fun `getCertificate succeeds for MeasurementConsumerCertificate`() = runBlocking {
    val externalMeasurementConsumerId =
      population.createMeasurementConsumer(measurementConsumersService)
        .externalMeasurementConsumerId

    val request = certificate {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    val createdCertificate = certificatesService.createCertificate(request)

    val certificate =
      certificatesService.getCertificate(
        getCertificateRequest {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          externalCertificateId = createdCertificate.externalCertificateId
        }
      )

    assertThat(certificate).isEqualTo(createdCertificate)
  }

  @Test
  fun `getCertificate fails for missing DataProviderCertificate`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.getCertificate(
          getCertificateRequest {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            externalCertificateId = EXTERNAL_CERTIFICATE_ID
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createCertificate fails due to DataProvider owner not_found `() = runBlocking {
    val certificate = certificate {
      externalDataProviderId = 5678
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `createCertificate suceeds for DataProviderCertificate`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val request = certificate {
      this.externalDataProviderId = externalDataProviderId
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    val certificate = certificatesService.createCertificate(request)

    assertThat(certificate)
      .isEqualTo(
        request
          .toBuilder()
          .also { it.externalCertificateId = certificate.externalCertificateId }
          .build()
      )
  }

  @Test
  fun `getCertificate succeeds for DataProviderCertificate`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val request = certificate {
      this.externalDataProviderId = externalDataProviderId
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details = details { x509Der = X509_DER }
    }

    val createdCertificate = certificatesService.createCertificate(request)

    val certificate =
      certificatesService.getCertificate(
        getCertificateRequest {
          this.externalDataProviderId = externalDataProviderId
          externalCertificateId = createdCertificate.externalCertificateId
        }
      )

    assertThat(certificate).isEqualTo(createdCertificate)
  }

  @Test
  fun `revokeCertificate throws INVALID_ARGUMENT when parent not specified`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.revokeCertificate(
          revokeCertificateRequest { revocationState = Certificate.RevocationState.REVOKED }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `revokeCertificate fails due to wrong DataProviderId`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDataProviderId = externalDataProviderId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = details { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalDataProviderId = 1234L // wrong externalDataProviderId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.revokeCertificate(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Certificate not found")
  }

  @Test
  fun `revokeCertificate succeeds for DataProviderCertificate`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDataProviderId = externalDataProviderId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = details { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalDataProviderId = externalDataProviderId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val revokedCertificate = certificatesService.revokeCertificate(request)

    assertThat(revokedCertificate)
      .isEqualTo(
        certificatesService.getCertificate(
          getCertificateRequest {
            this.externalDataProviderId = externalDataProviderId
            externalCertificateId = certificate.externalCertificateId
          }
        )
      )

    assertThat(revokedCertificate.revocationState).isEqualTo(Certificate.RevocationState.REVOKED)
  }

  @Test
  fun `revokeCertificate fails due to wrong MeasurementConsumerId`() = runBlocking {
    val externalMeasurementConsumerId =
      population.createMeasurementConsumer(measurementConsumersService)
        .externalMeasurementConsumerId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = details { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalMeasurementConsumerId = 1234L // wrong MeasurementConsumerId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.revokeCertificate(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Certificate not found")
  }

  @Test
  fun `revokeCertificate succeeds for MeasurementConsumerCertificate`() = runBlocking {
    val externalMeasurementConsumerId =
      population.createMeasurementConsumer(measurementConsumersService)
        .externalMeasurementConsumerId

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = details { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val revokedCertificate = certificatesService.revokeCertificate(request)

    assertThat(revokedCertificate)
      .isEqualTo(
        certificatesService.getCertificate(
          getCertificateRequest {
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            externalCertificateId = certificate.externalCertificateId
          }
        )
      )

    assertThat(revokedCertificate.revocationState).isEqualTo(Certificate.RevocationState.REVOKED)
  }

  @Test
  fun `revokeCertificate fails due to wrong DuchyId`() = runBlocking {
    val certificate =
      certificatesService.createCertificate(
        certificate {
          externalDuchyId = EXTERNAL_DUCHY_IDS[0]
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = details { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalDuchyId = "non-existing-duchy-id" // wrong MeasurementConsumerId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.revokeCertificate(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Duchy not found")
  }

  @Test
  fun `revokeCertificate succeeds for DuchyCertificate`() = runBlocking {
    val externalDuchyId = EXTERNAL_DUCHY_IDS[0]

    val certificate =
      certificatesService.createCertificate(
        certificate {
          this.externalDuchyId = externalDuchyId
          notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
          notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
          details = details { x509Der = X509_DER }
        }
      )

    val request = revokeCertificateRequest {
      this.externalDuchyId = externalDuchyId
      externalCertificateId = certificate.externalCertificateId
      revocationState = Certificate.RevocationState.REVOKED
    }

    val revokedCertificate = certificatesService.revokeCertificate(request)

    assertThat(revokedCertificate)
      .isEqualTo(
        certificatesService.getCertificate(
          getCertificateRequest {
            this.externalDuchyId = externalDuchyId
            externalCertificateId = certificate.externalCertificateId
          }
        )
      )

    assertThat(revokedCertificate.revocationState).isEqualTo(Certificate.RevocationState.REVOKED)
  }
}
