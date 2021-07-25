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
import java.time.Instant
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase

private const val RANDOM_SEED = 1
private const val EXTERNAL_CERTIFICATE_ID = 123L
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 234L
private const val EXTERNAL_DATA_PROVIDER_ID = 345L

private val EXTERNAL_DUCHY_IDS = listOf("duchy_1", "duchy_2", "duchy_3")

private val TEST_INSTANT = Instant.ofEpochMilli(123456789L)
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val PREFERRED_MC_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a MC certificate der.")
private val PREFERRED_DP_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a DP certificate der.")

private val PREFERRED_MC_SUBJECT_KEY_IDENTIFIER =
  ByteString.copyFromUtf8("This is a MC subject key identifier.")
private val PREFERRED_DP_SUBJECT_KEY_IDENTIFIER =
  ByteString.copyFromUtf8("This is a DP subject key identifier.")

private val X509_DER = ByteString.copyFromUtf8("This is a X.509 certificate in DER format.")

@RunWith(JUnit4::class)
abstract class CertificatesServiceTest<T : CertificatesCoroutineImplBase> {

  protected data class Services<T>(
    val certificatesService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase
  )

  protected val idGenerator =
    RandomIdGenerator(TestClockWithNamedInstants(TEST_INSTANT), Random(RANDOM_SEED))

  protected lateinit var certificatesService: T
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  private suspend fun insertMeasurementConsumer(): Long {
    return measurementConsumersService.createMeasurementConsumer(
      MeasurementConsumer.newBuilder()
        .apply {
              preferredCertificateBuilder.apply {
                notValidBeforeBuilder.seconds = 12345
            notValidAfterBuilder.seconds = 23456
            subjectKeyIdentifier = PREFERRED_MC_SUBJECT_KEY_IDENTIFIER
            detailsBuilder.x509Der = PREFERRED_MC_CERTIFICATE_DER
              }
          detailsBuilder.apply {
                apiVersion = "v2alpha"
            publicKey = PUBLIC_KEY
            publicKeySignature = PUBLIC_KEY_SIGNATURE
              }
            }
        .build()
    )
      .externalMeasurementConsumerId
  }

  private suspend fun insertDataProvider(): Long {
    return dataProvidersService.createDataProvider(
      DataProvider.newBuilder()
        .apply {
              preferredCertificateBuilder.apply {
                notValidBeforeBuilder.seconds = 12345
            notValidAfterBuilder.seconds = 23456
            subjectKeyIdentifier = PREFERRED_DP_SUBJECT_KEY_IDENTIFIER
            detailsBuilder.x509Der = PREFERRED_DP_CERTIFICATE_DER
              }
          detailsBuilder.apply {
                apiVersion = "v2alpha"
            publicKey = PUBLIC_KEY
            publicKeySignature = PUBLIC_KEY_SIGNATURE
              }
            }
        .build()
    )
      .externalDataProviderId
  }

  @Before
  fun initServices() {
    val services = newServices(idGenerator)
    certificatesService = services.certificatesService
    measurementConsumersService = services.measurementConsumersService
    dataProvidersService = services.dataProvidersService
  }

  @Test
  fun `getCertificate fails due to missing parent field`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.getCertificate(
          GetCertificateRequest.newBuilder()
            .apply { externalCertificateId = EXTERNAL_CERTIFICATE_ID }
            .build()
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("INVALID_ARGUMENT: GetCertificateRequest is missing parent field")
  }

  @Test
  fun `createCertificate fails due to missing parent field`() = runBlocking {
    val certificate =
      Certificate.newBuilder()
        .also {
          it.notValidBeforeBuilder.seconds = 12345
          it.notValidAfterBuilder.seconds = 23456
          it.detailsBuilder.x509Der = X509_DER
        }
        .build()

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("INVALID_ARGUMENT: Certificate is missing parent field")
  }

  @Test fun `getCertificate fails for missing DuchyCertificate`() = runBlocking {}

  @Test
  fun `createCertificate fails due to Duchy owner not_found `() = runBlocking {
    DuchyIds.setForTest(EXTERNAL_DUCHY_IDS)
    val certificate =
      Certificate.newBuilder()
        .also {
          it.externalDuchyId = "non-existing-duchy-id"
          it.notValidBeforeBuilder.seconds = 12345
          it.notValidAfterBuilder.seconds = 23456
          it.detailsBuilder.x509Der = X509_DER
        }
        .build()

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("INVALID_ARGUMENT: Duchy not found")
  }

    @Test
    fun `createCertificate suceeds for DuchyCertificate`() = runBlocking {
      DuchyIds.setForTest(EXTERNAL_DUCHY_IDS)
      val certificate =
        Certificate.newBuilder()
          .also {
            it.externalDuchyId = EXTERNAL_DUCHY_IDS.get(0)
            it.notValidBeforeBuilder.seconds = 12345
            it.notValidAfterBuilder.seconds = 23456
            it.detailsBuilder.x509Der = X509_DER
          }
          .build()

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
  fun `getCertificate fails for missing MeasurementConsumerCertificate`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.getCertificate(
          GetCertificateRequest.newBuilder()
            .apply {
              externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
              externalCertificateId = EXTERNAL_CERTIFICATE_ID
            }
            .build()
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createCertificate fails due to MeasurementConsumer owner not_found `() = runBlocking {
    val certificate =
      Certificate.newBuilder()
        .also {
          it.externalMeasurementConsumerId = 5678
          it.notValidBeforeBuilder.seconds = 12345
          it.notValidAfterBuilder.seconds = 23456
          it.detailsBuilder.x509Der = X509_DER
        }
        .build()

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("INVALID_ARGUMENT: MeasurementConsumer not found")
  }

<<<<<<< HEAD
  @Test
  fun `createCertificate fails due to subjectKeyIdentifier collision`() = runBlocking {
    val externalMeasurementConsumerId = insertMeasurementConsumer()
    val certificate =
      Certificate.newBuilder()
        .also {
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.notValidBeforeBuilder.seconds = 12345
          it.notValidAfterBuilder.seconds = 23456
          it.detailsBuilder.x509Der = X509_DER
        }
        .build()

    val createdCertificate = certificatesService.createCertificate(certificate)
    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception)
      .hasMessageThat()
      .contains("Certificate with the same subject key identifier (SKID) already exists.")
  }
=======
  //  !!!!Still in discussion, will implement this!!!

  //   @Test
  //   fun `createCertificate fails due to subjectKeyIdentifier collision`() = runBlocking {
  //     val externalMeasurementConsumerId = insertMeasurementConsumer()
  //     val certificate =
  //       Certificate.newBuilder()
  //         .also {
  //           it.externalMeasurementConsumerId = externalMeasurementConsumerId
  //           it.notValidBeforeBuilder.seconds = 12345
  //           it.notValidAfterBuilder.seconds = 23456
  //           it.detailsBuilder.x509Der = X509_DER
  //         }
  //         .build()

  //     val createdCertificate = certificatesService.createCertificate(certificate)
  //     val exception = certificatesService.createCertificate(certificate)

  //     assertThat(exception)
  //       .isEqualTo(
  //         certificate
  //           .toBuilder()
  //           .also { it.externalCertificateId = createdCertificate.externalCertificateId }
  //           .build()
  //       )
  //   }
>>>>>>> af111fbd (create duchy cert ok)

  @Test
  fun `createCertificate suceeds for MeasurementConsumerCertificate`() = runBlocking {
    val externalMeasurementConsumerId = insertMeasurementConsumer()
    val certificate =
      Certificate.newBuilder()
        .also {
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.notValidBeforeBuilder.seconds = 12345
          it.notValidAfterBuilder.seconds = 23456
          it.detailsBuilder.x509Der = X509_DER
        }
        .build()

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
    val externalMeasurementConsumerId = insertMeasurementConsumer()

    val request =
      Certificate.newBuilder()
        .also {
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.notValidBeforeBuilder.seconds = 12345
          it.notValidAfterBuilder.seconds = 23456
          it.detailsBuilder.x509Der = X509_DER
        }
        .build()

    val createdCertificate = certificatesService.createCertificate(request)

    val certificate =
      certificatesService.getCertificate(
        GetCertificateRequest.newBuilder()
          .also {
            it.externalMeasurementConsumerId = externalMeasurementConsumerId
            it.externalCertificateId = createdCertificate.externalCertificateId
          }
          .build()
      )

    assertThat(certificate).isEqualTo(createdCertificate)
  }

  @Test
  fun `getCertificate fails for missing DataProviderCertificate`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        certificatesService.getCertificate(
          GetCertificateRequest.newBuilder()
            .apply {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              externalCertificateId = EXTERNAL_CERTIFICATE_ID
            }
            .build()
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createCertificate fails due to DataProvider owner not_found `() = runBlocking {
    val certificate =
      Certificate.newBuilder()
        .also {
          it.externalDataProviderId = 5678
          it.notValidBeforeBuilder.seconds = 12345
          it.notValidAfterBuilder.seconds = 23456
          it.detailsBuilder.x509Der = X509_DER
        }
        .build()

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("INVALID_ARGUMENT: DataProvider not found")
  }

  @Test
  fun `createCertificate suceeds for DataProviderCertificate`() = runBlocking {
    val externalDataProviderId = insertDataProvider()
    val certificate =
      Certificate.newBuilder()
        .also {
          it.externalDataProviderId = externalDataProviderId
          it.notValidBeforeBuilder.seconds = 12345
          it.notValidAfterBuilder.seconds = 23456
          it.detailsBuilder.x509Der = X509_DER
        }
        .build()

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
  fun `getCertificate succeeds for DataProviderCertificate`() = runBlocking {
    val externalDataProviderId = insertDataProvider()

    val request =
      Certificate.newBuilder()
        .also {
          it.externalDataProviderId = externalDataProviderId
          it.notValidBeforeBuilder.seconds = 12345
          it.notValidAfterBuilder.seconds = 23456
          it.detailsBuilder.x509Der = X509_DER
        }
        .build()

    val createdCertificate = certificatesService.createCertificate(request)

    val certificate =
      certificatesService.getCertificate(
        GetCertificateRequest.newBuilder()
          .also {
            it.externalDataProviderId = externalDataProviderId
            it.externalCertificateId = createdCertificate.externalCertificateId
          }
          .build()
      )

    assertThat(certificate).isEqualTo(createdCertificate)
  }
}
