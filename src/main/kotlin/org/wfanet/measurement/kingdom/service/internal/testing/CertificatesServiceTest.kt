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
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
<<<<<<< HEAD
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetCertificateRequestKt
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerKt
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelProviderKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.getCertificateRequest
import org.wfanet.measurement.internal.kingdom.measurementConsumer
import org.wfanet.measurement.internal.kingdom.modelProvider
=======
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.certificate
>>>>>>> 7499c463 (continuing)
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private const val EXTERNAL_CERTIFICATE_ID = 123L
private val EXTERNAL_DUCHY_IDS = listOf("duchy_1", "duchy_2", "duchy_3")
private const val NOT_AN_ID = 13579L

private val TEST_INSTANT = Instant.ofEpochMilli(123456789L)
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")

private val CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")

private val CERTIFICATE = certificate {
  notValidBefore = timestamp { seconds = 12345 }
  notValidAfter = timestamp { seconds = 23456 }
  subjectKeyIdentifier = ByteString.copyFromUtf8("This is an SKID")
  details = CertificateKt.details { x509Der = CERTIFICATE_DER }
}

@RunWith(JUnit4::class)
abstract class CertificatesServiceTest<T : CertificatesCoroutineImplBase> {
  @get:Rule val duchyIdSetter = DuchyIdSetter(EXTERNAL_DUCHY_IDS)

  protected data class Services<T>(
    val certificatesService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val modelProvidersService: ModelProvidersCoroutineImplBase
  )

  private val clock: Clock = TestClockWithNamedInstants(TEST_INSTANT)
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var certificatesService: T
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var modelProvidersService: ModelProvidersCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

<<<<<<< HEAD
  private suspend fun insertMeasurementConsumer(): Long {
    val measurementConsumer =
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          certificate =
            CERTIFICATE.copy {
              subjectKeyIdentifier = ByteString.copyFromUtf8("This is an MC SKID")
            }
          details =
            MeasurementConsumerKt.details {
              apiVersion = "v2alpha"
              publicKey = PUBLIC_KEY
              publicKeySignature = PUBLIC_KEY_SIGNATURE
            }
        }
      )
    return measurementConsumer.externalMeasurementConsumerId
  }

  private suspend fun insertDataProvider(): Long {
    val dataProvider =
      dataProvidersService.createDataProvider(
        dataProvider {
          certificate =
            CERTIFICATE.copy { subjectKeyIdentifier = ByteString.copyFromUtf8("This is a DP SKID") }
          details =
            DataProviderKt.details {
              apiVersion = "v2alpha"
              publicKey = PUBLIC_KEY
              publicKeySignature = PUBLIC_KEY_SIGNATURE
            }
        }
      )
    return dataProvider.externalDataProviderId
  }

  private suspend fun insertModelProvider(): Long {
    val modelProvider =
      modelProvidersService.createModelProvider(
        modelProvider {
          certificate =
            CERTIFICATE.copy {
              subjectKeyIdentifier = ByteString.copyFromUtf8("This is an MP SKID")
            }
          details =
            ModelProviderKt.details {
              apiVersion = "v2alpha"
              publicKey = PUBLIC_KEY
              publicKeySignature = PUBLIC_KEY_SIGNATURE
            }
        }
      )
    return modelProvider.externalModelProviderId
  }

=======
>>>>>>> 7499c463 (continuing)
  @Before
  fun initServices() {
    val services = newServices(idGenerator)
    certificatesService = services.certificatesService
    measurementConsumersService = services.measurementConsumersService
    dataProvidersService = services.dataProvidersService
    modelProvidersService = services.modelProvidersService
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
    val certificate = CERTIFICATE.copy { clearParent() }

    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  private fun assertGetFailsWithMissingCertificate(init: GetCertificateRequestKt.Dsl.() -> Unit) {
    val request = getCertificateRequest {
      init()
      externalCertificateId = EXTERNAL_CERTIFICATE_ID
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { certificatesService.getCertificate(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getCertificate fails for missing certificates`() = runBlocking {
    assertGetFailsWithMissingCertificate { externalDuchyId = EXTERNAL_DUCHY_IDS[0] }

    val dataProviderId = insertDataProvider()
    assertGetFailsWithMissingCertificate { externalDataProviderId = dataProviderId }

    val measurementConsumerId = insertMeasurementConsumer()
    assertGetFailsWithMissingCertificate { externalMeasurementConsumerId = measurementConsumerId }

    val modelProviderId = insertModelProvider()
    assertGetFailsWithMissingCertificate { externalModelProviderId = modelProviderId }
  }

  private fun assertCreateFailsWithMissingOwner(
    expectedMessage: String,
    init: CertificateKt.Dsl.() -> Unit
  ) {
    val certificate = CERTIFICATE.copy { init() }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { certificatesService.createCertificate(certificate) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains(expectedMessage)
  }

  @Test
  fun `createCertificate fails due to owner not_found`() {
    assertCreateFailsWithMissingOwner("Duchy not found") { externalDuchyId = "missing-duchy-id" }
    assertCreateFailsWithMissingOwner("DataProvider not found") {
      externalDataProviderId = NOT_AN_ID
    }
    assertCreateFailsWithMissingOwner("MeasurementConsumer not found") {
      externalMeasurementConsumerId = NOT_AN_ID
    }
    assertCreateFailsWithMissingOwner("ModelProvider not found") {
      externalModelProviderId = NOT_AN_ID
    }
  }

  private fun assertCreateCertificateSucceeds(init: CertificateKt.Dsl.() -> Unit) {
    val requestCertificate =
      CERTIFICATE.copy {
        init()
        subjectKeyIdentifier = ByteString.copyFromUtf8("Some unique SKID for $parentCase")
      }
    val createdCertificate = runBlocking {
      certificatesService.createCertificate(requestCertificate)
    }

    val expectedCertificate =
      requestCertificate.copy { externalCertificateId = createdCertificate.externalCertificateId }
    assertThat(createdCertificate).isEqualTo(expectedCertificate)
  }

  @Test
  fun `createCertificate succeeds`() = runBlocking {
    assertCreateCertificateSucceeds { externalDuchyId = EXTERNAL_DUCHY_IDS[0] }

    val dataProviderId = insertDataProvider()
    assertCreateCertificateSucceeds { externalDataProviderId = dataProviderId }

    val measurementConsumerId = insertMeasurementConsumer()
    assertCreateCertificateSucceeds { externalMeasurementConsumerId = measurementConsumerId }

    val modelProviderId = insertModelProvider()
    assertCreateCertificateSucceeds { externalModelProviderId = modelProviderId }
  }

  private fun assertGetCertificateSucceeds(init: CertificateKt.Dsl.() -> Unit) = runBlocking {
    val requestCertificate =
      CERTIFICATE.copy {
        init()
        subjectKeyIdentifier = ByteString.copyFromUtf8("Some unique SKID for $parentCase")
      }
    val createdCertificate = certificatesService.createCertificate(requestCertificate)
    val getRequest = getCertificateRequest {
      externalCertificateId = createdCertificate.externalCertificateId

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (requestCertificate.parentCase) {
        Certificate.ParentCase.EXTERNAL_DATA_PROVIDER_ID ->
          externalDataProviderId = requestCertificate.externalDataProviderId
        Certificate.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID ->
          externalMeasurementConsumerId = requestCertificate.externalMeasurementConsumerId
        Certificate.ParentCase.EXTERNAL_DUCHY_ID ->
          externalDuchyId = requestCertificate.externalDuchyId
        Certificate.ParentCase.EXTERNAL_MODEL_PROVIDER_ID ->
          externalModelProviderId = requestCertificate.externalModelProviderId
        Certificate.ParentCase.PARENT_NOT_SET -> error("Invalid parentCase")
      }
    }

    assertThat(certificatesService.getCertificate(getRequest)).isEqualTo(createdCertificate)
  }

  @Test
  fun `getCertificate succeeds`() = runBlocking {
    assertGetCertificateSucceeds { externalDuchyId = EXTERNAL_DUCHY_IDS[0] }

    val dataProviderId = insertDataProvider()
    assertGetCertificateSucceeds { externalDataProviderId = dataProviderId }

    val measurementConsumerId = insertMeasurementConsumer()
    assertGetCertificateSucceeds { externalMeasurementConsumerId = measurementConsumerId }

    val modelProviderId = insertModelProvider()
    assertGetCertificateSucceeds { externalModelProviderId = modelProviderId }
  }

  @Test
  fun `createCertificate fails due to subjectKeyIdentifier collision`() = runBlocking {
    val externalMeasurementConsumerId =
      population.createMeasurementConsumer(measurementConsumersService)
        .externalMeasurementConsumerId
    val certificate =
      CERTIFICATE.copy { this.externalMeasurementConsumerId = externalMeasurementConsumerId }

    certificatesService.createCertificate(certificate)
    val exception =
      assertFailsWith<StatusRuntimeException> { certificatesService.createCertificate(certificate) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception)
      .hasMessageThat()
      .contains("Certificate with the same subject key identifier (SKID) already exists.")
  }
<<<<<<< HEAD
=======

  @Test
  fun `createCertificate suceeds for MeasurementConsumerCertificate`() = runBlocking {
    val externalMeasurementConsumerId =
      population.createMeasurementConsumer(measurementConsumersService)
        .externalMeasurementConsumerId
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
    val externalMeasurementConsumerId =
      population.createMeasurementConsumer(measurementConsumersService)
        .externalMeasurementConsumerId

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

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `createCertificate suceeds for DataProviderCertificate`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId
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
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

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
>>>>>>> 7499c463 (continuing)
}
