package org.wfanet.measurement.api.v2alpha.tools

import com.google.common.truth.Truth
import com.google.protobuf.kotlin.toByteString
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.eq
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.CreateCertificateRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.ListCertificatesRequestKt
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.listCertificatesRequest
import org.wfanet.measurement.api.v2alpha.listCertificatesResponse
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyAndCapture

private val SECRET_FILES_PATH: Path =
  checkNotNull(
    getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )
  )
private const val EDP_DISPLAY_NAME = "edp1"
private const val EDP_ID = "someDataProvider"
private const val EDP_NAME = "dataProviders/$EDP_ID"

@RunWith(JUnit4::class)
class CertificateRegistrarTest {

  private val edpResultCertificate =
      readCertificate(
          SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_result_cs_cert.der").toFile()
      )
  private val dataProviderResultCsCertificateKey =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))
  private val dataProviderResultCsCertificate = certificate {
      name = dataProviderResultCsCertificateKey.toName()
      x509Der = edpResultCertificate.encoded.toByteString()
      subjectKeyIdentifier = edpResultCertificate.subjectKeyIdentifier!!
  }

  private val certificatesServiceMock: CertificatesGrpcKt.CertificatesCoroutineImplBase =
      mockService {
          onBlocking {
              listCertificates(
                  eq(
                      listCertificatesRequest {
                          parent = EDP_NAME
                          filter =
                              ListCertificatesRequestKt.filter {
                                  subjectKeyIdentifiers +=
                                      edpResultCertificate.subjectKeyIdentifier!!
                              }
                      }
                  )
              )
          }
              .thenReturn(listCertificatesResponse { certificates += dataProviderResultCsCertificate })
      }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(certificatesServiceMock) }

  private val certificatesStub: CertificatesGrpcKt.CertificatesCoroutineStub by lazy {
      CertificatesGrpcKt.CertificatesCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `registerCertificate registers Certificate`() {
    val dataProviderCertificateRegistrar =
        CertificateRegistrar(
            EDP_DISPLAY_NAME,
            certificatesStub,
        )

      runBlocking { dataProviderCertificateRegistrar.registerCertificate(edpResultCertificate) }

    // Verify certificate request contains information from EDP_DATA.
    val createCertificateRequest: CreateCertificateRequest =
        verifyAndCapture(
            certificatesServiceMock,
            CertificatesGrpcKt.CertificatesCoroutineImplBase::createCertificate
        )
    Truth.assertThat(createCertificateRequest.parent).isEqualTo(EDP_NAME)
    Truth.assertThat(createCertificateRequest.certificate.subjectKeyIdentifier)
      .isEqualTo(edpResultCertificate.subjectKeyIdentifier)
    Truth.assertThat(createCertificateRequest.certificate.x509Der)
      .isEqualTo(edpResultCertificate.encoded.toByteString())
  }
}
