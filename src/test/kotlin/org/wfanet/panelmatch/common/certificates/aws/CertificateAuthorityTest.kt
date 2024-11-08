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

package org.wfanet.panelmatch.common.certificates.aws

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import java.security.KeyPair
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.crypto.HashAlgorithm
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.sign
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.crypto.verifySignature
import org.wfanet.panelmatch.common.certificates.CertificateAuthority
import software.amazon.awssdk.services.acmpca.model.ASN1Subject
import software.amazon.awssdk.services.acmpca.model.ApiPassthrough
import software.amazon.awssdk.services.acmpca.model.ExtendedKeyUsage
import software.amazon.awssdk.services.acmpca.model.ExtendedKeyUsageType
import software.amazon.awssdk.services.acmpca.model.Extensions
import software.amazon.awssdk.services.acmpca.model.GeneralName
import software.amazon.awssdk.services.acmpca.model.GetCertificateRequest
import software.amazon.awssdk.services.acmpca.model.GetCertificateResponse
import software.amazon.awssdk.services.acmpca.model.IssueCertificateRequest
import software.amazon.awssdk.services.acmpca.model.IssueCertificateResponse
import software.amazon.awssdk.services.acmpca.model.KeyUsage
import software.amazon.awssdk.services.acmpca.model.SigningAlgorithm
import software.amazon.awssdk.services.acmpca.model.Validity

private val CONTEXT =
  CertificateAuthority.Context(
    commonName = "some-common-name",
    organization = "some-org-name",
    dnsName = "some-domain-name",
    validDays = 5,
  )

private const val CERTIFICATE_AUTHORITY_ARN = "some-ca-arn"
private const val CERTIFICATE_ARN = "some-cert-arn"
private val ROOT_X509 by lazy { readCertificate(TestData.FIXED_CA_CERT_PEM_FILE) }
private val ROOT_PUBLIC_KEY by lazy { ROOT_X509.publicKey }
private val ROOT_PRIVATE_KEY_FILE by lazy {
  TestData.FIXED_CA_CERT_PEM_FILE.resolveSibling("ca.key")
}
private val CERTIFICATE_LIFETIME =
  Validity.builder().value(CONTEXT.validDays.toLong()).type("DAYS").build()

@RunWith(JUnit4::class)
class CertificateAuthorityTest {
  @Test
  fun generateX509CertificateAndPrivateKey() = runBlocking {
    val mockCreateCertificateClient: CreateCertificateClient = mock()

    val expectedCertificateParams =
      ApiPassthrough.builder()
        .extensions(
          Extensions.builder()
            .keyUsage(
              KeyUsage.builder()
                .digitalSignature(true)
                .nonRepudiation(true)
                .keyEncipherment(true)
                .build()
            )
            .extendedKeyUsage(
              ExtendedKeyUsage.builder()
                .extendedKeyUsageType(ExtendedKeyUsageType.SERVER_AUTH)
                .build()
            )
            .subjectAlternativeNames(GeneralName.builder().dnsName(CONTEXT.dnsName).build())
            .build()
        )
        .subject(
          ASN1Subject.builder()
            .commonName(CONTEXT.commonName)
            .organization(CONTEXT.organization)
            .build()
        )
        .build()

    val expectedGetCertificateRequest =
      GetCertificateRequest.builder()
        .certificateArn(CERTIFICATE_ARN)
        .certificateAuthorityArn(CERTIFICATE_AUTHORITY_ARN)
        .build()

    whenever(mockCreateCertificateClient.issueCertificate(any()))
      .thenReturn(IssueCertificateResponse.builder().certificateArn(CERTIFICATE_ARN).build())

    whenever(mockCreateCertificateClient.getCertificate(any()))
      .thenReturn(
        GetCertificateResponse.builder()
          .certificate(TestData.FIXED_CA_CERT_PEM_FILE.readText())
          .build()
      )

    val certificateAuthority =
      CertificateAuthority(
        CONTEXT,
        CERTIFICATE_AUTHORITY_ARN,
        mockCreateCertificateClient,
        signatureAlgorithm = SignatureAlgorithm.ECDSA_WITH_SHA256,
        generateKeyPair = { KeyPair(ROOT_PUBLIC_KEY, readPrivateKey(ROOT_PRIVATE_KEY_FILE, "EC")) },
      )

    val (x509, privateKey) = certificateAuthority.generateX509CertificateAndPrivateKey()
    val signatureAlgorithm =
      SignatureAlgorithm.fromKeyAndHashAlgorithm(privateKey, HashAlgorithm.SHA256)!!

    val issueCertificateRequest: IssueCertificateRequest =
      argumentCaptor { verify(mockCreateCertificateClient).issueCertificate(capture()) }.firstValue
    assertThat(issueCertificateRequest.templateArn()).isEqualTo(AWS_CERTIFICATE_TEMPLATE_ARN)
    assertThat(issueCertificateRequest.apiPassthrough()).isEqualTo(expectedCertificateParams)
    assertThat(issueCertificateRequest.certificateAuthorityArn())
      .isEqualTo(CERTIFICATE_AUTHORITY_ARN)
    assertThat(issueCertificateRequest.signingAlgorithm())
      .isEqualTo(SigningAlgorithm.SHA256_WITHECDSA)
    assertThat(issueCertificateRequest.validity()).isEqualTo(CERTIFICATE_LIFETIME)

    val getCertificateRequest =
      argumentCaptor { verify(mockCreateCertificateClient).getCertificate(capture()) }.firstValue
    assertThat(getCertificateRequest).isEqualTo(expectedGetCertificateRequest)

    val data = "some-data-to-be-signed".toByteStringUtf8()
    val signature = privateKey.sign(signatureAlgorithm, data)

    assertThat(x509.verifySignature(signatureAlgorithm, data, signature)).isTrue()
  }
}
