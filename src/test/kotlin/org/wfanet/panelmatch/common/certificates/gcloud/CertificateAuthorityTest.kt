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

package org.wfanet.panelmatch.common.certificates.gcloud

import com.google.cloud.security.privateca.v1.CaPoolName
import com.google.cloud.security.privateca.v1.Certificate
import com.google.cloud.security.privateca.v1.CertificateConfig
import com.google.cloud.security.privateca.v1.CreateCertificateRequest
import com.google.cloud.security.privateca.v1.PublicKey as CloudPublicKey
import com.google.cloud.security.privateca.v1.Subject
import com.google.cloud.security.privateca.v1.SubjectAltNames
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import java.security.KeyPair
import java.time.Duration
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
import org.wfanet.panelmatch.common.toProto

private val CONTEXT =
  CertificateAuthority.Context(
    commonName = "some-common-name",
    organization = "some-org-name",
    dnsName = "some-domain-name",
    validDays = 5,
  )

private val CA_POOL_NAME = CaPoolName.of("some-project-id", "some-ca-location", "some-pool-id")
private const val CERTIFICATE_AUTHORITY_NAME = "some-certificate-authority-name"
private val ROOT_X509 by lazy { readCertificate(TestData.FIXED_CA_CERT_PEM_FILE) }
private val ROOT_PUBLIC_KEY by lazy { ROOT_X509.publicKey }
private val ROOT_PRIVATE_KEY_FILE by lazy {
  TestData.FIXED_CA_CERT_PEM_FILE.resolveSibling("ca.key")
}
private val CERTIFICATE_LIFETIME = Duration.ofDays(CONTEXT.validDays.toLong())
private val CLOUD_PUBLIC_KEY: CloudPublicKey = ROOT_PUBLIC_KEY.toGCloudPublicKey()

private val SUBJECT_CONFIG =
  CertificateConfig.SubjectConfig.newBuilder()
    .setSubject(
      Subject.newBuilder()
        .setCommonName(CONTEXT.commonName)
        .setOrganization(CONTEXT.organization)
        .build()
    )
    .setSubjectAltName(SubjectAltNames.newBuilder().addDnsNames(CONTEXT.dnsName).build())
    .build()

@RunWith(JUnit4::class)
class CertificateAuthorityTest {

  @Test
  fun generateX509CertificateAndPrivateKey() = runBlocking {
    val mockCreateCertificateClient: CreateCertificateClient = mock()

    val certificate: Certificate =
      Certificate.newBuilder()
        .setConfig(
          CertificateConfig.newBuilder()
            .setPublicKey(CLOUD_PUBLIC_KEY)
            .setSubjectConfig(SUBJECT_CONFIG)
            .setX509Config(X509_PARAMETERS)
            .build()
        )
        .setLifetime(CERTIFICATE_LIFETIME.toProto())
        .build()

    val expectedCertificateRequest: CreateCertificateRequest =
      CreateCertificateRequest.newBuilder()
        .setParent(CA_POOL_NAME.toString())
        .setCertificate(certificate)
        .setIssuingCertificateAuthorityId("some-certificate-authority-name")
        .build()

    whenever(mockCreateCertificateClient.createCertificate(any()))
      .thenReturn(
        Certificate.newBuilder()
          .setPemCertificate(TestData.FIXED_CA_CERT_PEM_FILE.readText())
          .build()
      )

    val certificateAuthority =
      CertificateAuthority(
        context = CONTEXT,
        projectId = CA_POOL_NAME.project,
        caLocation = CA_POOL_NAME.location,
        poolId = CA_POOL_NAME.caPool,
        certificateAuthorityName = CERTIFICATE_AUTHORITY_NAME,
        client = mockCreateCertificateClient,
        generateKeyPair = { KeyPair(ROOT_PUBLIC_KEY, readPrivateKey(ROOT_PRIVATE_KEY_FILE, "ec")) },
      )

    val (x509, privateKey) = certificateAuthority.generateX509CertificateAndPrivateKey()
    val signatureAlgorithm =
      SignatureAlgorithm.fromKeyAndHashAlgorithm(privateKey, HashAlgorithm.SHA256)!!

    val createCertificateRequest =
      argumentCaptor { verify(mockCreateCertificateClient).createCertificate(capture()) }.firstValue
    assertThat(createCertificateRequest).isEqualTo(expectedCertificateRequest)

    assertThat(x509.publicKey).isEqualTo(ROOT_PUBLIC_KEY)
    assertThat(readPrivateKey(ROOT_PRIVATE_KEY_FILE, "ec")).isEqualTo(privateKey)

    val data = "some-data-to-be-signed".toByteStringUtf8()
    val signature = privateKey.sign(signatureAlgorithm, data)

    assertThat(x509.verifySignature(signatureAlgorithm, data, signature)).isTrue()
  }
}
