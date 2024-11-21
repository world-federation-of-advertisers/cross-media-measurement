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

import java.security.KeyPair
import java.security.PrivateKey
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.panelmatch.common.certificates.CertificateAuthority
import org.wfanet.panelmatch.common.certificates.CertificateSigningRequests.generateCsrFromKeyPair
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.acmpca.model.ASN1Subject
import software.amazon.awssdk.services.acmpca.model.ApiPassthrough
import software.amazon.awssdk.services.acmpca.model.ExtendedKeyUsage
import software.amazon.awssdk.services.acmpca.model.ExtendedKeyUsageType
import software.amazon.awssdk.services.acmpca.model.Extensions
import software.amazon.awssdk.services.acmpca.model.GeneralName
import software.amazon.awssdk.services.acmpca.model.GetCertificateRequest
import software.amazon.awssdk.services.acmpca.model.IssueCertificateRequest
import software.amazon.awssdk.services.acmpca.model.KeyUsage
import software.amazon.awssdk.services.acmpca.model.SigningAlgorithm
import software.amazon.awssdk.services.acmpca.model.Validity

/**
 * Defines the template ARN used by AWS private CA to issue certificate.
 *
 * More information:
 * https://docs.aws.amazon.com/acm-pca/latest/userguide/UsingTemplates.html#BlankSubordinateCACertificate_PathLen0_APIPassthrough
 */
const val AWS_CERTIFICATE_TEMPLATE_ARN =
  "arn:aws:acm-pca:::template/BlankSubordinateCACertificate_PathLen0_APIPassthrough/V1"

class CertificateAuthority(
  private val context: CertificateAuthority.Context,
  private val certificateAuthorityArn: String,
  private val client: CreateCertificateClient,
  private val signatureAlgorithm: SignatureAlgorithm,
  private val generateKeyPair: () -> KeyPair,
) : CertificateAuthority {

  private val certificateParams =
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
          .subjectAlternativeNames(GeneralName.builder().dnsName(context.dnsName).build())
          .build()
      )
      .subject(
        ASN1Subject.builder()
          .commonName(context.commonName)
          .organization(context.organization)
          .build()
      )
      .build()

  private val certificateLifetime =
    Validity.builder().value(context.validDays.toLong()).type("DAYS").build()

  override suspend fun generateX509CertificateAndPrivateKey(): Pair<X509Certificate, PrivateKey> {
    val keyPair: KeyPair = generateKeyPair()
    val privateKey: PrivateKey = keyPair.private

    val issueRequest =
      IssueCertificateRequest.builder()
        .templateArn(AWS_CERTIFICATE_TEMPLATE_ARN)
        .apiPassthrough(certificateParams)
        .certificateAuthorityArn(certificateAuthorityArn)
        .csr(
          SdkBytes.fromByteArray(
            generateCsrFromKeyPair(
                keyPair,
                context.commonName,
                context.organization,
                signatureAlgorithm,
              )
              .toByteArray()
          )
        )
        .signingAlgorithm(signatureAlgorithm.toAwsSigningAlgorithm())
        .validity(certificateLifetime)
        .build()

    val issueResponse = client.issueCertificate(issueRequest)

    val certificateArn = issueResponse.certificateArn()

    val getRequest =
      GetCertificateRequest.builder()
        .certificateArn(certificateArn)
        .certificateAuthorityArn(certificateAuthorityArn)
        .build()

    val getResponse = client.getCertificate(getRequest)

    return readCertificate(getResponse.certificate().byteInputStream()) to privateKey
  }

  private fun SignatureAlgorithm.toAwsSigningAlgorithm(): SigningAlgorithm {
    return when (this) {
      SignatureAlgorithm.ECDSA_WITH_SHA256 -> SigningAlgorithm.SHA256_WITHECDSA
      SignatureAlgorithm.ECDSA_WITH_SHA384 -> SigningAlgorithm.SHA384_WITHECDSA
      SignatureAlgorithm.ECDSA_WITH_SHA512 -> SigningAlgorithm.SHA512_WITHECDSA
      SignatureAlgorithm.SHA_256_WITH_RSA_ENCRYPTION -> SigningAlgorithm.SHA256_WITHRSA
      SignatureAlgorithm.SHA_384_WITH_RSA_ENCRYPTION -> SigningAlgorithm.SHA384_WITHRSA
      SignatureAlgorithm.SHA_512_WITH_RSA_ENCRYPTION -> SigningAlgorithm.SHA512_WITHRSA
    }
  }
}
