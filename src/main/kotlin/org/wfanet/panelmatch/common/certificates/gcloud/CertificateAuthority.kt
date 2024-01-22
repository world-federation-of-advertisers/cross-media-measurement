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
import com.google.cloud.security.privateca.v1.CertificateConfig.SubjectConfig
import com.google.cloud.security.privateca.v1.CreateCertificateRequest
import com.google.cloud.security.privateca.v1.KeyUsage
import com.google.cloud.security.privateca.v1.KeyUsage.ExtendedKeyUsageOptions
import com.google.cloud.security.privateca.v1.KeyUsage.KeyUsageOptions
import com.google.cloud.security.privateca.v1.Subject
import com.google.cloud.security.privateca.v1.SubjectAltNames
import com.google.cloud.security.privateca.v1.X509Parameters
import com.google.cloud.security.privateca.v1.X509Parameters.CaOptions
import java.security.KeyPair
import java.security.PrivateKey
import java.security.PublicKey
import java.security.cert.X509Certificate
import java.time.Duration
import org.wfanet.measurement.common.crypto.generateKeyPair
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.panelmatch.common.certificates.CertificateAuthority
import org.wfanet.panelmatch.common.toProto

/**
 * Fixed parameters for X509 certificates that allow digital signatures, key encipherment, and
 * certificate signing.
 *
 * This enables serverAuth extended key usage (see
 * https://datatracker.ietf.org/doc/html/rfc5280.html#section-4.2.1.12).
 */
val X509_PARAMETERS: X509Parameters =
  X509Parameters.newBuilder()
    .setKeyUsage(
      KeyUsage.newBuilder()
        .setBaseKeyUsage(
          KeyUsageOptions.newBuilder()
            .setDigitalSignature(true)
            .setKeyEncipherment(true)
            .setCertSign(true)
            .build()
        )
        .setExtendedKeyUsage(ExtendedKeyUsageOptions.newBuilder().setServerAuth(true).build())
        .build()
    )
    .setCaOptions(CaOptions.newBuilder().setIsCa(true).buildPartial())
    .build()

class CertificateAuthority(
  context: CertificateAuthority.Context,
  projectId: String,
  caLocation: String,
  poolId: String,
  private val certificateAuthorityName: String,
  private val client: CreateCertificateClient,
  private val generateKeyPair: () -> KeyPair = { generateKeyPair("EC") },
) : CertificateAuthority {

  private val subjectConfig =
    SubjectConfig.newBuilder()
      .setSubject(
        Subject.newBuilder()
          .setCommonName(context.commonName)
          .setOrganization(context.organization)
          .build()
      )
      .setSubjectAltName(SubjectAltNames.newBuilder().addDnsNames(context.dnsName).build())
      .build()

  private val caPoolName = CaPoolName.of(projectId, caLocation, poolId).toString()

  private val certificateLifetime = Duration.ofDays(context.validDays.toLong())

  override suspend fun generateX509CertificateAndPrivateKey(): Pair<X509Certificate, PrivateKey> {

    val keyPair: KeyPair = generateKeyPair()
    val privateKey: PrivateKey = keyPair.private
    val publicKey: PublicKey = keyPair.public

    val cloudPublicKeyInput = publicKey.toGCloudPublicKey()

    val certificate: Certificate =
      Certificate.newBuilder()
        .setConfig(
          CertificateConfig.newBuilder()
            .setPublicKey(cloudPublicKeyInput)
            .setSubjectConfig(subjectConfig)
            .setX509Config(X509_PARAMETERS)
            .build()
        )
        .setLifetime(certificateLifetime.toProto())
        .build()

    val certificateRequest: CreateCertificateRequest =
      CreateCertificateRequest.newBuilder()
        .setParent(caPoolName)
        .setCertificate(certificate)
        .setIssuingCertificateAuthorityId(certificateAuthorityName)
        .build()

    val response: Certificate = client.createCertificate(certificateRequest)

    return readCertificate(response.pemCertificate.byteInputStream()) to privateKey
  }
}
