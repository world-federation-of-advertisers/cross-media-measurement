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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import io.grpc.Status
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate as InternalCertificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.certificateDetails as internalCertificateDetails

private const val CERTIFICATE_DER_FIELD_NAME = "certificate_der"

/**
 * Parses a `certificate_der` [ByteString] request field into an [InternalCertificate].
 *
 * @throws io.grpc.StatusRuntimeException if [certificateDer] cannot be parsed into an
 *   [InternalCertificate].
 */
fun parseCertificateDer(certificateDer: ByteString): InternalCertificate = internalCertificate {
  fillCertificateFromDer(certificateDer)
}

fun CertificateKt.Dsl.fillCertificateFromDer(certificateDer: ByteString) {
  grpcRequire(!certificateDer.isEmpty) { "$CERTIFICATE_DER_FIELD_NAME is not specified" }

  val x509Certificate: X509Certificate =
    try {
      readCertificate(certificateDer)
    } catch (e: CertificateException) {
      throw Status.INVALID_ARGUMENT.withCause(e)
        .withDescription("Cannot parse $CERTIFICATE_DER_FIELD_NAME")
        .asRuntimeException()
    }
  fillFromX509(x509Certificate, certificateDer)
}

fun CertificateKt.Dsl.fillFromX509(x509Certificate: X509Certificate, encoded: ByteString? = null) {
  val skid: ByteString =
    grpcRequireNotNull(x509Certificate.subjectKeyIdentifier) {
      "Cannot find Subject Key Identifier of $CERTIFICATE_DER_FIELD_NAME"
    }

  subjectKeyIdentifier = skid
  notValidBefore = x509Certificate.notBefore.toInstant().toProtoTime()
  notValidAfter = x509Certificate.notAfter.toInstant().toProtoTime()
  details = internalCertificateDetails {
    x509Der = encoded ?: x509Certificate.encoded.toByteString()
  }
}
