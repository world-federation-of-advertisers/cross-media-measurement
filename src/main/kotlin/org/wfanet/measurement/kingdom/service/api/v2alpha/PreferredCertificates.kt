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
import io.grpc.Status
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate as InternalCertificate

private const val PREFERRED_CERTIFICATE_DER_FIELD_NAME = "preferred_certificate_der"

/**
 * Parses a `preferred_certificate_der` [ByteString] request field into an [InternalCertificate].
 *
 * @throws io.grpc.StatusRuntimeException if [preferredCertificateDer] cannot be parsed into an
 * [InternalCertificate].
 */
fun parsePreferredCertificateDer(preferredCertificateDer: ByteString): InternalCertificate {
  grpcRequire(!preferredCertificateDer.isEmpty) {
    "$PREFERRED_CERTIFICATE_DER_FIELD_NAME is not specified"
  }

  val x509Certificate: X509Certificate =
    try {
      readCertificate(preferredCertificateDer)
    } catch (e: CertificateException) {
      throw Status.INVALID_ARGUMENT
        .withCause(e)
        .withDescription("Cannot parse $PREFERRED_CERTIFICATE_DER_FIELD_NAME")
        .asRuntimeException()
    }
  val skid: ByteString =
    grpcRequireNotNull(x509Certificate.subjectKeyIdentifier) {
      "Cannot find Subject Key Identifier of $PREFERRED_CERTIFICATE_DER_FIELD_NAME"
    }

  return buildInternalCertificate {
    subjectKeyIdentifier = skid
    notValidBefore = x509Certificate.notBefore.toInstant().toProtoTime()
    notValidAfter = x509Certificate.notAfter.toInstant().toProtoTime()
    detailsBuilder.x509Der = preferredCertificateDer
  }
}

private inline fun buildInternalCertificate(
  fill: (@Builder InternalCertificate.Builder).() -> Unit
) = InternalCertificate.newBuilder().apply(fill).build()
