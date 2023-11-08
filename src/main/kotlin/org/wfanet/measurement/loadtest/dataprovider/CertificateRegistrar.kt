// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.kotlin.toByteString
import io.grpc.StatusException
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier

class CertificateRegistrar(
  private val parentResourceName: String,
  private val certificatesStub: CertificatesCoroutineStub
) {

  private suspend fun createCertificate(
    dataProviderResourceName: String,
    certificate: Certificate
  ): Certificate {
    return try {
      certificatesStub.createCertificate(
        createCertificateRequest {
          parent = parentResourceName
          this.certificate = certificate
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error creating certificate for $dataProviderResourceName", e)
    }
  }

  /** Registers a [Certificate] for the [DataProvider]. */
  suspend fun registerCertificate(signingKeyHandle: SigningKeyHandle): Certificate {
    return try {
      val certificate = certificate {
        x509Der = signingKeyHandle.certificate.encoded.toByteString()
        subjectKeyIdentifier = signingKeyHandle.certificate.subjectKeyIdentifier!!
      }
      createCertificate(parentResourceName, certificate)
    } catch (e: StatusException) {
      throw Exception("Error creating certificate", e)
    }
  }

}
