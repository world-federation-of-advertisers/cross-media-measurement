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

package org.wfanet.panelmatch.common.certificates.testing

import java.security.PrivateKey
import java.security.cert.X509Certificate
import org.wfanet.panelmatch.common.certificates.CertificateAuthority

object TestCertificateAuthority : CertificateAuthority {
  override suspend fun generateX509CertificateAndPrivateKey(): Pair<X509Certificate, PrivateKey> {
    return TestCertificateManager.CERTIFICATE to TestCertificateManager.PRIVATE_KEY
  }
}
