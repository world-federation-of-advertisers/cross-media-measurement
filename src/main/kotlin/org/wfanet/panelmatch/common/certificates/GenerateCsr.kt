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

package org.wfanet.panelmatch.common.certificates

import java.io.StringWriter
import java.security.KeyPair
import javax.security.auth.x500.X500Principal
import org.bouncycastle.openssl.jcajce.JcaPEMWriter
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder


/**
 * Generates a PEM format CSR through BouncyCastle API for a given private key, organization and common name.
 */
fun generateCsrFromPrivateKey(keyPair: KeyPair, organization: String, commonName: String, algorithm: String): String {
  val pkcs10CsrBuilder = JcaPKCS10CertificationRequestBuilder(X500Principal("O=$organization, CN=$commonName"), keyPair.public)
  val csBuilder = JcaContentSignerBuilder(algorithm)
  val signer = csBuilder.build(keyPair.private)
  val strWriter = StringWriter()
  val pemWriter = JcaPEMWriter(strWriter)
  pemWriter.writeObject(pkcs10CsrBuilder.build(signer))
  pemWriter.close()
  strWriter.close()
  return strWriter.toString()
}
