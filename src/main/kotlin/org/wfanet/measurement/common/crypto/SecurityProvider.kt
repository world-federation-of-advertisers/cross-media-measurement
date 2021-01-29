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

package org.wfanet.measurement.common.crypto

import java.io.File
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.Provider
import java.security.Security
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import org.conscrypt.Conscrypt
import org.wfanet.measurement.common.base64Decode

private const val CERTIFICATE_TYPE = "X.509"

private val conscryptProvider: Provider = Conscrypt.newProvider().also {
  Security.insertProviderAt(it, 1)
}

/** Primary JCE [Provider] for crypto operations. */
val jceProvider: Provider = conscryptProvider

private val certFactory = CertificateFactory.getInstance(CERTIFICATE_TYPE, jceProvider)

/** Reads an X.509 certificate from a PEM file. */
fun readCertificate(pemFile: File): X509Certificate {
  return pemFile.inputStream().use { fileInputStream ->
    certFactory.generateCertificate(fileInputStream)
  } as X509Certificate
}

/** Reads an X.509 certificate collection from a PEM file. */
fun readCertificateCollection(pemFile: File): Collection<X509Certificate> {
  @Suppress("UNCHECKED_CAST") // Underlying mutable collection never exposed.
  return pemFile.inputStream().use { fileInputStream ->
    certFactory.generateCertificates(fileInputStream)
  } as Collection<X509Certificate>
}

/** Reads a private key from a PKCS#8-encoded PEM file. */
fun readPrivateKey(pemFile: File, algorithm: String): PrivateKey {
  return KeyFactory.getInstance(algorithm, jceProvider).generatePrivate(readKey(pemFile))
}

private fun readKey(pemFile: File): PKCS8EncodedKeySpec {
  return PKCS8EncodedKeySpec(PemIterable(pemFile).single())
}

class PemIterable(private val pemFile: File) : Iterable<ByteArray> {
  override fun iterator(): Iterator<ByteArray> = iterator {
    pemFile.bufferedReader().use { reader ->
      var line = reader.readLine()
      while (line != null) {
        check(line.startsWith(BEGIN_PREFIX)) { "Expected $BEGIN_PREFIX" }
        line = reader.readLine() ?: error("Unexpected end of file")

        val buffer = StringBuffer()
        do {
          buffer.append(line)
          line = reader.readLine() ?: error("Unexpected end of file")
        } while (!line.startsWith(END_PREFIX))

        yield(buffer.toString().base64Decode())
        line = reader.readLine()
      }
    }
  }

  companion object {
    private const val BEGIN_PREFIX = "-----BEGIN"
    private const val END_PREFIX = "-----END"
  }
}
