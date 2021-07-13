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

import com.google.protobuf.ByteString
import java.io.File
import java.io.InputStream
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.Provider
import java.security.Security
import java.security.Signature
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import org.conscrypt.Conscrypt
import org.wfanet.measurement.common.base64Decode

private const val CERTIFICATE_TYPE = "X.509"
private const val SUBJECT_KEY_IDENTIFIER_OID = "2.5.29.14"
private const val AUTHORITY_KEY_IDENTIFIER_OID = "2.5.29.35"
private const val OCTET_STRING_TAG = 0x04.toByte()
private const val SEQUENCE_TAG = 0x30.toByte()
/** Context-specific tag for keyIdentifier within AuthorityKeyIdentifier extension. */
private const val KEY_IDENTIFIER_TAG = 0x80.toByte()

private val conscryptProvider: Provider =
  Conscrypt.newProvider().also { Security.insertProviderAt(it, 1) }

/** Primary JCE [Provider] for crypto operations. */
val jceProvider: Provider = conscryptProvider

private val certFactory = CertificateFactory.getInstance(CERTIFICATE_TYPE, jceProvider)

/**
 * Reads an X.509 certificate from a PEM file.
 *
 * @throws java.security.cert.CertificateException on parsing errors
 */
fun readCertificate(pemFile: File): X509Certificate = readCertificate(pemFile::inputStream)

/**
 * Reads an X.509 certificate from a DER-encoded [ByteString].
 *
 * @throws java.security.cert.CertificateException on parsing errors
 */
fun readCertificate(der: ByteString): X509Certificate = readCertificate(der::newInput)

private inline fun readCertificate(newInputStream: () -> InputStream): X509Certificate {
  return newInputStream().use { certFactory.generateCertificate(it) } as X509Certificate
}

/**
 * Reads an X.509 certificate collection from a PEM file.
 *
 * @throws java.security.cert.CertificateException on parsing errors
 */
fun readCertificateCollection(pemFile: File): Collection<X509Certificate> {
  @Suppress("UNCHECKED_CAST") // Underlying mutable collection never exposed.
  return pemFile.inputStream().use { fileInputStream ->
    certFactory.generateCertificates(fileInputStream)
  } as
    Collection<X509Certificate>
}

/** Reads a private key from a PKCS#8-encoded PEM file. */
fun readPrivateKey(pemFile: File, algorithm: String): PrivateKey {
  return KeyFactory.getInstance(algorithm, jceProvider).generatePrivate(readKey(pemFile))
}

private fun readKey(pemFile: File): PKCS8EncodedKeySpec {
  return PKCS8EncodedKeySpec(PemIterable(pemFile).single())
}

@JvmName("signInternal")
private fun sign(
  privateKey: PrivateKey,
  certificate: X509Certificate,
  data: ByteString
): ByteString {
  val signer = Signature.getInstance(certificate.sigAlgName, jceProvider)
  signer.initSign(privateKey)
  signer.update(data.asReadOnlyByteBuffer())
  return ByteString.copyFrom(signer.sign())
}

@JvmName("verifyInternal")
private fun verifySignature(
  certificate: X509Certificate,
  data: ByteString,
  signature: ByteString
): Boolean {
  val verifier = Signature.getInstance(certificate.sigAlgName, jceProvider)
  verifier.initVerify(certificate)
  verifier.update(data.asReadOnlyByteBuffer())
  return verifier.verify(signature.toByteArray())
}

private class PemIterable(private val pemFile: File) : Iterable<ByteArray> {
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

/** The keyIdentifier from the SubjectKeyIdentifier (AKI) X.509 extension. */
val X509Certificate.subjectKeyIdentifier: ByteString
  get() {
    val extension: ByteArray = getExtensionValue(SUBJECT_KEY_IDENTIFIER_OID)
    check(extension.size >= 4 && extension[2] == OCTET_STRING_TAG) {
      "Cannot find keyIdentifier in SKI extension"
    }

    val length = extension[3].toInt() // Assuming short form, where length <= 127 bytes.
    return ByteString.copyFrom(extension, 4, length)
  }

/** The keyIdentifier from the AuthorityKeyIdentifier (AKI) X.509 extension. */
val X509Certificate.authorityKeyIdentifier: ByteString
  get() {
    val extension: ByteArray = getExtensionValue(AUTHORITY_KEY_IDENTIFIER_OID)
    check(
      extension.size >= 6 && extension[2] == SEQUENCE_TAG && extension[4] == KEY_IDENTIFIER_TAG
    ) { "Cannot find keyIdentifier in AKI extension" }

    val length = extension[5].toInt() // Assuming short form, where length <= 127 bytes.
    return ByteString.copyFrom(extension, 6, length)
  }

/**
 * Signs [data] using this [PrivateKey].
 *
 * @param certificate the [X509Certificate] that can be used to verify the signature
 */
fun PrivateKey.sign(certificate: X509Certificate, data: ByteString) = sign(this, certificate, data)

/**
 * Verifies that the [signature] for [data] was signed by the entity represented by this
 * [X509Certificate].
 */
fun X509Certificate.verifySignature(data: ByteString, signature: ByteString) =
  verifySignature(this, data, signature)
