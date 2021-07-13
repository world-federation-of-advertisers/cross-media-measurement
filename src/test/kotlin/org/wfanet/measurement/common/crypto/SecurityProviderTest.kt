package org.wfanet.measurement.common.crypto

import com.google.common.collect.Range
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.file.Paths
import java.security.cert.X509Certificate
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.byteStringOf
import org.wfanet.measurement.common.getRuntimePath

private const val KEY_ALGORITHM = "EC"
private val SERVER_SKID =
  byteStringOf(
    0xE7,
    0xB3,
    0xB5,
    0x45,
    0x77,
    0x1B,
    0xC2,
    0xB9,
    0xA1,
    0x88,
    0x02,
    0x07,
    0x90,
    0x3F,
    0x87,
    0xA5,
    0xC4,
    0x2C,
    0x63,
    0xA8
  )

private val TESTDATA_DIR =
  Paths.get(
    "wfa_measurement_system",
    "src",
    "main",
    "kotlin",
    "org",
    "wfanet",
    "measurement",
    "common",
    "crypto",
    "testing",
    "testdata"
  )

private val DATA = ByteString.copyFromUtf8("I am some data to sign")
private val ALT_DATA = ByteString.copyFromUtf8("I am some alternative data")

@RunWith(JUnit4::class)
class SecurityProviderTest {
  @Test
  fun `readCertificate reads cert from PEM file`() {
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)

    assertThat(certificate.subjectDN.name).isEqualTo("CN=server.example.com,O=Server")
  }

  @Test
  fun `readPrivateKey reads key from PKCS#8 PEM file`() {
    val privateKey = readPrivateKey(SERVER_KEY_FILE, KEY_ALGORITHM)

    assertThat(privateKey.format).isEqualTo("PKCS#8")
  }

  @Test
  fun `sign returns signature of correct size`() {
    val privateKey = readPrivateKey(SERVER_KEY_FILE, KEY_ALGORITHM)
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)

    val signature = privateKey.sign(certificate, DATA)

    // DER-encoded ECDSA signature using 256-bit key can be 70, 71, or 72 bytes.
    assertThat(signature.size()).isIn(Range.closed(70, 72))
  }

  @Test
  fun `verifySignature returns true for valid signature`() {
    val privateKey = readPrivateKey(SERVER_KEY_FILE, KEY_ALGORITHM)
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)
    val signature = privateKey.sign(certificate, DATA)

    assertTrue(certificate.verifySignature(DATA, signature))
  }

  @Test
  fun `verifySignature returns false for signature from different data`() {
    val privateKey = readPrivateKey(SERVER_KEY_FILE, KEY_ALGORITHM)
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)
    val signature = privateKey.sign(certificate, DATA)

    assertFalse(certificate.verifySignature(ALT_DATA, signature))
  }

  @Test
  fun `subjectKeyIdentifier returns SKID`() {
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)

    assertThat(certificate.subjectKeyIdentifier).isEqualTo(SERVER_SKID)
  }

  @Test
  fun `authorityKeyIdentifier returns SKID of issuer`() {
    val issuerCertificate = readCertificate(CA_CERT_PEM_FILE)
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)

    assertThat(certificate.authorityKeyIdentifier).isEqualTo(issuerCertificate.subjectKeyIdentifier)
  }

  companion object {
    private val CA_CERT_PEM_FILE = getRuntimePath(TESTDATA_DIR.resolve("ca.pem"))!!.toFile()
    private val SERVER_CERT_PEM_FILE = getRuntimePath(TESTDATA_DIR.resolve("server.pem"))!!.toFile()
    private val SERVER_KEY_FILE = getRuntimePath(TESTDATA_DIR.resolve("server.key"))!!.toFile()
  }
}
