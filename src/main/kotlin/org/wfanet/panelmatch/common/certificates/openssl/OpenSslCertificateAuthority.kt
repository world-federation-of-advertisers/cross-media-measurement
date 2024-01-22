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

package org.wfanet.panelmatch.common.certificates.openssl

import java.io.File
import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.util.UUID
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.panelmatch.common.certificates.CertificateAuthority

/**
 * [CertificateAuthority] that calls OpenSSL in a subprocess.
 *
 * DO NOT USE THIS IN PRODUCTION WITHOUT ADDITIONAL SAFEGUARDS!
 *
 * Be very careful about allowing code to access your root private key!
 *
 * Additionally, arbitrary code execution could occur because OpenSSL CLI arguments are passed via
 * string interpolation with no sanitization. You MUST pre-sanitize the
 * [CertificateAuthority.Context].
 */
class OpenSslCertificateAuthority(
  private val context: CertificateAuthority.Context,
  private val rootPrivateKeyFile: File,
  private val rootCertificateFile: File,
  private val baseDirectory: File,
) : CertificateAuthority {
  init {
    require(baseDirectory.exists() && baseDirectory.isDirectory) {
      "Not an existing directory: ${baseDirectory.path}"
    }
  }

  override suspend fun generateX509CertificateAndPrivateKey(): Pair<X509Certificate, PrivateKey> {
    val id = UUID.randomUUID().toString()
    val subdirectory = File(baseDirectory, id)
    check(subdirectory.mkdir())

    try {
      return GenerateKeyPair(rootPrivateKeyFile, rootCertificateFile, context, subdirectory)
        .generate()
    } finally {
      subdirectory.deleteRecursively()
    }
  }
}

private class GenerateKeyPair(
  private val rootPrivateKeyFile: File,
  private val rootCertificateFile: File,
  private val context: CertificateAuthority.Context,
  private val baseDir: File,
) {
  private val csrFile = File(baseDir, "csr")
  private val keyFile = File(baseDir, "key")
  private val cnfFile = File(baseDir, "cnf")
  private val pemFile = File(baseDir, "pem")

  fun generate(): Pair<X509Certificate, PrivateKey> {
    generateCsrAndKey()
    writeCnf()
    generateX509()

    val x509Certificate = readCertificate(pemFile)
    val privateKey = readPrivateKey(keyFile, "ec")

    return x509Certificate to privateKey
  }

  private fun generateCsrAndKey() {
    subprocess(
      baseDir,
      "openssl",
      "req",
      "-out",
      csrFile.relativeTo(baseDir).path,
      "-new",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:prime256v1",
      "-nodes",
      "-keyout",
      keyFile.relativeTo(baseDir).path,
      "-subj",
      "/O=${context.organization}/CN=${context.commonName}",
    )
  }

  private fun writeCnf() {
    cnfFile.writeText(
      """
      [usr_cert]
      keyUsage=nonRepudiation,digitalSignature,keyEncipherment
      authorityKeyIdentifier=keyid:always,issuer
      subjectKeyIdentifier=hash
      basicConstraints=CA:FALSE
      subjectAltName=DNS:${context.dnsName}
      """
        .trimIndent()
    )
  }

  private fun generateX509() {
    subprocess(
      baseDir,
      "openssl",
      "x509",
      "-in",
      csrFile.relativeTo(baseDir).path,
      "-out",
      pemFile.relativeTo(baseDir).path,
      "-days",
      context.validDays.toString(),
      "-req",
      "-CA",
      rootCertificateFile.relativeTo(baseDir).path,
      "-CAform",
      "PEM",
      "-CAkey",
      rootPrivateKeyFile.relativeTo(baseDir).path,
      "-CAcreateserial",
      "-extfile",
      cnfFile.relativeTo(baseDir).path,
      "-extensions",
      "usr_cert",
    )
  }
}

private fun subprocess(base: File, vararg args: String) {
  val process = ProcessBuilder(*args).directory(base).redirectErrorStream(true).start()
  val exitCode = process.waitFor()
  val output = process.inputStream.use { it.bufferedReader().readText() }
  check(exitCode == 0) {
    "Command ${args.joinToString(" ")} failed with code $exitCode. Output:\n$output"
  }
}
