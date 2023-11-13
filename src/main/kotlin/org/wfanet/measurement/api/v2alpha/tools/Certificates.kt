/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.api.v2alpha.tools

import com.google.protobuf.ByteString
import java.io.File
import java.security.PrivateKey
import java.security.cert.X509Certificate
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.readByteString
import picocli.CommandLine
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier

/**
 * Command-line utility for [Certificate] messages.
 *
 * Use the `help` subcommand for usage help.
 */
@CommandLine.Command(
  description = ["Utility for Certificate messages."],
  subcommands = [CommandLine.HelpCommand::class, Serialize::class, Deserialize::class, Sign::class]
)
class Certificates private constructor() : Runnable {
  override fun run() {
    // No-op. See subcommands.
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(Certificates(), args)
  }
}

@CommandLine.Command(name = "serialize", showDefaultValues = true)
private class Create : Runnable {

  @CommandLine.Option(
    names = ["--x509-der"],
    description = ["File containing format-specific key data"],
    required = true
  )
  private lateinit var x509DerFile: File

  @CommandLine.Option(
    names = ["--parent-resource"],
    description = ["API resource name of the parent resource"],
    required = true
  )
  lateinit var parentResource: String
  override fun run() {
    val derBytes = x509DerFile.readByteString()
    val certificate = certificate {
      subjectKeyIdentifier = this@Serialize.format
      x509Der = derBytes
    }




    // Validate that input is an EncryptionPublicKey message.
    EncryptionPublicKey.parseFrom(serialized)

    val certificate: X509Certificate = certificateFile.inputStream().use { readCertificate(it) }
    val privateKey: PrivateKey =
      readPrivateKey(signingKeyFile.readByteString(), certificate.publicKey.algorithm)
    val keyHandle = SigningKeyHandle(certificate, privateKey)
    val algorithm = algorithm ?: keyHandle.defaultAlgorithm
    val signature: ByteString = keyHandle.sign(algorithm, serialized)

    println("Signature algorithm: $algorithm (${algorithm.oid})")

    outFile.outputStream().use { signature.writeTo(it) }
  }
}
