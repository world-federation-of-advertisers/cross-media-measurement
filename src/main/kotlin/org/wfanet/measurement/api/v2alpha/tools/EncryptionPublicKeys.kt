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
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.readByteString
import picocli.CommandLine

/**
 * Command-line utility for [EncryptionPublicKey] messages.
 *
 * Use the `help` subcommand for usage help.
 */
@CommandLine.Command(
  description = ["Utility for EncryptionPublicKey messages."],
  subcommands = [CommandLine.HelpCommand::class, Serialize::class, Deserialize::class, Sign::class],
)
class EncryptionPublicKeys private constructor() : Runnable {
  override fun run() {
    // No-op. See subcommands.
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(EncryptionPublicKeys(), args)
  }
}

@CommandLine.Command(name = "serialize", showDefaultValues = true)
private class Serialize : Runnable {
  @CommandLine.Option(
    names = ["--format"],
    description = ["EncryptionPublicKey format"],
    defaultValue = "TINK_KEYSET",
  )
  private lateinit var format: EncryptionPublicKey.Format

  @CommandLine.Option(
    names = ["--data"],
    description = ["File containing format-specific key data"],
    required = true,
  )
  private lateinit var data: File

  @CommandLine.Option(names = ["--out", "-o"], description = ["Output file"], required = true)
  private lateinit var out: File

  override fun run() {
    val encryptionPublicKey = encryptionPublicKey {
      format = this@Serialize.format
      data = this@Serialize.data.readByteString()
    }

    out.outputStream().use { encryptionPublicKey.writeTo(it) }
  }
}

@CommandLine.Command(name = "deserialize", showDefaultValues = true)
private class Deserialize : Runnable {
  @CommandLine.Option(
    names = ["--in", "-i"],
    description = ["Input File containing EncryptionPublicKey message"],
    required = true,
  )
  private lateinit var input: File

  @CommandLine.Option(names = ["--out", "-o"], description = ["Output file"], required = true)
  private lateinit var output: File

  override fun run() {
    val message = input.inputStream().use { EncryptionPublicKey.parseFrom(it) }

    output.outputStream().use { message.data.writeTo(it) }
  }
}

@CommandLine.Command(
  name = "sign",
  description = ["Signs the input message, outputting a digital signature"],
)
private class Sign : Runnable {
  @CommandLine.Option(
    names = ["--certificate"],
    description = ["X.509 certificate in DER or PEM format"],
    required = true,
  )
  private lateinit var certificateFile: File

  @CommandLine.Option(
    names = ["--signing-key"],
    description = ["Signing key in DER format"],
    required = true,
  )
  private lateinit var signingKeyFile: File

  @CommandLine.Option(
    names = ["--signature-algorithm"],
    description = ["Signature algorithm. One will be chosen if not specified."],
    required = false,
  )
  private var algorithm: SignatureAlgorithm? = null

  @CommandLine.Option(
    names = ["--in", "-i"],
    description = ["Input serialized EncryptionPublicKey message"],
    required = true,
  )
  private lateinit var inFile: File

  @CommandLine.Option(names = ["--out", "-o"], description = ["Output file"], required = true)
  private lateinit var outFile: File

  override fun run() {
    val serialized = inFile.readByteString()

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
