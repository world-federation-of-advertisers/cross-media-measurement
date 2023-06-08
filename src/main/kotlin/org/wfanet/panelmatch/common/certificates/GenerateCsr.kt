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

import java.io.ByteArrayOutputStream
import java.security.PrivateKey
import org.wfanet.measurement.common.crypto.PemWriter

/**
 * Generates a PEM format CSR through OpenSSL for a given private key, organization and common name.
 */
fun generateCsrFromPrivateKey(key: PrivateKey, organization: String, commonName: String): String {
  val outputStream = ByteArrayOutputStream()
  PemWriter(outputStream).write(key)
  return runProcessWithInputAndReturnOutput(
    outputStream.toByteArray(),
    "openssl",
    "req",
    "-new",
    "-subj",
    "/O=$organization/CN=$commonName",
    "-key",
    "/dev/stdin"
  )
}

private fun runProcessWithInputAndReturnOutput(input: ByteArray, vararg args: String): String {
  val process = ProcessBuilder(*args).redirectErrorStream(true).start()
  process.outputStream.write(input)
  process.outputStream.close()
  val exitCode = process.waitFor()
  val output = process.inputStream.use { it.bufferedReader().readText() }
  check(exitCode == 0) {
    "Command ${args.joinToString(" ")} failed with code $exitCode. Output:\n$output"
  }
  return output
}
