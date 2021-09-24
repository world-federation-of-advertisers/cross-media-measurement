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

package org.wfanet.panelmatch.common.beam

import com.google.common.truth.Truth.assertThat
import java.io.File
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlin.test.assertFails
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.FIXED_CA_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_FILE
import org.wfanet.measurement.common.crypto.testing.KEY_ALGORITHM
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
class SignedFilesTest : BeamTestBase() {
  @get:Rule val temporaryFolder = TemporaryFolder()

  private val inputValues = (0 until 150).map { "item-$it" }
  private val inputs = pcollectionOf("Create Inputs", inputValues.map { it.toByteString() })
  private val baseDirectory by lazy { temporaryFolder.newFolder("some-dir").absoluteFile.toPath() }
  private val fileSpec by lazy { baseDirectory.resolve("foo-*-of-00012").toString() }

  @Test
  fun roundTrip() {
    val certificate: X509Certificate = readCertificate(FIXED_SERVER_CERT_PEM_FILE)
    val privateKey: PrivateKey = readPrivateKey(FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)

    val writeResults = inputs.apply(SignedFiles.write(fileSpec, privateKey, certificate))

    assertThat(writeResults.perDestinationOutputFilenames.values()).satisfies { fileNames ->
      for (fileName in fileNames) {
        assertThat(File(fileName!!).exists())
      }
      null
    }
    pipeline.run()

    assertThat(temporaryFolder.root.walk().count { it.isFile }).isAtMost(12)

    val readResult = pipeline.apply(SignedFiles.read(fileSpec, certificate))
    assertThat(readResult.map { it.toStringUtf8() }).containsInAnyOrder(inputValues)
  }

  @Test
  fun wrongCert() {
    val certificate: X509Certificate = readCertificate(FIXED_CA_CERT_PEM_FILE)
    val privateKey: PrivateKey = readPrivateKey(FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)

    inputs.apply(SignedFiles.write(fileSpec, privateKey, certificate))
    pipeline.run()

    pipeline.apply(SignedFiles.read(fileSpec, certificate))
    assertFails { pipeline.run() }
  }
}
