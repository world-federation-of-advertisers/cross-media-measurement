/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.panelmatch.integration.k8s

import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.crypto.SigningCerts

abstract class PanelMatchAbstractCorrectnessTest {

  @Test(timeout = 1 * 60 * 1000)
  fun `sample test`() = runBlocking {
    println("Here's the test!")
  }

  companion object {

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")
    val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")

    val KINGDOM_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles =
        getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("kingdom_root.pem").toFile()
      val cert = secretFiles.resolve("kingdom_tls.pem").toFile()
      val key = secretFiles.resolve("kingdom_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }


    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }

  }

}
