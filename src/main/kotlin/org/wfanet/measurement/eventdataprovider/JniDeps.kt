// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider

import java.nio.file.Paths
import org.wfanet.measurement.common.loadLibrary
import java.io.InputStream
import java.nio.file.Files

fun loadLibraryFromResource(libraryName: String, resourcePathPrefix: String) {
  val fullLibraryName = System.mapLibraryName(libraryName)
  val libraryPath = "$resourcePathPrefix/$fullLibraryName"

  val tmpDir =
    Paths.get(
      System.getProperty("java.io.tmpdir"),
      "org-wfanet-panelmatch-common-beam-LoadLibrary",
      System.currentTimeMillis().toString()
    )
      .toAbsolutePath()
  check(tmpDir.toFile().mkdirs())
  tmpDir.toFile().deleteOnExit()
  val outputPath = tmpDir.resolve(fullLibraryName)

  val stream: InputStream =
    checkNotNull(getResourceAsStream(libraryPath)) {
      "Could not load resource with path $libraryPath"
    }

  stream.use { Files.copy(stream, outputPath) }
  outputPath.toFile().deleteOnExit()
  System.load(outputPath.toString())
}

private fun getResourceAsStream(path: String): InputStream? {
  return object {}::class.java.getResourceAsStream(path)
}

class JniDeps {

  companion object {
    init {
      loadLibraryFromResource(
        libraryName = "sketch_encrypter_adapter",
        resourcePathPrefix =
        "/any_sketch_java/src/main/java/org/wfanet/anysketch/crypto"

      )
      /*loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath =
          Paths.get(
            "any_sketch_java",
            "src",
            "main",
            "java",
            "org",
            "wfanet",
            "anysketch",
            "crypto"
          )
      )*/
    }
  }
}
