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

package org.wfanet.panelmatch.common

import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Loads a library embedded in a jar.
 *
 * This requires that `System.getProperty("java.io.tmpdir")` is writeable.
 *
 * @param libraryName the base library name, i.e. without leading 'lib' or trailing '.so'
 * @param resourcePathPrefix the resource path to the directory containing the library
 *
 * TODO(@efoxepstein): move to common-jvm.
 */
fun loadLibraryFromResource(libraryName: String, resourcePathPrefix: String) {
  val fullLibraryName = System.mapLibraryName(libraryName)
  val libraryPath = "$resourcePathPrefix/$fullLibraryName"

  val tmpDir =
    Paths.get(
        System.getProperty("java.io.tmpdir"),
        "org-wfanet-panelmatch-common-beam-LoadLibrary",
        System.currentTimeMillis().toString(),
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
