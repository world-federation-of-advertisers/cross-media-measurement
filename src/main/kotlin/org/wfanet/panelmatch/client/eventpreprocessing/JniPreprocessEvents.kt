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

package org.wfanet.panelmatch.client.eventpreprocessing

import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths
import org.wfanet.panelmatch.client.PreprocessEventsRequest
import org.wfanet.panelmatch.client.PreprocessEventsResponse
import org.wfanet.panelmatch.client.logger.loggerFor
import org.wfanet.panelmatch.common.wrapJniException

/** A [PreprocessEvents] implementation using the JNI [PreprocessEvents]. */
class JniPreprocessEvents : PreprocessEvents {

  override fun preprocess(request: PreprocessEventsRequest): PreprocessEventsResponse {
    return wrapJniException {
      PreprocessEventsResponse.parseFrom(
        EventPreprocessing.preprocessEventsWrapper(request.toByteArray())
      )
    }
  }

  companion object {
    private val logger by loggerFor()

    init {
      val libraryName = System.mapLibraryName("preprocess_events")
      val libraryPath = "/main/swig/wfanet/panelmatch/client/eventpreprocessing/$libraryName"

      val tmpDir =
        Paths.get(
            System.getProperty("java.io.tmpdir"),
            "panel-exchange-client",
            "JniPreprocessEvents",
            System.currentTimeMillis().toString()
          )
          .toAbsolutePath()
      check(tmpDir.toFile().mkdirs())
      tmpDir.toFile().deleteOnExit()
      val outputPath = tmpDir.resolve(libraryName)

      val stream: InputStream = this::class.java.getResourceAsStream(libraryPath)!!
      stream.use { Files.copy(stream, outputPath) }
      outputPath.toFile().deleteOnExit()
      System.load(outputPath.toString())
    }
  }
}
