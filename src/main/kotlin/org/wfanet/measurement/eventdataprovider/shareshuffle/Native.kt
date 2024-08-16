/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.shareshuffle

import java.nio.file.Path
import org.wfanet.measurement.common.NativeLibraryLoader
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.common.load

object Native : NativeLibraryLoader {
  private const val NATIVE_LIB_RESOURCE_NAME =
    "org/wfanet/frequencycount/libsecret_share_generator_adapter.so"

  override fun loadLibrary() {
    val resourcePath: Path =
      this::class.java.classLoader.getJarResourcePath(NATIVE_LIB_RESOURCE_NAME)
        ?: error("$NATIVE_LIB_RESOURCE_NAME not found in JAR")
    Runtime.getRuntime().load(resourcePath)
  }
}
