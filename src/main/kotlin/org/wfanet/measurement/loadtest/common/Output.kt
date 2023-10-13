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

package org.wfanet.measurement.loadtest.common

import java.io.Closeable
import java.io.File

/**
 * Interface for handling output of file data.
 *
 * This presents the same interface for either outputting to a [File] or to the console (STDOUT).
 */
sealed interface Output {
  fun resolve(relative: String): Output

  fun writer(): Writable
}

interface Writable : Appendable, Closeable

/** [Output] where the destination is a [file]. */
class FileOutput(private val file: File) : Output {
  override fun resolve(relative: String): Output {
    return FileOutput(file.resolve(relative))
  }

  override fun writer(): Writable {
    val writer = file.bufferedWriter()
    return object : Writable, Appendable by writer, Closeable by writer {}
  }
}

/** [Output] where the destination is the console (STDOUT). */
object ConsoleOutput : Output {
  override fun resolve(relative: String): Output {
    println("${relative}:")
    return this
  }

  override fun writer(): Writable {
    return object : Writable, Appendable by System.out {
      override fun close() {
        println()
      }
    }
  }
}
