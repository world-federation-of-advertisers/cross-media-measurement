/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.k8s.testing

import java.io.InputStream
import java.util.logging.Logger
import org.jetbrains.annotations.Blocking

object Processes {
  @PublishedApi internal val logger = Logger.getLogger(this::class.java.name)

  /**
   * Executes [command], waiting for it to complete.
   *
   * @throws IllegalStateException if the command fails (has a non-zero exit code)
   */
  @Blocking
  fun runCommand(vararg command: String) {
    logger.fine { "Running `${command.joinToString(" ")}`" }

    val process: Process =
      ProcessBuilder(*command)
        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .start()
    val exitCode: Int = process.waitFor()
    check(exitCode == 0) { "`${command.joinToString(" ")}` failed with exit code $exitCode" }
  }

  /**
   * Executes [command], waiting for it to complete.
   *
   * @throws IllegalStateException if the command fails (has a non-zero exit code)
   */
  @Blocking
  inline fun <T> runCommand(vararg command: String, consumeOutput: (InputStream) -> T): T {
    logger.fine { "Running `${command.joinToString(" ")}`" }

    val process: Process = ProcessBuilder(*command).start()
    val result = process.inputStream.use { consumeOutput(it) }

    val exitCode: Int = process.waitFor()
    check(exitCode == 0) {
      val errOutput = process.errorStream.bufferedReader().use { it.readText() }
      "`${command.joinToString(" ")}` failed with exit code $exitCode\n$errOutput"
    }

    return result
  }
}
