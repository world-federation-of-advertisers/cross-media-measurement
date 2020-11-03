// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.tools

import java.util.logging.Logger
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/** Runs a subprocess. */
fun runSubprocess(
  command: String,
  redirectErrorStream: Boolean = true,
  exitOnFail: Boolean = true
) {
  logger.info("*** RUNNING: $command***")

  val process =
    ProcessBuilder(command.split("\\s".toRegex()))
      .redirectErrorStream(redirectErrorStream)
      .start()

  runBlocking {
    joinAll(
      launch { process.errorStream.bufferedReader().forEachLine(logger::severe) },
      launch { process.inputStream.bufferedReader().forEachLine(logger::info) }
    )
  }

  process.waitFor()

  if (exitOnFail && process.exitValue() != 0) {
    throw Exception("Command terminated with non-zero exit value: $command")
  }
}

private val logger: Logger = Logger.getLogger(::runSubprocess.javaClass.enclosingClass.name)
