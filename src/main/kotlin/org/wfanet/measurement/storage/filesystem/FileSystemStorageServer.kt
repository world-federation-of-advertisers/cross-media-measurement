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

package org.wfanet.measurement.storage.filesystem

import java.io.File
import java.util.logging.Logger
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import picocli.CommandLine

private const val SERVER_NAME = "FileSystemStorageServer"
private val QUALIFIED_CLASS_NAME: String = ::main.javaClass.enclosingClass.name
private val logger = Logger.getLogger(QUALIFIED_CLASS_NAME)

private class Flags {
  @CommandLine.Option(
    names = ["--blob-storage-directory"],
    description = [
      "Directory to store blobs on the file system.",
      "If not specified, a new temporary directory will be created and used."
    ]
  )
  var directory: File? = null
    private set
}

@CommandLine.Command(
  name = SERVER_NAME,
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin serverFlags: CommonServer.Flags,
  @CommandLine.Mixin flags: Flags
) {
  val directory = flags.directory ?: createTempDir()
  logger.info("Storing blobs in $directory")

  CommonServer.fromFlags(
    serverFlags,
    SERVER_NAME,
    FileSystemStorageService(directory)
  ).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
