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

package org.wfanet.measurement.loadtest.dataprovider

import java.io.File
import java.nio.file.Files
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import picocli.CommandLine

@CommandLine.Command(
  name = "FileSystemEdpSimulatorRunner",
  description = ["${EdpSimulator.DAEMON_NAME} Daemon"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
/** Implementation of [EdpSimulator] using the File System to store blobs. */
class FileSystemEdpSimulatorRunner : EdpSimulator() {
  @CommandLine.Option(
    names = ["--output-directory"],
    description = ["File path of output directory where files will be written."],
    required = true
  )
  private lateinit var outputDir: File

  override fun run() {
    run(FileSystemStorageClient(makeFile(outputDir)))
  }

  private fun makeFile(directory: File): File {
    val path = directory.toPath()
    return Files.createDirectories(path).toFile()
  }
}

fun main(args: Array<String>) = commandLineMain(FileSystemEdpSimulatorRunner(), args)
