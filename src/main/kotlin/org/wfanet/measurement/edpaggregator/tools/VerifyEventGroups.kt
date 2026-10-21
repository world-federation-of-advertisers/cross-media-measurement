// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.tools

import java.io.File
import java.util.concurrent.Callable
import kotlin.system.exitProcess
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupReader
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupValidator
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option

/** Command-line validator for EventGroup files before they are uploaded to EDPA. */
@Command(
  name = "verify-event-groups",
  description = ["Verifies EventGroup export files before they are uploaded to EDPA."],
  mixinStandardHelpOptions = true,
)
class VerifyEventGroups : Callable<Int> {
  @Option(
    names = ["--event-groups-file"],
    description =
      [
        "Local EventGroup export file to verify. Must be JSON (.json) or Mesos RecordIO " +
          "binary protobuf (.binpb). May be specified more than once."
      ],
    required = true,
  )
  private var eventGroupFiles: List<File> = emptyList()

  override fun call(): Int = runBlocking {
    var invalidFileCount = 0
    for (file in eventGroupFiles) {
      if (!verify(file)) {
        invalidFileCount++
      }
    }
    if (invalidFileCount == 0) 0 else 1
  }

  private suspend fun verify(file: File): Boolean {
    var eventGroupCount = 0
    var invalidEventGroupCount = 0
    try {
      check(file.isFile) { "File does not exist or is not a regular file" }
      val absoluteFile = file.absoluteFile
      val eventGroups =
        EventGroupReader(FileSystemStorageClient(checkNotNull(absoluteFile.parentFile)))
          .read(absoluteFile.name)
      eventGroups.collect { eventGroup ->
        eventGroupCount++
        try {
          EventGroupValidator.validate(eventGroup)
        } catch (e: IllegalStateException) {
          invalidEventGroupCount++
          System.err.println(
            "FAIL: ${file.path} record $eventGroupCount (${eventGroup.identifier()}): " + e.message
          )
        }
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      System.err.println("FAIL: ${file.path}: ${e.message ?: e::class.java.simpleName}")
      return false
    }

    if (invalidEventGroupCount > 0) {
      System.err.println(
        "FAIL: ${file.path}: $invalidEventGroupCount of $eventGroupCount EventGroups are invalid"
      )
      return false
    }
    println("PASS: ${file.path}: verified $eventGroupCount EventGroups")
    return true
  }

  private fun EventGroup.identifier(): String {
    if (hasEntityKey() && entityKey.entityId.isNotBlank()) {
      return "${entityKey.entityType}/${entityKey.entityId}"
    }
    return eventGroupReferenceId.ifBlank { "no identifier" }
  }
}

fun main(args: Array<String>) {
  val exitCode = CommandLine(VerifyEventGroups()).execute(*args)
  if (exitCode != 0) {
    exitProcess(exitCode)
  }
}
