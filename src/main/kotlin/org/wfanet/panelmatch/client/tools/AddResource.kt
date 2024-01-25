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

package org.wfanet.panelmatch.client.tools

import java.util.concurrent.Callable
import kotlin.system.exitProcess
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.HelpCommand

@Command(
  name = "add_resource",
  description = ["Creates resources for panel client"],
  subcommands =
    [
      HelpCommand::class,
      AddWorkflow::class,
      AddRootCertificate::class,
      AddPrivateStorageInfo::class,
      AddSharedStorageInfo::class,
      ProvideWorkflowInput::class,
      AddAllResources::class,
    ],
)
class AddResource : Callable<Int> {
  /** Return 0 for success -- all work happens in subcommands. */
  override fun call(): Int = 0
}

/**
 * Creates resources for each client
 *
 * Use the `help` command to see usage details:
 * ```
 * $ bazel build //src/main/kotlin/org/wfanet/panelmatch/client/tools:AddResource
 * $ bazel-bin/src/main/kotlin/org/wfanet/panelmatch/client/tools/AddResource help
 * Usage: AddResource [COMMAND]
 * Adds resources for each client
 * Commands:
 *  help                              Displays help information about the specified command
 *  add_workflow                      Adds a workflow
 *  add_root_certificate              Adds root certificate for another party
 *  add_shared_storage_info           Add shared storage info
 *  add_private_storage_info          Add private storage info
 *  provide_workflow_input            Adds a workflow
 *  add_all_resources                 Adds all resources into GCS
 * ```
 */
fun main(args: Array<String>) {
  exitProcess(CommandLine(AddResource()).execute(*args))
}
