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

import com.google.protobuf.kotlin.toByteString
import java.io.File
import java.util.concurrent.Callable
import kotlinx.coroutines.runBlocking
import picocli.CommandLine

@CommandLine.Command(name = "add_workflow", description = ["Adds an Exchange Workflow"])
class AddWorkflow : Callable<Int> {

  @CommandLine.Mixin private lateinit var flags: CustomStorageFlags

  @CommandLine.Option(
    names = ["--recurring-exchange-id"],
    description = ["API resource name of the recurring-exchange-id"],
    required = true,
  )
  private lateinit var recurringExchangeId: String

  @CommandLine.Option(
    names = ["--serialized-exchange-workflow-file"],
    description = ["Public API serialized ExchangeWorkflow"],
    required = true,
  )
  private lateinit var exchangeWorkflowFile: File

  @CommandLine.Option(
    names = ["--start-date"],
    description = ["Date in format of YYYY-MM-DD"],
    required = true,
  )
  private lateinit var startDate: String

  override fun call(): Int {
    val serializedExchangeWorkflow = exchangeWorkflowFile.readBytes().toByteString()
    runBlocking { flags.addResource.addWorkflow(serializedExchangeWorkflow, recurringExchangeId) }
    return 0
  }
}
