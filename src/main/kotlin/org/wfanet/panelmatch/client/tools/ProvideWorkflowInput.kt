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
import java.time.LocalDate
import java.util.concurrent.Callable
import kotlinx.coroutines.runBlocking
import picocli.CommandLine

@CommandLine.Command(
  name = "provide_workflow_input",
  description = ["Will copy workflow input to private storage"],
)
class ProvideWorkflowInput : Callable<Int> {

  @CommandLine.Mixin private lateinit var flags: CustomStorageFlags

  @CommandLine.Option(
    names = ["--recurring-exchange-id"],
    description = ["API resource name of the recurring-exchange-id"],
    required = true,
  )
  private lateinit var recurringExchangeId: String

  @CommandLine.Option(
    names = ["--exchange-date"],
    description = ["Date in format of YYYY-MM-DD"],
    required = true,
  )
  private lateinit var exchangeDate: String

  @CommandLine.Option(
    names = ["--blob-key"],
    description = ["Blob Key in Private Storage"],
    required = true,
  )
  private lateinit var blobKey: String

  @CommandLine.Option(names = ["--blob-contents"], description = ["Blob Contents"], required = true)
  private lateinit var blobContents: File

  override fun call(): Int {
    runBlocking {
      flags.addResource.provideWorkflowInput(
        recurringExchangeId,
        LocalDate.parse(exchangeDate),
        flags.privateStorageFactories,
        blobKey,
        blobContents.readBytes().toByteString(),
      )
    }
    return 0
  }
}
