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

import java.io.File
import java.nio.file.Files
import java.util.concurrent.Callable
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.panelmatch.client.storage.storageDetails
import picocli.CommandLine

@CommandLine.Command(name = "add_private_storage_info", description = ["Adds Private Storage Info"])
class AddPrivateStorageInfo : Callable<Int> {

  @CommandLine.Mixin private lateinit var flags: CustomStorageFlags

  @CommandLine.Option(
    names = ["--recurring-exchange-id"],
    description = ["API resource name of the recurring-exchange-id"],
    required = true,
  )
  private lateinit var recurringExchangeId: String

  @CommandLine.Option(
    names = ["--private-storage-info-file"],
    description = ["Private Storage textproto file"],
    required = true,
  )
  private lateinit var privateStorageInfo: File

  override fun call(): Int {
    val storageDetails =
      checkNotNull(Files.newInputStream(privateStorageInfo.toPath())).use { input ->
        parseTextProto(input.bufferedReader(), storageDetails {})
      }
    runBlocking { flags.addResource.addPrivateStorageInfo(recurringExchangeId, storageDetails) }
    return 0
  }
}
