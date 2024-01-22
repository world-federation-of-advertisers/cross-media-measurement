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
import java.util.concurrent.Callable
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.readCertificate
import picocli.CommandLine

@CommandLine.Command(
  name = "add_root_certificate",
  description = ["Adds a Root Certificate for another party"],
)
class AddRootCertificate : Callable<Int> {

  @CommandLine.Mixin private lateinit var flags: CustomStorageFlags

  @CommandLine.Option(
    names = ["--partner-resource-name"],
    description = ["API partner resource name of the recurring exchange"],
    required = true,
  )
  private lateinit var partnerResourceName: String

  @CommandLine.Option(
    names = ["--certificate-file"],
    description = ["Certificate for the principal"],
    required = true,
  )
  private lateinit var certificateFile: File

  private val certificate by lazy { readCertificate(certificateFile) }

  override fun call(): Int {
    runBlocking { flags.addResource.addRootCertificates(partnerResourceName, certificate) }
    return 0
  }
}
