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

package org.wfanet.measurement.dataprovider.deploy.fake

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.dataprovider.daemon.DataProviderServer
import picocli.CommandLine

/** Implementation of [FakeDataProviderServer] using Fake Data Provider Service. */
@CommandLine.Command(
  name = "FakeDataProviderServer",
  description = ["Server daemon for ${DataProviderServer.SERVICE_NAME} service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class FakeDataProviderServer : DataProviderServer() {
  //  @CommandLine.Mixin private lateinit var forwardedStorageFlags: ForwardedStorageFromFlags.Flags

  override fun run() {
    run()
  }
}

fun main(args: Array<String>) = commandLineMain(FakeDataProviderServer(), args)
