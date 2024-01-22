// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.aws.server

import org.wfanet.measurement.aws.s3.S3Flags
import org.wfanet.measurement.aws.s3.S3StorageClient
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.deploy.common.server.RequisitionFulfillmentServer
import picocli.CommandLine

/** Implementation of [RequisitionFulfillmentServer] using AWS S3. */
@CommandLine.Command(
  name = "S3RequisitionFulfillmentServer",
  description = ["Server daemon for ${RequisitionFulfillmentServer.SERVICE_NAME} service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class S3RequisitionFulfillmentServer : RequisitionFulfillmentServer() {
  @CommandLine.Mixin private lateinit var s3Flags: S3Flags

  override fun run() {
    val storageClient = S3StorageClient.fromFlags(s3Flags)
    run(storageClient)
  }
}

fun main(args: Array<String>) = commandLineMain(S3RequisitionFulfillmentServer(), args)
