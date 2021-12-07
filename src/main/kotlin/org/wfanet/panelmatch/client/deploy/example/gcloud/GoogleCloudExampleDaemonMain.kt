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

package org.wfanet.panelmatch.client.deploy.example.gcloud

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.deploy.example.ExampleDaemon
import org.wfanet.panelmatch.common.certificates.CertificateAuthority
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin

@Command(
  name = "GoogleCloudExampleDaemon",
  description = ["Example daemon to execute ExchangeWorkflows on Google Cloud"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private class GoogleCloudExampleDaemon : ExampleDaemon() {
  @Mixin private lateinit var gcsFlags: GcsFromFlags.Flags

  override val rootStorageClient: StorageClient by lazy {
    GcsStorageClient.fromFlags(GcsFromFlags(gcsFlags))
  }

  override val certificateAuthority: CertificateAuthority
    get() = TODO("Not yet implemented")
}

/** Reference Google Cloud implementation of a daemon for executing Exchange Workflows. */
fun main(args: Array<String>) = commandLineMain(GoogleCloudExampleDaemon(), args)
