// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.server

import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationTypes
import org.wfanet.measurement.duchy.deploy.common.server.ComputationsServer
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.ComputationMutations
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.GcpSpannerComputationsDatabaseReader
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.GcpSpannerComputationsDatabaseTransactor
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.continuationtoken.SpannerContinuationTokensService
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.storage.forwarded.ForwardedStorageFromFlags
import picocli.CommandLine

/** Implementation of [ComputationsServer] using Google Cloud Spanner and Fake Storage Service. */
@CommandLine.Command(
  name = "ForwardedStorageSpannerComputationsServer",
  description = ["Server daemon for ${ComputationsServer.SERVICE_NAME} service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class ForwardedStorageSpannerComputationServer : ComputationsServer() {
  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags
  @CommandLine.Mixin private lateinit var forwardedStorageFlags: ForwardedStorageFromFlags.Flags

  override val protocolStageEnumHelper = ComputationProtocolStages
  override val computationProtocolStageDetails = ComputationProtocolStageDetails

  override fun run() = runBlocking {
    spannerFlags.usingSpanner { spanner ->
      val databaseClient = spanner.databaseClient
      val storageClient =
        ForwardedStorageFromFlags(forwardedStorageFlags, flags.server.tlsFlags).storageClient
      run(
        GcpSpannerComputationsDatabaseReader(databaseClient, protocolStageEnumHelper),
        GcpSpannerComputationsDatabaseTransactor(
          databaseClient = databaseClient,
          computationMutations =
            ComputationMutations(
              ComputationTypes,
              protocolStageEnumHelper,
              computationProtocolStageDetails
            )
        ),
        SpannerContinuationTokensService(databaseClient),
        storageClient
      )
    }
  }
}

fun main(args: Array<String>) = commandLineMain(ForwardedStorageSpannerComputationServer(), args)
