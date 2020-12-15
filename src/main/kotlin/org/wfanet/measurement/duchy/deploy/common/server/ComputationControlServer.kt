// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.duchy.deploy.common.server

import io.grpc.ManagedChannel
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.identity.DuchyIdFlags
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.common.identity.withDuchyIdentities
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.service.system.v1alpha.ComputationControlService
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine

abstract class ComputationControlServer : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  @CommandLine.Mixin
  protected lateinit var duchyIdFlags: DuchyIdFlags
    private set

  protected fun run(storageClient: StorageClient) {
    val duchyName = flags.duchy.duchyName
    val latestDuchyPublicKeys = DuchyPublicKeys.fromFlags(flags.duchyPublicKeys).latest
    require(latestDuchyPublicKeys.containsKey(duchyName)) {
      "Public key not specified for Duchy $duchyName"
    }
    DuchyIds.setDuchyIdsFromFlags(duchyIdFlags)
    require(latestDuchyPublicKeys.keys.toSet() == DuchyIds.ALL)

    val channel: ManagedChannel = buildChannel(flags.asyncComputationControlServiceTarget)

    CommonServer.fromFlags(
      flags.server,
      javaClass.name,
      ComputationControlService(
        AsyncComputationControlCoroutineStub(channel),
        storageClient
      ).withDuchyIdentities()
    ).start().blockUntilShutdown()
  }

  protected class Flags {
    @CommandLine.Mixin
    lateinit var server: CommonServer.Flags
      private set

    @CommandLine.Mixin
    lateinit var duchy: CommonDuchyFlags
      private set

    @CommandLine.Mixin
    lateinit var duchyPublicKeys: DuchyPublicKeys.Flags
      private set

    @CommandLine.Option(
      names = ["--async-computation-control-service-target"],
      description = ["Address and port of the internal Aysnc Computation Control service"],
      required = true
    )
    lateinit var asyncComputationControlServiceTarget: String
      private set
  }

  companion object {
    const val SERVICE_NAME = "ComputationControl"
  }
}
