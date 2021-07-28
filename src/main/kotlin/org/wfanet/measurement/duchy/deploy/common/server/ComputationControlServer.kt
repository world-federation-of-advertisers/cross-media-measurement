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

package org.wfanet.measurement.duchy.deploy.common.server

import io.grpc.ManagedChannel
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.common.identity.withDuchyIdentities
import org.wfanet.measurement.duchy.deploy.common.AsyncComputationControlServiceFlags
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
  protected lateinit var duchyInfoFlags: DuchyInfoFlags
    private set

  protected fun run(storageClient: StorageClient) {
    DuchyInfo.initializeFromFlags(duchyInfoFlags)

    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.server.tlsFlags.certFile,
        privateKeyFile = flags.server.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.server.tlsFlags.certCollectionFile
      )

    val channel: ManagedChannel =
      buildMutualTlsChannel(
        flags.asyncComputationControlServiceFlags.target,
        clientCerts,
        flags.asyncComputationControlServiceFlags.certHost
      )

    CommonServer.fromFlags(
        flags.server,
        javaClass.name,
        ComputationControlService(AsyncComputationControlCoroutineStub(channel), storageClient)
          .withDuchyIdentities()
      )
      .start()
      .blockUntilShutdown()
  }

  protected class Flags {
    @CommandLine.Mixin
    lateinit var server: CommonServer.Flags
      private set

    @CommandLine.Mixin
    lateinit var duchy: CommonDuchyFlags
      private set

    @CommandLine.Mixin
    lateinit var asyncComputationControlServiceFlags: AsyncComputationControlServiceFlags
      private set
  }

  companion object {
    const val SERVICE_NAME = "ComputationControl"
  }
}
