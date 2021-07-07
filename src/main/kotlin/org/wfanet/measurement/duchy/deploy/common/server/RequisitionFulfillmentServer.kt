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

import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.service.api.v2alpha.RequisitionFulfillmentService
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub as SystemRequisitionsCoroutineStub
import picocli.CommandLine

abstract class RequisitionFulfillmentServer : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  protected fun run(storageClient: StorageClient) {
    val computationsClient =
      ComputationsCoroutineStub(buildChannel(flags.computationsServiceTarget))
    val systemRequisitionsClient =
      SystemRequisitionsCoroutineStub(buildChannel(flags.systemRequisitionsServiceTarget))
        .withDuchyId(flags.duchy.duchyName)

    val service =
      RequisitionFulfillmentService(
        systemRequisitionsClient,
        computationsClient,
        RequisitionStore(storageClient)
      )

    CommonServer.fromFlags(flags.server, javaClass.name, service).start().blockUntilShutdown()
  }

  protected class Flags {
    @CommandLine.Mixin
    lateinit var duchy: CommonDuchyFlags
      private set

    @CommandLine.Mixin
    lateinit var server: CommonServer.Flags
      private set

    @CommandLine.Option(
      names = ["--computations-service-target"],
      description =
        ["gRPC target (authority string or URI) for Duchy internal Computations service."],
      required = true
    )
    lateinit var computationsServiceTarget: String
      private set

    @CommandLine.Option(
      names = ["--system-requisitions-service-target"],
      description =
        ["gRPC target (authority string or URI) for Requisitions service in the system API."],
      required = true
    )
    lateinit var systemRequisitionsServiceTarget: String
      private set
  }

  companion object {
    const val SERVICE_NAME = "RequisitionFulfillment"
  }
}
