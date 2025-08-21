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

import java.io.File
import kotlinx.coroutines.asCoroutineDispatcher
import org.wfanet.measurement.api.v2alpha.AccountPrincipal
import org.wfanet.measurement.api.v2alpha.AkidPrincipalLookup
import org.wfanet.measurement.api.v2alpha.ContextKeys
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DuchyPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.common.api.grpc.AkidPrincipalServerInterceptor
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.ApiChangeMetricsInterceptor
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withInterceptors
import org.wfanet.measurement.common.identity.AuthorityKeyServerInterceptor
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.deploy.common.ComputationsServiceFlags
import org.wfanet.measurement.duchy.deploy.common.SystemApiFlags
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
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.server.tlsFlags.certFile,
        privateKeyFile = flags.server.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.server.tlsFlags.certCollectionFile,
      )
    val computationsClient =
      ComputationsCoroutineStub(
        buildMutualTlsChannel(
            flags.computationsServiceFlags.target,
            clientCerts,
            flags.computationsServiceFlags.certHost,
          )
          .withDefaultDeadline(flags.computationsServiceFlags.defaultDeadlineDuration)
      )
    val systemRequisitionsClient =
      SystemRequisitionsCoroutineStub(
          buildMutualTlsChannel(
            flags.systemApiFlags.target,
            clientCerts,
            flags.systemApiFlags.certHost,
          )
        )
        .withDuchyId(flags.duchy.duchyName)

    val akidInterceptor = AuthorityKeyServerInterceptor()
    val akidPrincipalInterceptor =
      AkidPrincipalServerInterceptor(
        ContextKeys.PRINCIPAL_CONTEXT_KEY,
        AuthorityKeyServerInterceptor.CLIENT_AUTHORITY_KEY_IDENTIFIER_CONTEXT_KEY,
        AkidPrincipalLookup(flags.authorityKeyIdentifierToPrincipalMap),
      )
    val apiChangeMetricsInterceptor = ApiChangeMetricsInterceptor { context ->
      when (val principal: MeasurementPrincipal? = ContextKeys.PRINCIPAL_CONTEXT_KEY.get(context)) {
        is DataProviderPrincipal -> principal.resourceKey.toName()
        is AccountPrincipal,
        is DuchyPrincipal,
        is MeasurementConsumerPrincipal,
        is ModelProviderPrincipal,
        null -> error("Unsupported principal type $principal")
      }
    }
    val service =
      RequisitionFulfillmentService(
          flags.duchy.duchyName,
          systemRequisitionsClient,
          computationsClient,
          RequisitionStore(storageClient),
          flags.service.executor.asCoroutineDispatcher(),
        )
        .withInterceptors(apiChangeMetricsInterceptor, akidPrincipalInterceptor, akidInterceptor)

    CommonServer.fromFlags(flags.server, javaClass.name, service).start().blockUntilShutdown()
  }

  protected class Flags {
    @CommandLine.Mixin
    lateinit var duchy: CommonDuchyFlags
      private set

    @CommandLine.Mixin
    lateinit var service: ServiceFlags
      private set

    @CommandLine.Mixin
    lateinit var server: CommonServer.Flags
      private set

    @CommandLine.Mixin
    lateinit var systemApiFlags: SystemApiFlags
      private set

    @CommandLine.Mixin
    lateinit var computationsServiceFlags: ComputationsServiceFlags
      private set

    @CommandLine.Option(
      names = ["--authority-key-identifier-to-principal-map-file"],
      description = ["AuthorityKeyToPrincipalMap proto message in text format."],
      required = true,
    )
    lateinit var authorityKeyIdentifierToPrincipalMap: File
      private set
  }

  companion object {
    const val SERVICE_NAME = "RequisitionFulfillment"
  }
}
