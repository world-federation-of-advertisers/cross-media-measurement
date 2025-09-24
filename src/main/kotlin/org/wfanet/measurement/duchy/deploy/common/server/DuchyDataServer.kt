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

package org.wfanet.measurement.duchy.deploy.common.server

import com.google.protobuf.duration
import io.grpc.ManagedChannel
import io.grpc.serviceconfig.MethodConfigKt
import io.grpc.serviceconfig.copy
import io.grpc.serviceconfig.methodConfig
import java.io.File
import java.time.Duration
import kotlinx.coroutines.runInterruptible
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.JsonServiceConfig
import org.wfanet.measurement.common.grpc.ProtobufServiceConfig
import org.wfanet.measurement.common.grpc.ServiceConfig
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.deploy.common.SystemApiFlags
import org.wfanet.measurement.duchy.deploy.common.service.DuchyDataServices
import org.wfanet.measurement.duchy.deploy.common.service.toList
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpc
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import picocli.CommandLine

abstract class DuchyDataServer : Runnable {
  @CommandLine.Mixin
  lateinit var serverFlags: CommonServer.Flags
    private set

  @CommandLine.Mixin
  lateinit var duchyFlags: CommonDuchyFlags
    private set

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."],
    required = true,
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @CommandLine.Mixin
  lateinit var systemApiFlags: SystemApiFlags
    private set

  @CommandLine.Option(
    names = ["--default-service-config"],
    description = ["Path to default gRPC ServiceConfig"],
    required = false,
  )
  private lateinit var defaultServiceConfigFile: File

  val defaultServiceConfig: ServiceConfig by lazy {
    if (this::defaultServiceConfigFile.isInitialized) {
      if (defaultServiceConfigFile.extension == "json") {
        JsonServiceConfig(defaultServiceConfigFile.readText())
      } else {
        ProtobufServiceConfig(
          parseTextProto(
            defaultServiceConfigFile,
            io.grpc.serviceconfig.ServiceConfig.getDefaultInstance(),
          )
        )
      }
    } else {
      ProtobufServiceConfig(
        ProtobufServiceConfig.DEFAULT.message.copy {
          methodConfig += methodConfig {
            name +=
              MethodConfigKt.name {
                service = ComputationLogEntriesGrpc.SERVICE_NAME
                method =
                  ComputationLogEntriesGrpc.getCreateComputationLogEntryMethod().bareMethodName!!
              }
            timeout = duration { seconds = 5 }
          }
        }
      )
    }
  }

  protected val computationLogEntriesClient by lazy {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = serverFlags.tlsFlags.certFile,
        privateKeyFile = serverFlags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = serverFlags.tlsFlags.certCollectionFile,
      )
    val channel: ManagedChannel =
      buildMutualTlsChannel(
          systemApiFlags.target,
          clientCerts,
          hostName = systemApiFlags.certHost,
          defaultServiceConfig = defaultServiceConfig,
        )
        .withShutdownTimeout(channelShutdownTimeout)
    ComputationLogEntriesCoroutineStub(channel).withDuchyId(duchyFlags.duchyName)
  }

  protected suspend fun run(services: DuchyDataServices) {
    val server = CommonServer.fromFlags(serverFlags, this::class.simpleName!!, services.toList())

    runInterruptible { server.start().blockUntilShutdown() }
  }

  companion object {
    const val SERVICE_NAME = "Computations"
  }
}
