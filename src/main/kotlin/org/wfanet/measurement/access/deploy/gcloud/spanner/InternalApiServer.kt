/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.access.deploy.gcloud.spanner

import io.grpc.BindableService
import java.io.File
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMap
import org.wfanet.measurement.config.access.PermissionsConfig
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import picocli.CommandLine

private const val SERVER_NAME = "AccessInternalApiServer"

@CommandLine.Command(name = SERVER_NAME)
class InternalApiServer : Runnable {
  @CommandLine.Mixin private lateinit var serverFlags: CommonServer.Flags
  @CommandLine.Mixin private lateinit var serviceFlags: ServiceFlags
  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  @CommandLine.Option(
    names = ["--authority-key-identifier-to-principal-map-file"],
    description =
      ["Path to file containing an AuthorityKeyToPrincipalMap protobuf message in text format"],
    required = true,
  )
  private lateinit var authorityKeyToPrincipalMapFile: File

  @CommandLine.Option(
    names = ["--permissions-config"],
    description = ["Path to file containing a PermissionsConfig protobuf message in text format"],
    required = true,
  )
  private lateinit var permissionsConfigFile: File

  override fun run() {
    val permissionsConfig =
      parseTextProto(permissionsConfigFile, PermissionsConfig.getDefaultInstance())
    val permissionMapping = PermissionMapping(permissionsConfig)

    val authorityKeyToPrincipalMap =
      parseTextProto(
        authorityKeyToPrincipalMapFile,
        AuthorityKeyToPrincipalMap.getDefaultInstance(),
      )
    val tlsClientMapping = TlsClientPrincipalMapping(authorityKeyToPrincipalMap)

    runBlocking {
      spannerFlags.usingSpanner { spanner ->
        val databaseClient: AsyncDatabaseClient = spanner.databaseClient
        val services: List<BindableService> =
          InternalApiServices.build(
              databaseClient,
              permissionMapping,
              tlsClientMapping,
              serviceFlags.executor.asCoroutineDispatcher(),
            )
            .toList()
        val server = CommonServer.fromFlags(serverFlags, SERVER_NAME, services)

        server.start().blockUntilShutdown()
      }
    }
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(InternalApiServer(), args)
  }
}
