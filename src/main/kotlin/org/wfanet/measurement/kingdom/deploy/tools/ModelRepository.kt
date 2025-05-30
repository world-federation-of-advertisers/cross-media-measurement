/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.tools

import io.grpc.ManagedChannel
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ListModelProvidersResponse
import org.wfanet.measurement.api.v2alpha.ListModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.ModelProvidersGrpcKt.ModelProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineStub
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.getModelProviderRequest
import org.wfanet.measurement.api.v2alpha.getModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.listModelProvidersRequest
import org.wfanet.measurement.api.v2alpha.listModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import picocli.CommandLine.ParentCommand

private val CHANNEL_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30)

@Command(
  name = "model-repository",
  description = ["Manages all Model Repository artifacts"],
  subcommands = [CommandLine.HelpCommand::class, ModelProviders::class, ModelSuites::class],
)
class ModelRepository private constructor() : Runnable {
  @Mixin private lateinit var tlsFlags: TlsFlags

  @Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  private lateinit var target: String
    private set

  @Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target.",
      ],
    required = false,
  )
  private var certHost: String? = null

  val channel: ManagedChannel by lazy {
    buildMutualTlsChannel(target, tlsFlags.signingCerts, certHost)
      .withShutdownTimeout(CHANNEL_SHUTDOWN_TIMEOUT)
  }

  override fun run() {
    // No-op. See subcommands.
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(ModelRepository(), args)
  }
}

@Command(
  name = "model-providers",
  subcommands = [CommandLine.HelpCommand::class, GetModelProvider::class, ListModelProviders::class],
)
private class ModelProviders {
  @ParentCommand private lateinit var parentCommand: ModelRepository

  val modelProvidersClient: ModelProvidersCoroutineStub by lazy {
    ModelProvidersCoroutineStub(parentCommand.channel)
  }
}

@Command(name = "get", description = ["Get a ModelProvider"])
class GetModelProvider : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelProviders

  @Parameters(index = "0", description = ["API resource name of the ModelProvider"], arity = "1")
  private lateinit var modelProviderName: String

  override fun run() {
    val modelProvider = runBlocking {
      parentCommand.modelProvidersClient.getModelProvider(
        getModelProviderRequest { name = modelProviderName }
      )
    }
    println(modelProvider)
  }
}

@Command(name = "list", description = ["List ModelProviders"])
class ListModelProviders : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelProviders

  @Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val response: ListModelProvidersResponse = runBlocking {
      parentCommand.modelProvidersClient.listModelProviders(
        listModelProvidersRequest {
          pageSize = pageParams.pageSize
          pageToken = pageParams.pageToken
        }
      )
    }

    println(response)
  }
}

@Command(
  name = "model-suites",
  subcommands =
    [
      CommandLine.HelpCommand::class,
      GetModelSuite::class,
      CreateModelSuite::class,
      ListModelSuites::class,
    ],
)
private class ModelSuites {
  @ParentCommand private lateinit var parentCommand: ModelRepository

  val modelSuitesClient: ModelSuitesCoroutineStub by lazy {
    ModelSuitesCoroutineStub(parentCommand.channel)
  }
}

@Command(name = "get", description = ["Get a ModelSuite"])
class GetModelSuite : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelSuites

  @Parameters(index = "0", description = ["API resource name of the ModelSuite"], arity = "1")
  private lateinit var modelSuiteName: String

  override fun run() {
    val modelSuite = runBlocking {
      parentCommand.modelSuitesClient.getModelSuite(getModelSuiteRequest { name = modelSuiteName })
    }
    println(modelSuite)
  }
}

@Command(name = "create", description = ["Create a ModelSuite"])
class CreateModelSuite : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelSuites

  @Option(
    names = ["--parent"],
    description = ["Resource name of the parent ModelProvider"],
    required = true,
  )
  private lateinit var parentModelProvider: String

  @Option(
    names = ["--display-name"],
    description = ["Human-readable nickname for the ModelSuite"],
    required = true,
  )
  private lateinit var modelSuiteDisplayName: String

  @Option(
    names = ["--description"],
    description = ["Human-readable description of usage of the ModelSuite"],
    required = false,
  )
  private lateinit var modelSuiteDescription: String

  override fun run() {
    val modelSuite = runBlocking {
      parentCommand.modelSuitesClient.createModelSuite(
        createModelSuiteRequest {
          parent = parentModelProvider
          modelSuite = modelSuite {
            displayName = modelSuiteDisplayName
            description = modelSuiteDescription
          }
        }
      )
    }

    println(modelSuite)
  }
}

@Command(name = "list", description = ["List ModelSuites"])
class ListModelSuites : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelSuites

  @Option(
    names = ["--parent"],
    description = ["Resource name of the parent ModelProvider"],
    required = true,
  )
  private lateinit var parentModelProvider: String

  @Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val response: ListModelSuitesResponse = runBlocking {
      parentCommand.modelSuitesClient.listModelSuites(
        listModelSuitesRequest {
          parent = parentModelProvider
          pageSize = pageParams.pageSize
          pageToken = pageParams.pageToken
        }
      )
    }

    println(response)
  }
}

private class PageParams {
  @Option(
    names = ["--page-size"],
    description = ["The maximum number of resources to return. The maximum value is 1000"],
    required = false,
  )
  var pageSize: Int = 1000
    private set

  @Option(
    names = ["--page-token"],
    description = ["Page token from a previous list call to retrieve the next page"],
    defaultValue = "",
    required = false,
  )
  lateinit var pageToken: String
    private set
}
