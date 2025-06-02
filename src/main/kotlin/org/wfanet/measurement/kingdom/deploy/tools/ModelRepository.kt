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
import org.wfanet.measurement.api.v2alpha.ListModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationKt.populationBlob
import org.wfanet.measurement.api.v2alpha.PopulationsGrpcKt.PopulationsCoroutineStub
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.eventTemplate
import org.wfanet.measurement.api.v2alpha.getModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.getPopulationRequest
import org.wfanet.measurement.api.v2alpha.listModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.listPopulationsRequest
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.api.v2alpha.population
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
  subcommands = [CommandLine.HelpCommand::class, ModelSuites::class, Populations::class],
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

@Command(
  name = "populations",
  subcommands =
    [
      CommandLine.HelpCommand::class,
      CreatePopulation::class,
      GetPopulation::class,
      ListPopulations::class,
    ],
)
private class Populations {
  @ParentCommand private lateinit var parentCommand: ModelRepository

  val populationsClient: PopulationsCoroutineStub by lazy {
    PopulationsCoroutineStub(parentCommand.channel)
  }
}

@Command(name = "create", description = ["Create a Population"])
class CreatePopulation : Runnable {
  @ParentCommand private lateinit var parentCommand: Populations

  @Option(
    names = ["--parent"],
    description = ["Resource name of the parent DataProvider"],
    required = true,
  )
  private lateinit var parentDataProvider: String

  @Option(
    names = ["--description"],
    description = ["Human-readable description of usage of the Population"],
    required = false,
  )
  private lateinit var populationDescription: String

  @Option(names = ["--model-blob-uri"], description = ["URI of the model blob"], required = true)
  private lateinit var modelBlobUriValue: String

  @Option(
    names = ["--event-template-type"],
    description = ["Type of the EventTemplate"],
    required = true,
  )
  private lateinit var eventTemplateType: String

  override fun run() {
    val population = runBlocking {
      parentCommand.populationsClient.createPopulation(
        createPopulationRequest {
          parent = parentDataProvider
          population = population {
            description = populationDescription
            populationBlob = populationBlob { modelBlobUri = modelBlobUriValue }
            eventTemplate = eventTemplate { type = eventTemplateType }
          }
        }
      )
    }

    println(population)
  }
}

@Command(name = "get", description = ["Get a Population"])
class GetPopulation : Runnable {
  @ParentCommand private lateinit var parentCommand: Populations

  @Parameters(index = "0", description = ["API resource name of the Population"], arity = "1")
  private lateinit var populationName: String

  override fun run() {
    val population = runBlocking {
      parentCommand.populationsClient.getPopulation(getPopulationRequest { name = populationName })
    }
    println(population)
  }
}

@Command(name = "list", description = ["List Populations"])
class ListPopulations : Runnable {
  @ParentCommand private lateinit var parentCommand: Populations

  @Option(
    names = ["--parent"],
    description = ["Resource name of the parent DataProvider"],
    required = true,
  )
  private lateinit var parentDataProvider: String

  @Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val response = runBlocking {
      parentCommand.populationsClient.listPopulations(
        listPopulationsRequest {
          parent = parentDataProvider
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
