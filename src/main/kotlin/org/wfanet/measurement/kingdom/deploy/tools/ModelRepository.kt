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

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import com.google.protobuf.util.Timestamps
import io.grpc.ManagedChannel
import java.io.File
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.io.inputStream
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequestKt
import org.wfanet.measurement.api.v2alpha.ListModelLinesResponse
import org.wfanet.measurement.api.v2alpha.ListModelProvidersResponse
import org.wfanet.measurement.api.v2alpha.ListModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpc
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpc.ModelLinesBlockingStub
import org.wfanet.measurement.api.v2alpha.ModelProvidersGrpc
import org.wfanet.measurement.api.v2alpha.ModelProvidersGrpc.ModelProvidersBlockingStub
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpc
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpc.ModelReleasesBlockingStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpc
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpc.ModelRolloutsBlockingStub
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpc
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpc.ModelSuitesBlockingStub
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.api.v2alpha.PopulationsGrpc
import org.wfanet.measurement.api.v2alpha.PopulationsGrpc.PopulationsBlockingStub
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.getModelProviderRequest
import org.wfanet.measurement.api.v2alpha.getModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.getPopulationRequest
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.listModelProvidersRequest
import org.wfanet.measurement.api.v2alpha.listModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.listPopulationsRequest
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.api.v2alpha.setModelLineActiveEndTimeRequest
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.ProtobufMessages
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
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
  subcommands =
    [
      CommandLine.HelpCommand::class,
      ModelProviders::class,
      ModelSuites::class,
      Populations::class,
      ModelLines::class,
    ],
)
class ModelRepository private constructor() : Runnable {
  @Mixin private lateinit var tlsFlags: TlsFlags

  @Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server."],
    required = true,
  )
  private lateinit var target: String

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

  val modelProvidersClient: ModelProvidersBlockingStub by lazy {
    ModelProvidersGrpc.newBlockingStub(parentCommand.channel)
  }
}

@Command(
  name = "get",
  description = ["Get a ModelProvider"],
  subcommands = [CommandLine.HelpCommand::class],
)
class GetModelProvider : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelProviders

  @Parameters(index = "0", description = ["API resource name of the ModelProvider"], arity = "1")
  private lateinit var modelProviderName: String

  override fun run() {
    val modelProvider =
      parentCommand.modelProvidersClient.getModelProvider(
        getModelProviderRequest { name = modelProviderName }
      )

    println(modelProvider)
  }
}

@Command(
  name = "list",
  description = ["List ModelProviders"],
  subcommands = [CommandLine.HelpCommand::class],
)
class ListModelProviders : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelProviders

  @Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val response: ListModelProvidersResponse =
      parentCommand.modelProvidersClient.listModelProviders(
        listModelProvidersRequest {
          pageSize = pageParams.pageSize
          pageToken = pageParams.pageToken
        }
      )

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

  val modelSuitesClient: ModelSuitesBlockingStub by lazy {
    ModelSuitesGrpc.newBlockingStub(parentCommand.channel)
  }
}

@Command(
  name = "get",
  description = ["Get a ModelSuite"],
  subcommands = [CommandLine.HelpCommand::class],
)
class GetModelSuite : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelSuites

  @Parameters(index = "0", description = ["API resource name of the ModelSuite"], arity = "1")
  private lateinit var modelSuiteName: String

  override fun run() {
    val modelSuite =
      parentCommand.modelSuitesClient.getModelSuite(getModelSuiteRequest { name = modelSuiteName })

    println(modelSuite)
  }
}

@Command(
  name = "create",
  description = ["Create a ModelSuite"],
  subcommands = [CommandLine.HelpCommand::class],
)
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
    val modelSuite =
      parentCommand.modelSuitesClient.createModelSuite(
        createModelSuiteRequest {
          parent = parentModelProvider
          modelSuite = modelSuite {
            displayName = modelSuiteDisplayName
            description = modelSuiteDescription
          }
        }
      )

    println(modelSuite)
  }
}

@Command(
  name = "list",
  description = ["List ModelSuites"],
  subcommands = [CommandLine.HelpCommand::class],
)
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
    val response: ListModelSuitesResponse =
      parentCommand.modelSuitesClient.listModelSuites(
        listModelSuitesRequest {
          parent = parentModelProvider
          pageSize = pageParams.pageSize
          pageToken = pageParams.pageToken
        }
      )

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

  val populationsClient: PopulationsBlockingStub by lazy {
    PopulationsGrpc.newBlockingStub(parentCommand.channel)
  }
}

@Command(
  name = "create",
  description = ["Create a Population"],
  subcommands = [CommandLine.HelpCommand::class],
)
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

  @Option(
    names = ["--population-spec"],
    description = ["Filesystem path to PopulationSpec message"],
    required = true,
  )
  private lateinit var populationSpecFile: File

  @Option(
    names = ["--event-message-descriptor-set"],
    description =
      [
        "Serialized FileDescriptorSet for the event message and its dependencies.",
        "This can be specified multiple times.",
      ],
    required = true,
  )
  private lateinit var eventMessageDescriptorSetFiles: List<File>

  @Option(
    names = ["--event-message-type-url"],
    description = ["Protobuf type URL of the event message for the CMMS instance"],
    required = true,
  )
  private lateinit var eventMessageTypeUrl: String

  override fun run() {
    val populationSpec =
      ProtobufMessages.parseMessage(populationSpecFile, PopulationSpec.getDefaultInstance())
    val typeRegistry: TypeRegistry = buildTypeRegistry()
    val eventMessageDescriptor =
      requireNotNull(typeRegistry.getDescriptorForTypeUrl(eventMessageTypeUrl)) {
        "Event message descriptor not found"
      }
    PopulationSpecValidator.validate(populationSpec, eventMessageDescriptor)

    val population =
      parentCommand.populationsClient.createPopulation(
        createPopulationRequest {
          parent = parentDataProvider
          population = population {
            description = populationDescription
            this.populationSpec = populationSpec
          }
        }
      )

    println(population)
  }

  private fun buildTypeRegistry(): TypeRegistry {
    val fileDescriptorSets: List<DescriptorProtos.FileDescriptorSet> =
      eventMessageDescriptorSetFiles.map {
        it.inputStream().use { input ->
          DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
        }
      }
    return TypeRegistry.newBuilder()
      .add(ProtoReflection.buildDescriptors(fileDescriptorSets))
      .build()
  }

  companion object {
    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable
  }
}

@Command(
  name = "get",
  description = ["Get a Population"],
  subcommands = [CommandLine.HelpCommand::class],
)
class GetPopulation : Runnable {
  @ParentCommand private lateinit var parentCommand: Populations

  @Parameters(index = "0", description = ["API resource name of the Population"], arity = "1")
  private lateinit var populationName: String

  override fun run() {
    val population =
      parentCommand.populationsClient.getPopulation(getPopulationRequest { name = populationName })

    println(population)
  }
}

@Command(
  name = "list",
  description = ["List Populations"],
  subcommands = [CommandLine.HelpCommand::class],
)
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
    val response =
      parentCommand.populationsClient.listPopulations(
        listPopulationsRequest {
          parent = parentDataProvider
          pageSize = pageParams.pageSize
          pageToken = pageParams.pageToken
        }
      )

    println(response)
  }
}

@Command(
  name = "model-lines",
  subcommands =
    [
      CommandLine.HelpCommand::class,
      CreateModelLine::class,
      GetModelLine::class,
      ListModelLines::class,
      SetModelLineActiveEndTime::class,
    ],
)
private class ModelLines {
  @ParentCommand private lateinit var parentCommand: ModelRepository

  val modelLinesClient: ModelLinesBlockingStub by lazy {
    ModelLinesGrpc.newBlockingStub(parentCommand.channel)
  }

  val modelReleasesClient: ModelReleasesBlockingStub by lazy {
    ModelReleasesGrpc.newBlockingStub(parentCommand.channel)
  }

  val modelRolloutsClient: ModelRolloutsBlockingStub by lazy {
    ModelRolloutsGrpc.newBlockingStub(parentCommand.channel)
  }
}

@Command(
  name = "create",
  description = ["Create a ModelLine, a ModelRelease, and a ModelRollout)"],
  subcommands = [CommandLine.HelpCommand::class],
)
class CreateModelLine : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelLines

  @Option(
    names = ["--parent"],
    description = ["API resource name of the parent ModelSuite"],
    required = true,
  )
  private lateinit var parentModelSuite: String

  @Option(
    names = ["--display-name"],
    description = ["Human-readable nickname for the ModelLine"],
    required = false,
    defaultValue = "",
  )
  private lateinit var modelLineDisplayName: String

  @Option(
    names = ["--description"],
    description = ["Human-readable description of usage of the ModelLine"],
    required = false,
    defaultValue = "",
  )
  private lateinit var modelLineDescription: String

  @Option(
    names = ["--active-start-time"],
    description = ["The start of the time range when this ModelLine is active"],
    required = true,
  )
  private lateinit var startTime: Instant

  @Option(
    names = ["--active-end-time"],
    description = ["The end (exclusive) of the time range when the ModelLine is active"],
    required = false,
  )
  private var endTime: Instant? = null

  @Option(names = ["--type"], description = ["Type of the ModelLine"], required = true)
  private lateinit var modelLineType: ModelLine.Type

  @Option(
    names = ["--holdback-model-line"],
    description =
      [
        "API resource name of the holdback ModelLine that is used to generate reports " +
          "when this ModelLine presents outages. Only PROD ModelLine can have this set."
      ],
    required = false,
    defaultValue = "",
  )
  private lateinit var holdbackModelLineName: String

  @Option(
    names = ["--population"],
    description = ["API resource name of the Population referenced by ModelRelease"],
    required = true,
  )
  private lateinit var populationName: String

  override fun run() {
    val modelLine =
      parentCommand.modelLinesClient.createModelLine(
        createModelLineRequest {
          parent = parentModelSuite
          modelLine = modelLine {
            displayName = modelLineDisplayName
            description = modelLineDescription
            activeStartTime = startTime.toProtoTime()
            if (endTime != null) {
              activeEndTime = endTime!!.toProtoTime()
            }
            type = modelLineType
            holdbackModelLine = holdbackModelLineName
          }
        }
      )

    println(modelLine)

    val modelRelease =
      parentCommand.modelReleasesClient.createModelRelease(
        createModelReleaseRequest {
          parent = parentModelSuite
          modelRelease = modelRelease { population = populationName }
        }
      )

    parentCommand.modelRolloutsClient.createModelRollout(
      createModelRolloutRequest {
        parent = modelLine.name
        modelRollout = modelRollout {
          this.modelRelease = modelRelease.name
          instantRolloutDate =
            modelLine.activeStartTime.toInstant().atZone(ZoneOffset.UTC).toLocalDate().toProtoDate()
        }
      }
    )
  }
}

@Command(
  name = "get",
  description = ["Get a ModelLine"],
  subcommands = [CommandLine.HelpCommand::class],
)
class GetModelLine : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelLines

  @Parameters(index = "0", description = ["API resource name of the ModelLine"], arity = "1")
  private lateinit var modelLineName: String

  override fun run() {
    val modelLine =
      parentCommand.modelLinesClient.getModelLine(getModelLineRequest { name = modelLineName })

    println(modelLine)
  }
}

@Command(
  name = "list",
  description = ["List ModelLines"],
  subcommands = [CommandLine.HelpCommand::class],
)
class ListModelLines : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelLines

  @Option(
    names = ["--parent"],
    description = ["Resource name of the parent ModelSuite"],
    required = true,
  )
  private lateinit var parentModelSuite: String

  @Mixin private lateinit var pageParams: PageParams

  @Option(
    names = ["--type"],
    description = ["Type of the ModelLine to filter by. Can be repeated."],
    required = false,
  )
  private lateinit var typeList: List<ModelLine.Type>

  override fun run() {
    val response: ListModelLinesResponse =
      parentCommand.modelLinesClient.listModelLines(
        listModelLinesRequest {
          parent = parentModelSuite
          pageSize = pageParams.pageSize
          pageToken = pageParams.pageToken
          if (typeList.isNotEmpty()) {
            filter = ListModelLinesRequestKt.filter { typeIn += typeList }
          }
        }
      )

    println(response)
  }
}

@Command(
  name = "set-active-end-time",
  description = ["Set the active end time of a ModelLine"],
  subcommands = [CommandLine.HelpCommand::class],
)
class SetModelLineActiveEndTime : Runnable {
  @ParentCommand private lateinit var parentCommand: ModelLines

  @Option(names = ["--name"], description = ["API resource name of the ModelLine"], required = true)
  private lateinit var modelLineName: String

  @Option(
    names = ["--active-end-time"],
    description = ["Timestamp of when the ModelLine becomes inactive in RFC3339 format"],
    required = false,
  )
  private lateinit var endTime: String

  override fun run() {
    val result =
      parentCommand.modelLinesClient.setModelLineActiveEndTime(
        setModelLineActiveEndTimeRequest {
          name = modelLineName
          activeEndTime = Timestamps.parse(endTime)
        }
      )

    println(result)
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
