/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.common.server

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import io.grpc.BindableService
import java.io.File
import kotlin.properties.Delegates
import kotlin.reflect.full.declaredMemberProperties
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.postgres.PostgresFlags
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig
import org.wfanet.measurement.gcloud.spanner.SpannerDatabaseConnector
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.reporting.deploy.v2.common.SpannerFlags
import org.wfanet.measurement.reporting.deploy.v2.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import picocli.CommandLine

abstract class AbstractInternalReportingServer : Runnable {
  @CommandLine.Spec lateinit var spec: CommandLine.Model.CommandSpec

  @CommandLine.Mixin private lateinit var serverFlags: CommonServer.Flags
  @CommandLine.Mixin
  protected lateinit var spannerFlags: SpannerFlags
    private set

  @set:CommandLine.Option(
    names = ["--basic-reports-enabled"],
    description =
      [
        "Whether the BasicReports service is enabled. This includes services it exclusively depends on."
      ],
    required = false,
    defaultValue = "false",
  )
  protected var basicReportsEnabled by Delegates.notNull<Boolean>()
    private set

  @CommandLine.Option(
    names = ["--impression-qualification-filter-config-file"],
    description =
      [
        "Path to file containing a ImpressionQualificationsFilterConfig protobuf message in  " +
          "text format.",
        "Required if --basic-reports-enabled is true.",
      ],
    required = false,
  )
  protected lateinit var impressionQualificationFilterConfigFile: File
    private set

  @CommandLine.Option(
    names = ["--event-message-descriptor-set"],
    description =
      [
        "Serialized FileDescriptorSet for the event message and its dependencies. " +
          "This can be specified multiple times.",
        "Required if --basic-reports-enabled is true.",
      ],
    required = false,
  )
  private lateinit var eventMessageDescriptorSetFiles: List<File>

  @CommandLine.Option(
    names = ["--event-message-type-url"],
    description =
      [
        "Protobuf type URL of the event message for the CMMS instance",
        "Required if --basic-reports-enabled is true.",
      ],
    required = false,
  )
  private lateinit var eventMessageTypeUrl: String

  protected fun getEventMessageDescriptor(): Descriptors.Descriptor {
    val typeRegistry: TypeRegistry = buildTypeRegistry()
    return checkNotNull(typeRegistry.getDescriptorForTypeUrl(eventMessageTypeUrl)) {
      "Event message type not found in descriptor sets"
    }
  }

  private fun buildTypeRegistry(): TypeRegistry {
    val fileDescriptorSets: List<DescriptorProtos.FileDescriptorSet> =
      loadFileDescriptorSets(eventMessageDescriptorSetFiles)
    return TypeRegistry.newBuilder()
      .apply { add(ProtoReflection.buildDescriptors(fileDescriptorSets)) }
      .build()
  }

  // TODO(@tristanvuong2021): Delete flag when there is a better alternative.
  @set:CommandLine.Option(
    names = ["--disable-metrics-reuse"],
    description = ["Whether Metrics Reuse is disabled. Will be removed when testing is finished."],
    required = true,
  )
  var disableMetricsReuse by Delegates.notNull<Boolean>()
    private set

  private var validated = false

  protected fun validateCommandLine() {
    if (basicReportsEnabled) {
      if (
        spannerFlags.projectName.isEmpty() ||
          spannerFlags.instanceName.isEmpty() ||
          spannerFlags.databaseName.isEmpty()
      ) {
        throw CommandLine.MissingParameterException(
          spec.commandLine(),
          spec.args(),
          "--spanner-project, --spanner-instance, and --spanner-database are all required if " +
            "--basic-reports-enabled is true",
        )
      }

      if (!::impressionQualificationFilterConfigFile.isInitialized) {
        throw CommandLine.MissingParameterException(
          spec.commandLine(),
          spec.args(),
          "--impression-qualification-filter-config-file is required if " +
            "--basic-reports-enabled is true",
        )
      }

      if (!::eventMessageDescriptorSetFiles.isInitialized || !::eventMessageTypeUrl.isInitialized) {
        throw CommandLine.MissingParameterException(
          spec.commandLine(),
          spec.args(),
          "--event-message-descriptor-set and --event-message-type-url are required if " +
            "--basic-reports-enabled is true",
        )
      }
    }
    validated = true
  }

  protected suspend fun run(services: Services) {
    require(validated)
    val server = CommonServer.fromFlags(serverFlags, this::class.simpleName!!, services.toList())

    runInterruptible { server.start().blockUntilShutdown() }
  }

  companion object {
    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    private fun loadFileDescriptorSets(
      files: Iterable<File>
    ): List<DescriptorProtos.FileDescriptorSet> {
      return files.map { file ->
        file.inputStream().use { input ->
          DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
        }
      }
    }

    fun Services.toList(): List<BindableService> {
      return Services::class.declaredMemberProperties.mapNotNull {
        it.get(this) as BindableService?
      }
    }
  }
}

@CommandLine.Command(
  name = "InternalReportingServer",
  description = ["Start the internal Reporting data-layer services in a single blocking server."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class InternalReportingServer : AbstractInternalReportingServer() {
  @CommandLine.Mixin private lateinit var serviceFlags: ServiceFlags
  @CommandLine.Mixin private lateinit var postgresFlags: PostgresFlags

  override fun run() = runBlocking {
    validateCommandLine()
    val idGenerator = RandomIdGenerator()
    val serviceDispatcher: CoroutineDispatcher = serviceFlags.executor.asCoroutineDispatcher()

    val postgresClient = PostgresDatabaseClient.fromFlags(postgresFlags)

    if (basicReportsEnabled) {
      val impressionQualificationFilterConfig =
        parseTextProto(
          impressionQualificationFilterConfigFile,
          ImpressionQualificationFilterConfig.getDefaultInstance(),
        )

      val eventMessageDescriptor = getEventMessageDescriptor()

      val impressionQualificationFilterMapping =
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          eventMessageDescriptor,
        )

      spannerFlags.usingSpanner { spanner: SpannerDatabaseConnector ->
        val spannerClient = spanner.databaseClient
        run(
          DataServices.create(
            idGenerator,
            postgresClient,
            spannerClient,
            impressionQualificationFilterMapping,
            eventMessageDescriptor,
            disableMetricsReuse,
            serviceDispatcher,
          )
        )
      }
    } else {
      run(
        DataServices.create(
          idGenerator,
          postgresClient,
          null,
          null,
          null,
          disableMetricsReuse,
          serviceDispatcher,
        )
      )
    }
  }
}

fun main(args: Array<String>) = commandLineMain(InternalReportingServer(), args)
