// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.tools

import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.protobuf.kotlin.toByteString
import java.io.File
import java.nio.file.Files
import java.time.LocalDate
import java.util.Optional
import java.util.concurrent.Callable
import kotlin.system.exitProcess
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.storage.FileSystemStorageFactory
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.client.storage.gcloud.gcs.GcsStorageFactory
import org.wfanet.panelmatch.client.storage.storageDetails
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.storage.StorageFactory
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.HelpCommand
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

// TODO: Add flags to support other storage clients
class CustomStorageFlags {
  @Option(
    names = ["--storage-type"],
    description = ["Type of destination storage: \${COMPLETION-CANDIDATES}"],
    required = true,
  )
  lateinit var storageType: PlatformCase
    private set

  @Option(
    names = ["--private-storage-root"],
    description = ["Private storage root directory"],
  )
  private lateinit var privateStorageRoot: File

  @Option(
    names = ["--tink-key-uri"],
    description = ["URI for tink"],
    required = true,
  )
  private lateinit var tinkKeyUri: String

  @CommandLine.Mixin private lateinit var gcsFlags: GcsFromFlags.Flags

  private val rootStorageClient: StorageClient by lazy {
    when (storageType) {
      PlatformCase.GCS -> GcsStorageClient.fromFlags(GcsFromFlags(gcsFlags))
      PlatformCase.FILE -> {
        require(privateStorageRoot.exists() && privateStorageRoot.isDirectory)
        FileSystemStorageClient(privateStorageRoot)
      }
      else -> throw IllegalArgumentException("Unsupported storage type")
    }
  }

  private val defaults by lazy {
    if (storageType == PlatformCase.GCS) {
      // Register GcpKmsClient before setting storage folders.
      GcpKmsClient.register(Optional.of(tinkKeyUri), Optional.empty())
    }
    DaemonStorageClientDefaults(rootStorageClient, tinkKeyUri, TinkKeyStorageProvider())
  }

  val addResource by lazy { ConfigureResource(defaults) }

  /** This should be customized per deployment. */
  val privateStorageFactories:
    Map<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory> by lazy {
    when (storageType) {
      PlatformCase.GCS -> mapOf(PlatformCase.GCS to ::GcsStorageFactory)
      PlatformCase.FILE -> mapOf(PlatformCase.FILE to ::FileSystemStorageFactory)
      else -> throw IllegalArgumentException("Unsupported storage type")
    }
  }
}

@Command(name = "add_workflow", description = ["Adds an Exchange Workflow"])
private class AddWorkflowCommand : Callable<Int> {

  @Mixin private lateinit var flags: CustomStorageFlags

  @Option(
    names = ["--recurring-exchange-id"],
    description = ["API resource name of the recurring-exchange-id"],
    required = true,
  )
  private lateinit var recurringExchangeId: String

  @Option(
    names = ["--exchange-workflow-file"],
    description = ["Public API serialized ExchangeWorkflow"],
    required = true,
  )
  private lateinit var exchangeWorkflowFile: File

  @Option(
    names = ["--start-date"],
    description = ["Date in format of YYYY-MM-DD"],
    required = true,
  )
  private lateinit var startDate: String

  override fun call(): Int {
    val serializedExchangeWorkflow = exchangeWorkflowFile.readBytes().toByteString()
    runBlocking { flags.addResource.addWorkflow(serializedExchangeWorkflow, recurringExchangeId) }
    return 0
  }
}

@Command(name = "add_root_certificate", description = ["Adds a Root Certificate for another party"])
private class AddRootCertificateCommand : Callable<Int> {

  @Mixin private lateinit var flags: CustomStorageFlags

  @Option(
    names = ["--partner-resource-name"],
    description = ["API partner resource name of the recurring exchange"],
    required = true,
  )
  private lateinit var partnerResourceName: String

  @Option(
    names = ["--certificate-file"],
    description = ["Certificate for the principal"],
    required = true
  )
  private lateinit var certificateFile: File

  private val certificate by lazy { readCertificate(certificateFile) }

  override fun call(): Int {
    runBlocking { flags.addResource.addRootCertificates(partnerResourceName, certificate) }
    return 0
  }
}

@Command(name = "add_private_storage_info", description = ["Adds Private Storage Info"])
private class AddPrivateStorageInfoCommand : Callable<Int> {

  @Mixin private lateinit var flags: CustomStorageFlags

  @Option(
    names = ["--recurring-exchange-id"],
    description = ["API resource name of the recurring-exchange-id"],
    required = true,
  )
  private lateinit var recurringExchangeId: String

  @Option(
    names = ["--private-storage-info-file"],
    description = ["Private Storage textproto file"],
    required = true,
  )
  private lateinit var privateStorageInfo: File

  override fun call(): Int {
    val storageDetails =
      checkNotNull(Files.newInputStream(privateStorageInfo.toPath())).use { input ->
        parseTextProto(input.bufferedReader(), storageDetails {})
      }
    runBlocking { flags.addResource.addPrivateStorageInfo(recurringExchangeId, storageDetails) }
    return 0
  }
}

@Command(name = "add_shared_storage_info", description = ["Adds Shared Storage Info"])
private class AddSharedStorageInfoCommand : Callable<Int> {

  @Mixin private lateinit var flags: CustomStorageFlags

  @Option(
    names = ["--recurring-exchange-id"],
    description = ["API resource name of the recurring-exchange-id"],
    required = true,
  )
  private lateinit var recurringExchangeId: String

  @Option(
    names = ["--shared-storage-info-file"],
    description = ["Shared Storage textproto file"],
    required = true,
  )
  private lateinit var sharedStorageInfo: File

  override fun call(): Int {
    val storageDetails =
      checkNotNull(Files.newInputStream(sharedStorageInfo.toPath())).use { input ->
        parseTextProto(input.bufferedReader(), storageDetails {})
      }
    runBlocking { flags.addResource.addSharedStorageInfo(recurringExchangeId, storageDetails) }
    return 0
  }
}

@Command(
  name = "provide_workflow_input",
  description = ["Will copy workflow input to private storage"]
)
private class ProvideWorkflowInputCommand : Callable<Int> {

  @Mixin private lateinit var flags: CustomStorageFlags

  @Option(
    names = ["--recurring-exchange-id"],
    description = ["API resource name of the recurring-exchange-id"],
    required = true,
  )
  private lateinit var recurringExchangeId: String

  @Option(
    names = ["--exchange-date"],
    description = ["Date in format of YYYY-MM-DD"],
    required = true,
  )
  private lateinit var exchangeDate: String

  @Option(
    names = ["--blob-key"],
    description = ["Blob Key in Private Storage"],
    required = true,
  )
  private lateinit var blobKey: String

  @Option(names = ["--blob-contents"], description = ["Blob Contents"], required = true)
  private lateinit var blobContents: File

  override fun call(): Int {
    runBlocking {
      flags.addResource.provideWorkflowInput(
        recurringExchangeId,
        LocalDate.parse(exchangeDate),
        flags.privateStorageFactories,
        blobKey,
        blobContents.readBytes().toByteString()
      )
    }
    return 0
  }
}

@Command(
  name = "add_resource",
  description = ["Creates resources for panel client"],
  subcommands =
    [
      HelpCommand::class,
      AddWorkflowCommand::class,
      AddRootCertificateCommand::class,
      AddPrivateStorageInfoCommand::class,
      AddSharedStorageInfoCommand::class,
      ProvideWorkflowInputCommand::class,
    ]
)
class AddResource : Callable<Int> {
  /** Return 0 for success -- all work happens in subcommands. */
  override fun call(): Int = 0
}

/**
 * Creates resources for each client
 *
 * Use the `help` command to see usage details:
 *
 * ```
 * $ bazel build //src/main/kotlin/org/wfanet/panelmatch/client/tools:AddResource
 * $ bazel-bin/src/main/kotlin/org/wfanet/panelmatch/client/tools/AddResource help
 * Usage: AddResource [COMMAND]
 * Adds resources for each client
 * Commands:
 *  help                              Displays help information about the specified command
 *  add_workflow                      Adds a workflow
 *  add_root_certificate              Adds root certificate for another party
 *  add_shared_storage_info           Add shared storage info
 *  add_private_storage_info          Add private storage info
 *  provide_workflow_input            Adds a workflow
 * ```
 */
fun main(args: Array<String>) {
  exitProcess(CommandLine(AddResource()).execute(*args))
}
