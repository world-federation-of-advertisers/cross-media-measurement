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

import com.google.protobuf.ByteString
import java.io.File
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.SdkHarnessOptions
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.aws.s3.S3StorageClient
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.common.TaskParameters
import org.wfanet.panelmatch.client.deploy.ProductionExchangeTaskMapper
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessingParameters
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.storage.FileSystemStorageFactory
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.client.storage.SharedStorageSelector
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.client.storage.aws.s3.S3StorageFactory
import org.wfanet.panelmatch.client.storage.gcloud.gcs.GcsStorageFactory
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.beam.BeamOptions
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.secrets.StorageClientSecretMap
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.withPrefix
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine.Option
import picocli.CommandLine.ParameterException
import picocli.CommandLine.Spec
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

private const val BUFFER_SIZE_BYTES = 32 * 1024 * 1024 // 32MiB

@Command(
  name = "parse-decrypted-event-data",
  description = ["Parses the decrypted event data produced by an exchange."],
)
class DecryptPrivateMembershipQueryResults : Runnable {

  @Spec lateinit var spec: CommandSpec

  val commandLine: CommandLine
    get() = spec.commandLine()

  @CommandLine.Mixin private lateinit var gcsFlags: GcsFromFlags.Flags

  val clock: Clock = Clock.systemUTC()

  @Option(
    names = ["--root-directory"],
    description =
      [
        "Root filesystem path to read from. If using a filesystem storage client, " +
        "will also be the root private storage directory."
      ],
    required = false,
  )
  private lateinit var rootDirectory: File

  @Option(
    names = ["--exchange-step-file"],
    description =
      [
        "The decrypt exchange step to run. Can be made manually, or serialized from " +
        "an existing workflow."
      ],
    required = true,
  )
  private lateinit var exchangeStepFile: File

  private val decryptStep by lazy { ExchangeStep.parseFrom(exchangeStepFile.readBytes()) }

  @Option(
    names = ["--exchange-step-attempt-id"],
    description =
      [
        "Resource ID for the decrypt exchange step attempt. If not tied to an " +
        "existing exchange, the only reason to set this is to keep track of your own " +
        "attempt counts."
      ],
    required = true,
    defaultValue = "A",
  )
  private lateinit var exchangeStepAttemptId: String

  private val attemptKey by lazy {
    logger.log(Level.INFO, decryptStep.name)
    requireNotNull(
      CanonicalExchangeStepAttemptKey.fromName(
        "${decryptStep.name}/attempts/$exchangeStepAttemptId"
      )
    )
  }

  @Option(
    names = ["--storage-type"],
    description = ["Type of destination storage: \${COMPLETION-CANDIDATES}"],
    required = true,
  )
  private lateinit var storageType: StorageDetails.PlatformCase

  @Option(
    names = ["--s3-region"],
    description = ["The region the s3 bucket is located in."],
    required = false,
  )
  private lateinit var s3Region: String

  @Option(
    names = ["--s3-storage-bucket"],
    description = ["The name of the s3 bucket used for default private storage."],
    required = false,
  )
  private lateinit var s3Bucket: String

  private val rootStorageClient: StorageClient by lazy {
    logger.log(Level.INFO, "$s3Region -- $s3Bucket")

    when (storageType) {
      StorageDetails.PlatformCase.AWS ->
        S3StorageClient(S3AsyncClient.builder().region(Region.of(s3Region)).build(), s3Bucket)

      StorageDetails.PlatformCase.GCS -> GcsStorageClient.fromFlags(GcsFromFlags(gcsFlags))
      StorageDetails.PlatformCase.FILE -> FileSystemStorageClient(rootDirectory)
      else ->
        throw ParameterException(
          commandLine,
          "Unsupported storage type. Must be one of: AWS, GCS, FILE"
        )

    }
  }

  private val privateStorageInfo: StorageDetailsProvider by lazy {
    val storageClient = rootStorageClient.withPrefix("private-storage-info")
    StorageDetailsProvider(StorageClientSecretMap(storageClient))
  }

  private fun makePipelineOptions(): PipelineOptions {
    return PipelineOptionsFactory.`as`(BeamOptions::class.java).apply {
      runner = DirectRunner::class.java
      defaultSdkHarnessLogLevel = SdkHarnessOptions.LogLevel.TRACE
    }
  }

  /** [PrivateStorageSelector] for writing to local (non-shared) storage. */
  private val privateStorageSelector: PrivateStorageSelector by lazy {
    PrivateStorageSelector(supportedStorageFactories, privateStorageInfo)
  }

  /** [SharedStorageSelector] for writing to shared storage. Not used, dummy declaration only. */
  private val sharedStorageSelector: SharedStorageSelector by lazy {
    SharedStorageSelector(TestCertificateManager, supportedStorageFactories, privateStorageInfo)
  }

  private val exchangeTaskMapper: ExchangeTaskMapper by lazy {
    ProductionExchangeTaskMapper(
      inputTaskThrottler = MinimumIntervalThrottler(clock, Duration.ofHours(24)), // Not used
      privateStorageSelector = privateStorageSelector,
      sharedStorageSelector = sharedStorageSelector, // Not used here.
      certificateManager = TestCertificateManager, // Not used here.
      makePipelineOptions = ::makePipelineOptions,
      taskContext =
      TaskParameters( // Not used. Set to defaults for consistency.
        setOf(PreprocessingParameters(maxByteSize = 1000000, fileCount = 1000))
      ),
    )
  }


  override fun run() = runBlocking {
    val workflow: ExchangeWorkflow =
      decryptStep.exchangeWorkflow.unpack(ExchangeWorkflow::class.java)
    val step = workflow.getSteps(decryptStep.stepIndex)

    require(
      step.stepCase == ExchangeWorkflow.Step.StepCase.DECRYPT_PRIVATE_MEMBERSHIP_QUERY_RESULTS_STEP
    ) {
      "The only step type supported is DECRYPT_PRIVATE_MEMBERSHIP_QUERY_RESULTS_STEP"
    }

    val exchangeContext =
      ExchangeContext(attemptKey, decryptStep.exchangeDate.toLocalDate(), workflow, step)

    val privateStorageClient: StorageClient =
      privateStorageSelector.getStorageClient(exchangeContext.exchangeDateKey)

    val exchangeTask: ExchangeTask = exchangeTaskMapper.getExchangeTaskForStep(exchangeContext)
    logger.log(Level.INFO, "Reading Inputs")
    val taskInput: Map<String, StorageClient.Blob> =
      if (exchangeTask.skipReadInput()) emptyMap() else readInputs(step, privateStorageClient)
    logger.log(Level.INFO, "Executing Exchange Task")
    val taskOutput: Map<String, Flow<ByteString>> = exchangeTask.execute(taskInput)
    logger.log(Level.INFO, "Writing Outputs")
    writeOutputs(step, taskOutput, privateStorageClient)
  }

  private suspend fun readInputs(
    step: ExchangeWorkflow.Step,
    privateStorage: StorageClient,
  ): Map<String, StorageClient.Blob> {
    return step.inputLabelsMap.mapValues { (label, blobKey) ->
      requireNotNull(privateStorage.getBlob(blobKey)) {
        "Missing blob key '$blobKey' for input label '$label'"
      }
    }
  }

  private suspend fun writeOutputs(
    step: ExchangeWorkflow.Step,
    taskOutput: Map<String, Flow<ByteString>>,
    privateStorage: StorageClient,
  ) {
    for ((genericLabel, flow) in taskOutput) {
      val blobKey =
        requireNotNull(step.outputLabelsMap[genericLabel]) {
          "Missing $genericLabel in outputLabels for step: $step"
        }
      privateStorage.writeBlob(blobKey, flow)
    }
  }

  companion object {
    val supportedStorageFactories:
      Map<StorageDetails.PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory> =
      mapOf(
        StorageDetails.PlatformCase.AWS to ::S3StorageFactory,
        StorageDetails.PlatformCase.FILE to ::FileSystemStorageFactory,
        StorageDetails.PlatformCase.GCS to ::GcsStorageFactory,
      )
    private val logger by loggerFor()
  }
}

fun main(args: Array<String>) = commandLineMain(DecryptPrivateMembershipQueryResults(), args)
