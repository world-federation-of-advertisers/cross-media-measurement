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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.io.File
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.KmsType
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.loadtest.edpaggregator.SyntheticDataGeneration
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine.Command
import picocli.CommandLine.Option

@Command(
  name = "generate-synthetic-data",
  description = ["Generates synthetic data for Panel Match."],
)
class GenerateSyntheticData : Runnable {
  @Option(
    names = ["--kms-type"],
    description = ["Type of kms: \${COMPLETION-CANDIDATES}"],
    required = true,
  )
  lateinit var kmsType: KmsType
    private set

  @Option(
    names = ["--local-storage-path"],
    description = ["Optional path to local storage."],
    required = false,
  )
  private var storagePath: File? = null

  @Option(
    names = ["--event-group-reference-id"],
    description = ["The EDP generated event group reference id for this campaign."],
    required = true,
  )
  lateinit var eventGroupReferenceId: String
    private set

  @set:Option(names = ["--kek-uri"], description = ["The KMS kek uri."], required = true)
  var kekUri: String = DEFAULT_KEK_URI
    private set

  @Option(
    names = ["--bucket"],
    description = ["The bucket where to write the files."],
    required = true,
  )
  lateinit var bucket: String
    private set

  @Option(
    names = ["--schema"],
    description = ["The schema to write to. Supported options are gs:// and file:///"],
    required = true,
    defaultValue = "file:///",
  )
  lateinit var schema: String
    private set

  @Option(
    names = ["--zone-id"],
    description = ["The time zone by which to shard the data."],
    required = true,
    defaultValue = "UTC",
  )
  lateinit var zoneId: String
    private set

  @Option(
    names = ["--population-spec"],
    description = ["The resource of the population-spec."],
    required = true,
    defaultValue = "synthetic_population_spec_360m.textproto",
  )
  lateinit var populationSpecResourceName: String
    private set

  @Option(
    names = ["--data-spec"],
    description = ["The resource of the data-spec."],
    required = true,
    defaultValue = "synthetic_event_group_spec_90day_1billion.textproto",
  )
  lateinit var dataSpecResourceName: String
    private set

  @kotlin.io.path.ExperimentalPathApi
  override fun run() {
    val syntheticPopulationSpec: SyntheticPopulationSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve(populationSpecResourceName).toFile(),
        SyntheticPopulationSpec.getDefaultInstance(),
      )
    val syntheticEventGroupSpec: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve(dataSpecResourceName).toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )
    val events =
      SyntheticDataGeneration.generateEvents(
        messageInstance = TestEvent.getDefaultInstance(),
        populationSpec = syntheticPopulationSpec,
        syntheticEventGroupSpec = syntheticEventGroupSpec,
      )
    val kmsClient: KmsClient = run {
      when (kmsType) {
        KmsType.FAKE -> {
          val client = FakeKmsClient()
          val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
          client.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
          client
        }
        KmsType.GCP -> GcpKmsClient()
      }
    }
    runBlocking {
      writeImpressionData(
        events,
        eventGroupReferenceId,
        kekUri,
        kmsClient,
        storagePath,
        bucket,
        schema,
        ZoneId.of(zoneId),
      )
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DEFAULT_KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "key1"

    private val TEST_DATA_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "proto",
        "wfa",
        "measurement",
        "loadtest",
        "edpaggregator",
      )
    private val TEST_DATA_RUNTIME_PATH = getRuntimePath(TEST_DATA_PATH)!!

    suspend fun writeImpressionData(
      events: Flow<Pair<LocalDate, Flow<LabeledImpression>>>,
      eventGroupReferenceId: String,
      kekUri: String,
      kmsClient: KmsClient,
      storagePath: File?,
      bucket: String,
      schema: String = "file:///",
      zoneId: ZoneId = ZoneId.of("UTC"),
    ) {
      // Set up streaming encryption
      val tinkKeyTemplateType = "AES128_GCM_HKDF_1MB"
      val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
      val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
      val serializedEncryptionKey =
        ByteString.copyFrom(
          TinkProtoKeysetFormat.serializeEncryptedKeyset(
            keyEncryptionHandle,
            kmsClient.getAead(kekUri),
            byteArrayOf(),
          )
        )
      val encryptedDek =
        EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()

      val storageMap: MutableMap<String, MesosRecordIoStorageClient> = mutableMapOf()

      var currentDsEvents = mutableListOf<LabeledImpression>()
      var currentDs: String? = null
      events.collect { (localDate: LocalDate, labeledImpressions: Flow<LabeledImpression>) ->
          //val date = LocalDate.ofInstant(labeledImpression.eventTime.toInstant(), zoneId)
          val ds = localDate.toString()
            logger.info("Writing Date: $currentDs")

            val impressionsBlobKey =
              "ds/$currentDs/event-group-reference-id/$eventGroupReferenceId/impressions"
            val impressionsFileUri = "$schema$bucket/$impressionsBlobKey"
            val mesosRecordIoStorageClient = run {
              val impressionsStorageClient = SelectedStorageClient(impressionsFileUri, storagePath)

              val aeadStorageClient =
                impressionsStorageClient.withEnvelopeEncryption(
                  kmsClient,
                  kekUri,
                  serializedEncryptionKey,
                )

              // Wrap aead client in mesos client
              val mesosRecordIoStorageClient = MesosRecordIoStorageClient(aeadStorageClient)
              storageMap.set(impressionsBlobKey, mesosRecordIoStorageClient)
              mesosRecordIoStorageClient
            }
            logger.info("Writing impressions to $impressionsBlobKey")
            // Write impressions to storage
            runBlocking {
              mesosRecordIoStorageClient.writeBlob(
                impressionsBlobKey,
                labeledImpressions.map { it.toByteString() },
              )
            }
            val impressionsMetaDataBlobKey =
              "ds/$ds/event-group-reference-id/$eventGroupReferenceId/metadata"

            val impressionsMetadataFileUri = "$schema$bucket/$impressionsMetaDataBlobKey"

            logger.info("Writing metadata to $impressionsMetadataFileUri")

            // Create the impressions metadata store
            val impressionsMetadataStorageClient =
              SelectedStorageClient(impressionsMetadataFileUri, storagePath)

            val blobDetails = blobDetails {
              this.blobUri = impressionsFileUri
              this.encryptedDek = encryptedDek
            }
            runBlocking {
              impressionsMetadataStorageClient.writeBlob(
                impressionsMetaDataBlobKey,
                blobDetails.toByteString(),
              )
            }
            currentDs = ds
            currentDsEvents.clear()
            logger.info("CLEARED")

        }
    }
  }

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }
}

fun main(args: Array<String>) = commandLineMain(GenerateSyntheticData(), args)
