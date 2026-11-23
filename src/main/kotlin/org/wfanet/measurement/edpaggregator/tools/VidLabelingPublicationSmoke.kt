// Copyright 2026 The Cross-Media Measurement Authors
// Licensed under the Apache License, Version 2.0.

package org.wfanet.measurement.edpaggregator.tools

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.Struct
import com.google.protobuf.Value
import com.google.protobuf.util.JsonFormat
import java.time.LocalDate
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeout
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileMetadata
import org.wfanet.measurement.edpaggregator.rawimpressions.UndigestedEvent
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.vidlabeler.ConvertedImpression
import org.wfanet.measurement.edpaggregator.vidlabeler.ImpressionConverter
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledOutputPublication
import org.wfanet.measurement.edpaggregator.vidlabeler.ModelLineContext
import org.wfanet.measurement.edpaggregator.vidlabeler.PlainVidLabelingSink
import org.wfanet.measurement.edpaggregator.vidlabeler.PopulationAttributeWriter
import org.wfanet.measurement.edpaggregator.vidlabeler.VidAssigner
import org.wfanet.measurement.edpaggregator.vidlabeler.VidLabelerMetrics
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.common.VirtualPersonActivity
import picocli.CommandLine

/**
 * Production-sink publication smoke with synthetic events and an ephemeral local test KEK.
 * Exercises GCS generation conditions and sidecar selection without production KMS/data access.
 */
class VidLabelingPublicationSmokeRunner(
  private val storageClientFactory: (String) -> ConditionalOperationStorageClient
) {
  suspend fun run(prefix: String): Struct =
    withTimeout(120_000L) {
      require(prefix.startsWith("gs://") && "/rollout/" in prefix) {
        "Smoke requires a dedicated GCS rollout prefix"
      }
      val runPrefix = "${prefix.trimEnd('/')}/${UUID.randomUUID()}"
      val root = SelectedStorageClient.parseBlobUri(runPrefix)
      val bucketClient = storageClientFactory(runPrefix)
      AeadConfig.register()
      val kekUri = FakeKmsClient.KEY_URI_PREFIX + UUID.randomUUID()
      val kms =
        FakeKmsClient().apply {
          setAead(
            kekUri,
            KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM")).getPrimitive(Aead::class.java),
          )
        }
      val population =
        PopulationSpec.newBuilder()
          .addSubpopulations(
            PopulationSpec.SubPopulation.newBuilder()
              .addAttributes(
                ProtoAny.pack(
                  Person.newBuilder()
                    .setGender(Person.Gender.FEMALE)
                    .setAgeGroup(Person.AgeGroup.YEARS_35_TO_54)
                    .setSocialGradeGroup(Person.SocialGradeGroup.C2_D_E)
                    .build()
                )
              )
              .addVidRanges(
                PopulationSpec.VidRange.newBuilder().setStartVid(1L).setEndVidInclusive(1000L)
              )
          )
          .build()
      val populationWriter = PopulationAttributeWriter(TestEvent.getDescriptor(), population)
      val assigner =
        object : VidAssigner {
          override fun assign(input: LabelerInput): LabelerOutput =
            LabelerOutput.newBuilder()
              .addPeople(VirtualPersonActivity.newBuilder().setVirtualPersonId(42L))
              .build()
        }
      val context =
        ModelLineContext(
          "modelProviders/rollout/modelSuites/smoke/modelLines/publication",
          ActiveWindow(1000L, 2000L),
          assigner,
          VidLabelerParams.ModelLineConfig.getDefaultInstance(),
        )
      val converter = ImpressionConverter { _, _ ->
        ConvertedImpression(
          LabelerInput.newBuilder().setTimestampUsec(1500L).build(),
          { TestEvent.getDefaultInstance() },
          populationWriter,
          emptyList(),
        )
      }
      val publications = CopyOnWriteArrayList<LabeledOutputPublication>()
      val sinks =
        List(2) {
          PlainVidLabelingSink(
            inputBlobUri = "$runPrefix/input.parquet",
            modelLineContexts = listOf(context),
            impressionConverter = converter,
            fileMetadata = RawImpressionFileMetadata(LocalDate.of(1970, 1, 1)),
            encryptKmsClient = kms,
            encryptKekUri = kekUri,
            outputStorageParams =
              VidLabelerParams.StorageParams.newBuilder()
                .setImpressionsBlobPrefix("$runPrefix/output")
                .build(),
            storageConfig = StorageConfig(),
            dataProvider = "dataProviders/rollout",
            metrics = VidLabelerMetrics(),
            encryptionKeySemaphore = Semaphore(2),
            publicationObserver = { publications += it },
            storageClientFactory = storageClientFactory,
          )
        }
      var records = 0
      try {
        val input = UndigestedEvent(emptyMap<String, ParquetValue>())
        sinks.forEach { it.processBatch(listOf(input)) }
        coroutineScope { sinks.map { sink -> async(Dispatchers.IO) { sink.commit() } }.awaitAll() }
        val blobs = bucketClient.listBlobs(root.key + "/").toList()
        val sidecars = blobs.filter { it.blobKey.endsWith(".metadata.binpb") }
        val data =
          blobs.filter { ".attempt-" in it.blobKey && !it.blobKey.endsWith(".metadata.binpb") }
        check(sidecars.size == 1 && data.size == 2) {
          "Smoke did not produce two competing writes and one sidecar"
        }
        val selected = BlobDetails.parseFrom(sidecars.single().read().flatten())
        check(data.any { it.blobKey == SelectedStorageClient.parseBlobUri(selected.blobUri).key }) {
          "Sidecar selected an unrelated/missing data object"
        }
        val encrypted =
          storageClientFactory(selected.blobUri)
            .withEnvelopeEncryption(kms, kekUri, selected.encryptedDek.ciphertext)
        val blob =
          checkNotNull(
            MesosRecordIoStorageClient(encrypted)
              .getBlob(SelectedStorageClient.parseBlobUri(selected.blobUri).key)
          )
        val impressions = blob.read().toList().map { LabeledImpression.parseFrom(it) }
        check(impressions.size == 1 && impressions.single().vid == 42L) {
          "Winning data/key pair is invalid"
        }
        check(
          publications.count {
            it.type == LabeledOutputPublication.Type.METADATA_SIDECAR &&
              it.outcome == "already_completed"
          } == 1
        ) {
          "Both writers did not observe the same committed winner"
        }
        records = impressions.size
      } finally {
        sinks.forEach { it.close() }
        val objects = bucketClient.listBlobs(root.key + "/").toList()
        objects.forEach { blob ->
          check(blob.blobKey.startsWith(root.key + "/")) { "Unsafe cleanup scope" }
          blob.delete()
        }
        check(bucketClient.listBlobs(root.key + "/").toList().isEmpty()) {
          "Smoke cleanup incomplete"
        }
      }
      Struct.newBuilder()
        .putFields("status", Value.newBuilder().setStringValue("PASS").build())
        .putFields("competing_writers", Value.newBuilder().setNumberValue(2.0).build())
        .putFields("sidecar_winners", Value.newBuilder().setNumberValue(1.0).build())
        .putFields(
          "decrypted_records",
          Value.newBuilder().setNumberValue(records.toDouble()).build(),
        )
        .putFields("cleanup_complete", Value.newBuilder().setBoolValue(true).build())
        .build()
    }
}

@CommandLine.Command(name = "vid-labeling-publication-smoke", mixinStandardHelpOptions = true)
class VidLabelingPublicationSmoke : Runnable {
  @CommandLine.Option(names = ["--blob-prefix"], required = true)
  private lateinit var prefix: String
  @CommandLine.Option(names = ["--project"], required = true) private lateinit var project: String

  override fun run() = runBlocking {
    val result =
      VidLabelingPublicationSmokeRunner { uri ->
          SelectedStorageClient(SelectedStorageClient.parseBlobUri(uri), projectId = project)
            .underlyingClient
        }
        .run(prefix)
    println(JsonFormat.printer().omittingInsignificantWhitespace().print(result))
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(VidLabelingPublicationSmoke(), args)
  }
}
