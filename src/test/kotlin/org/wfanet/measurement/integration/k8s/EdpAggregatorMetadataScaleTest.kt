// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.k8s

import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.common.truth.Truth.assertThat
import com.google.devtools.build.runfiles.Runfiles
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.measurement.integration.k8s.testing.HighOverlapDatasetManifest
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataView
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt.filter
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.integration.common.HighOverlapDataset
import org.wfanet.measurement.integration.common.ImpressionTestDataConfigs
import org.wfanet.measurement.storage.SelectedStorageClient

/** Checks compact metadata listing over real, seeded blobs with thousands of entity keys. */
@RunWith(JUnit4::class)
class EdpAggregatorMetadataScaleTest {
  @Test
  fun `compact listing filters thousands of keys and returns each blob exactly once`() =
    runBlocking {
      val config =
        parseTextProto(
          ImpressionTestDataConfigs.resolveSpecPath(HighOverlapDataset.CONFIG_RESOURCE_PATH),
          ImpressionTestDataConfig.getDefaultInstance(),
        )
      val modelLine = env("HIGH_OVERLAP_MODEL_LINE")
      val modelLineId = requireNotNull(ModelLineKey.fromName(modelLine)).modelLineId
      val bucket = env("SECURE_COMPUTATION_STORAGE_BUCKET")
      val runfiles = Runfiles.preload().withSourceRepository("")
      val environment = env("CMMS_ENVIRONMENT")
      check(environment in setOf("dev", "head", "qa"))
      val syncConfigFile =
        checkNotNull(
            runfiles.getRuntimePath(
              Paths.get(
                "wfa_measurement_system/src/main/terraform/gcloud/cmms/envs/$environment/data-availability-sync-config.textproto"
              )
            )
          )
          .toFile()
      val syncConfigs =
        parseTextProto(syncConfigFile, DataAvailabilitySyncConfigs.getDefaultInstance())
      val parentsByPath =
        syncConfigs.configsList.associate { it.edpImpressionPath to it.dataProvider }
      val storage =
        SelectedStorageClient(
          blobUri = SelectedStorageClient.parseBlobUri("gs://$bucket/"),
          rootDirectory = null,
          projectId = env("GOOGLE_CLOUD_PROJECT"),
        )
      val gcs =
        StorageOptions.newBuilder().setProjectId(env("GOOGLE_CLOUD_PROJECT")).build().service
      val manifestKey = HighOverlapDataset.manifestBlobKey(modelLineId)
      val manifest =
        HighOverlapDatasetManifest.parseFrom(
          checkNotNull(storage.getBlob(manifestKey)).read().flatten()
        )
      assertThat(manifest.specFingerprint)
        .isEqualTo(HighOverlapDataset.fingerprint(HighOverlapDataset.CONFIG_RESOURCE_PATH))
      val stressGroupsByEdp =
        config.eventGroupsList.filter { it.additionalEntityKeyCount > 0 }.groupBy { it.edpName }
      assertThat(stressGroupsByEdp).hasSize(4)
      val secrets =
        checkNotNull(
          runfiles.getRuntimePath(
            Paths.get("wfa_measurement_system/src/main/k8s/testing/secretfiles")
          )
        )
      val certs =
        SigningCerts.fromPemFiles(
          secrets.resolve("data_availability_tls.pem").toFile(),
          secrets.resolve("data_availability_tls.key").toFile(),
          secrets.resolve("edp_aggregator_root.pem").toFile(),
        )
      // Keep the channel's normal 4 MiB inbound message limit.
      val channel = buildMutualTlsChannel(env("EDPA_PUBLIC_API_TARGET"), certs, "localhost")
      try {
        val stub = ImpressionMetadataServiceCoroutineStub(channel)
        verifyMetadataCoverage(stub, config, parentsByPath, modelLine, bucket, gcs)
        for ((edp, groups) in stressGroupsByEdp) {
          val expectedKeysByUri =
            groups
              .flatMap { group ->
                group.additionalEntityKeyDatesList.map { date ->
                  val uri =
                    "gs://$bucket/${group.outputBasePath}/model-line/$modelLineId/${date.toLocalDate()}/metadata-${group.outputKey}.binpb"
                  val keys =
                    (0 until group.additionalEntityKeyCount).map { index ->
                      entityKey {
                        entityType = "ad-unit"
                        entityId = "scale-${group.eventGroupReferenceId}-$index"
                      }
                    }
                  uri to keys
                }
              }
              .toMap()
          assertThat(expectedKeysByUri).hasSize(2)
          val request = listImpressionMetadataRequest {
            parent = parentsByPath.getValue(groups.first().outputBasePath)
            pageSize = 1
            view = ImpressionMetadataView.IMPRESSION_METADATA_VIEW_BASIC
            this.filter = filter {
              this.modelLine = modelLine
              entityKeys += expectedKeysByUri.values.flatten()
            }
          }
          assertThat(request.serializedSize).isLessThan(4 * 1024 * 1024)
          val basicByUri = mutableMapOf<String, ImpressionMetadata>()
          val seenTokens = mutableSetOf<String>()
          var token = ""
          var basicBytes = 0
          val startedAt = System.nanoTime()
          do {
            val response =
              stub
                .withDeadlineAfter(30, TimeUnit.SECONDS)
                .listImpressionMetadata(request.toBuilder().setPageToken(token).build())
            basicBytes += response.serializedSize
            assertThat(response.impressionMetadataList).hasSize(1)
            for (metadata in response.impressionMetadataList) {
              assertThat(metadata.entityKeysList).isEmpty()
              assertThat(basicByUri.put(metadata.blobUri, metadata)).isNull()
            }
            token = response.nextPageToken
            if (token.isNotEmpty()) {
              assertThat(seenTokens.add(token)).isTrue()
              assertThat(basicByUri.size).isLessThan(expectedKeysByUri.size)
            }
          } while (token.isNotEmpty())
          assertThat(basicByUri.keys).containsExactlyElementsIn(expectedKeysByUri.keys)
          var fullBytes = 0
          for ((uri, expectedKeys) in expectedKeysByUri) {
            // Check both sides of the 4,000-key batching target and the end of the blob.
            for (index in listOf(0, 3_999, 4_000, expectedKeys.lastIndex).distinct()) {
              val bySingleKey =
                stub
                  .withDeadlineAfter(30, TimeUnit.SECONDS)
                  .listImpressionMetadata(
                    listImpressionMetadataRequest {
                      parent = request.parent
                      pageSize = 2
                      view = ImpressionMetadataView.IMPRESSION_METADATA_VIEW_BASIC
                      this.filter = filter {
                        this.modelLine = modelLine
                        entityKeys += expectedKeys[index]
                      }
                    }
                  )
              assertThat(bySingleKey.impressionMetadataList.map { it.blobUri }).containsExactly(uri)
              assertThat(bySingleKey.nextPageToken).isEmpty()
            }
            val full =
              stub
                .withDeadlineAfter(30, TimeUnit.SECONDS)
                .listImpressionMetadata(
                  listImpressionMetadataRequest {
                    parent = request.parent
                    pageSize = 1
                    view = ImpressionMetadataView.IMPRESSION_METADATA_VIEW_FULL
                    this.filter = filter { blobUris += uri }
                  }
                )
            assertThat(full.impressionMetadataList).hasSize(1)
            assertThat(full.nextPageToken).isEmpty()
            val metadata = full.impressionMetadataList.single()
            fullBytes += full.serializedSize
            assertThat(metadata.entityKeysList.filter { it.entityType == "ad-unit" })
              .containsExactlyElementsIn(expectedKeys)
            assertThat(metadata.toBuilder().clearEntityKeys().build())
              .isEqualTo(basicByUri.getValue(uri))
            assertThat(metadata.metadataBlobGeneration).isGreaterThan(0L)
            assertThat(metadata.impressionsBlobGeneration).isGreaterThan(0L)
            assertThat(metadata.outputDoneBlobGeneration).isGreaterThan(0L)
            val sidecarKey = SelectedStorageClient.parseBlobUri(uri).key
            assertThat(metadata.metadataBlobGeneration)
              .isEqualTo(checkNotNull(gcs.get(bucket, sidecarKey)).generation)
            val impressionsKey =
              SelectedStorageClient.parseBlobUri(
                  uri.substringBeforeLast('/') +
                    "/impressions" +
                    uri.substringAfterLast('/').removePrefix("metadata").removeSuffix(".binpb")
                )
                .key
            assertThat(metadata.impressionsBlobGeneration)
              .isEqualTo(checkNotNull(gcs.get(bucket, impressionsKey)).generation)
            val doneKey = sidecarKey.substringBeforeLast('/') + "/done"
            assertThat(metadata.outputDoneBlobGeneration)
              .isEqualTo(checkNotNull(gcs.get(bucket, doneKey)).generation)
            val sidecar =
              BlobDetails.parseFrom(checkNotNull(storage.getBlob(sidecarKey)).read().flatten())
            val sidecarKeys =
              sidecar.entityKeysList.flatMap { group ->
                group.entityIdsList.map { id ->
                  entityKey {
                    entityType = group.entityType
                    entityId = id
                  }
                }
              }
            assertThat(metadata.entityKeysList).containsExactlyElementsIn(sidecarKeys)
            assertThat(sidecar.modelLine).isEqualTo(modelLine)
          }
          assertThat(basicBytes.toLong() * 10).isLessThan(fullBytes.toLong())
          logger.info(
            "Metadata scale: edp=$edp blobs=${basicByUri.size} extra_keys_per_blob=5000 " +
              "filter_request_bytes=${request.serializedSize} basic_response_bytes=$basicBytes full_response_bytes=$fullBytes " +
              "elapsed_ms=${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)}"
          )
        }
      } finally {
        channel.shutdown()
      }
    }

  /** Audits every active seed row against the expected blobs and their live generations. */
  private suspend fun verifyMetadataCoverage(
    stub: ImpressionMetadataServiceCoroutineStub,
    config: ImpressionTestDataConfig,
    parentsByPath: Map<String, String>,
    modelLine: String,
    bucket: String,
    gcs: Storage,
  ) {
    val modelLineId = requireNotNull(ModelLineKey.fromName(modelLine)).modelLineId
    for ((path, groups) in config.eventGroupsList.groupBy { it.outputBasePath }) {
      val prefix = "$path/model-line/$modelLineId/"
      val expectedKeys = buildSet {
        for (group in groups) {
          for (entity in group.entityKeySpecsList) {
            for (dateSpec in
              ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(entity.dataSpecResourcePath)
                .dateSpecsList) {
              var date = dateSpec.dateRange.start.toLocalDate()
              val endExclusive = dateSpec.dateRange.endExclusive.toLocalDate()
              while (date < endExclusive) {
                add("$prefix$date/metadata-${group.outputKey}.binpb")
                date = date.plusDays(1)
              }
            }
          }
        }
      }
      val generations =
        gcs.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll().associate {
          it.name to it.generation
        }
      val liveMetadataKeys =
        generations.keys.filter {
          it.removePrefix(prefix).split('/').size == 2 &&
            it.substringAfterLast('/').startsWith("metadata-") &&
            it.endsWith(".binpb")
        }
      assertThat(liveMetadataKeys).containsExactlyElementsIn(expectedKeys)
      val seenKeys = mutableSetOf<String>()
      val seenTokens = mutableSetOf<String>()
      var token = ""
      var maxPageBytes = 0
      do {
        val response =
          stub
            .withDeadlineAfter(30, TimeUnit.SECONDS)
            .listImpressionMetadata(
              listImpressionMetadataRequest {
                parent = parentsByPath.getValue(path)
                pageSize = 1000
                pageToken = token
                view = ImpressionMetadataView.IMPRESSION_METADATA_VIEW_BASIC
                this.filter = filter {
                  this.modelLine = modelLine
                  blobUriPrefix = "gs://$bucket/$prefix"
                }
              }
            )
        maxPageBytes = maxOf(maxPageBytes, response.serializedSize)
        assertThat(response.serializedSize).isLessThan(4 * 1024 * 1024)
        for (metadata in response.impressionMetadataList) {
          val key = SelectedStorageClient.parseBlobUri(metadata.blobUri).key
          assertThat(seenKeys.add(key)).isTrue()
          assertThat(metadata.entityKeysList).isEmpty()
          assertThat(metadata.metadataBlobGeneration).isEqualTo(generations.getValue(key))
          val impressionsKey =
            key.substringBeforeLast('/') +
              "/impressions" +
              key.substringAfterLast('/').removePrefix("metadata").removeSuffix(".binpb")
          assertThat(metadata.impressionsBlobGeneration)
            .isEqualTo(generations.getValue(impressionsKey))
          assertThat(metadata.outputDoneBlobGeneration)
            .isEqualTo(generations.getValue(key.substringBeforeLast('/') + "/done"))
        }
        token = response.nextPageToken
        if (token.isNotEmpty()) {
          assertThat(seenTokens.add(token)).isTrue()
          assertThat(response.impressionMetadataList).isNotEmpty()
        }
      } while (token.isNotEmpty())
      assertThat(seenKeys).containsExactlyElementsIn(expectedKeys)
      logger.info(
        "Seed metadata audit: path=$path blobs=${seenKeys.size} max_basic_page_bytes=$maxPageBytes all_generations_match=true"
      )
    }
  }

  private fun env(name: String): String =
    checkNotNull(System.getenv(name)?.takeIf { it.isNotEmpty() }) {
      "$name must be set for the metadata scale cloud test"
    }

  companion object {
    private val logger = Logger.getLogger(EdpAggregatorMetadataScaleTest::class.java.name)
  }
}
