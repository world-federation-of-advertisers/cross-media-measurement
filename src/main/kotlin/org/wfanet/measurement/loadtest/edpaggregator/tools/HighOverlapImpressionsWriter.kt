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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import java.time.LocalDate
import java.util.logging.Logger
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.integration.common.ImpressionTestDataConfigs
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.edpaggregator.testing.ImpressionsWriter

/**
 * Writes the high overlap synthetic dataset as pre-labeled impressions stamped with a ModelLine.
 *
 * Impressions are stored under `<output_base_path>/model-line/<modelLineId>/<date>/`, so the
 * dataset is distinct from any other written under a different ModelLine.
 *
 * VIDs are taken straight from the data specs rather than passed through a VID model, which would
 * map them into the model's VID pool and collapse the overlap the dataset exists to provide.
 *
 * @param config high overlap [ImpressionTestDataConfig], restricted to the EDPs to write
 * @param populationSpec high overlap synthetic population
 * @param bucket impressions bucket
 * @param modelLine resource name of the ModelLine to stamp
 * @param kmsClientsByEdp KMS client and KEK URI per EDP; every EDP in [config] must have one
 * @param exporter receives each impression as it is written
 */
// TODO(world-federation-of-advertisers/cross-media-measurement#4564): Extract a shared
// SyntheticDataSeeder from GenerateSyntheticData and reduce this class to a wrapper around it.
class HighOverlapImpressionsWriter(
  private val config: ImpressionTestDataConfig,
  private val populationSpec: PopulationSpec,
  private val bucket: String,
  private val modelLine: String,
  private val kmsClientsByEdp: Map<String, KekClient>,
  private val exporter: HighOverlapImpressionExporter,
) {
  /** KMS client for an EDP's KEK. */
  data class KekClient(val kmsClient: KmsClient, val kekUri: String)

  init {
    val unhandled = config.eventGroupsList.map { it.edpName }.toSortedSet() - kmsClientsByEdp.keys
    require(unhandled.isEmpty()) { "No KMS configuration for high overlap EDP(s) $unhandled" }
  }

  suspend fun write() {
    for (eventGroup in config.eventGroupsList) {
      writeEventGroup(eventGroup, kmsClientsByEdp.getValue(eventGroup.edpName))
    }
  }

  /** Writes one impressions blob plus metadata per date for a single event group. */
  private suspend fun writeEventGroup(
    eventGroup: ImpressionTestDataConfig.SyntheticEventGroup,
    kekClient: KekClient,
  ) {
    val impressionWriter =
      ImpressionsWriter(
        eventGroupReferenceId = eventGroup.eventGroupReferenceId,
        eventGroupPath = "",
        kekUri = kekClient.kekUri,
        kmsClient = kekClient.kmsClient,
        impressionsBucket = bucket,
        impressionsMetadataBucket = bucket,
        storagePath = null,
        schema = GCS_SCHEME,
        outputKey = eventGroup.outputKey,
      )
    impressionWriter.writeLabeledImpressionData(
      generateShards(eventGroup),
      modelLine,
      modelLineOutputBasePath = eventGroup.outputBasePath,
    )
    logger.info(
      "Wrote high overlap impressions for event group '${eventGroup.eventGroupReferenceId}' " +
        "under ${eventGroup.outputBasePath}/model-line/<id>/<date>/."
    )
  }

  /** Generates the per-date shards for an event group, stamping each with its own entity key. */
  private fun generateShards(
    eventGroup: ImpressionTestDataConfig.SyntheticEventGroup
  ): Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> {
    require(eventGroup.entityKeySpecsList.isNotEmpty()) {
      "high overlap event group '${eventGroup.eventGroupReferenceId}' has no entity key spec"
    }

    val shardsByDate =
      mutableMapOf<LocalDate, MutableList<EntityKeysWithLabeledEvents<TestEvent>>>()
    for (entityKeySpec in eventGroup.entityKeySpecsList) {
      val entityKey = EntityKey(entityKeySpec.entityType, entityKeySpec.entityId)
      val events =
        SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(
            entityKeySpec.dataSpecResourcePath
          ),
        )
      for (shard in events) {
        // onEach is lazy, so each event reaches the exporter as the impressions writer pulls it.
        val labeledEvents =
          shard.labeledEvents.onEach {
            exporter.add(eventGroup.edpName, eventGroup.eventGroupReferenceId, entityKey, it)
          }
        shardsByDate
          .getOrPut(shard.localDate) { mutableListOf() }
          .add(EntityKeysWithLabeledEvents(listOf(entityKey), labeledEvents))
      }
    }
    return shardsByDate.entries
      .asSequence()
      .sortedBy { it.key }
      .map { (date, groups) -> EntityKeyedLabeledEventDateShard(date, groups.asSequence()) }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val GCS_SCHEME = "gs://"

    init {
      // Each blob is encrypted with a StreamingAead DEK wrapped by an Aead KEK.
      AeadConfig.register()
      StreamingAeadConfig.register()
    }
  }
}
