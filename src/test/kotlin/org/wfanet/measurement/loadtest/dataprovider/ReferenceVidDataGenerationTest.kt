/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.TypeRegistry
import java.nio.file.Paths
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.market.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.market.v1.MarketEvent
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.core.labeler.Labeler

@RunWith(JUnit4::class)
class ReferenceVidDataGenerationTest {

  @Test
  fun `generateEvents produces events with VIDs from labeler`() {
    val labeler = buildLabeler()
    val populationSpec = loadPopulationSpec()
    val spec = loadDataSpec()

    val shards =
      ReferenceVidDataGeneration.generateEvents(
          labeler,
          MarketEvent.getDefaultInstance(),
          populationSpec,
          spec,
        )
        .toList()

    assertThat(shards).isNotEmpty()
    val allEvents = shards.flatMap { it.labeledEvents.toList() }
    assertThat(allEvents).isNotEmpty()
    allEvents.forEach { event -> assertThat(event.vid).isGreaterThan(0) }
  }

  @Test
  fun `generateEvents distributes events across dates`() {
    val labeler = buildLabeler()
    val shards =
      ReferenceVidDataGeneration.generateEvents(
          labeler,
          MarketEvent.getDefaultInstance(),
          loadPopulationSpec(),
          loadDataSpec(),
        )
        .toList()

    val dates = shards.map { it.localDate }.distinct()
    assertThat(dates).hasSize(3)
  }

  @Test
  fun `generateEvents produces expected total event count`() {
    val labeler = buildLabeler()
    val shards =
      ReferenceVidDataGeneration.generateEvents(
          labeler,
          MarketEvent.getDefaultInstance(),
          loadPopulationSpec(),
          loadDataSpec(),
        )
        .toList()

    val totalEvents = shards.sumOf { it.labeledEvents.toList().size }
    assertThat(totalEvents).isEqualTo(600)
  }

  @Test
  fun `correction matrix causes some demographic reassignment`() {
    val labeler = buildLabeler()
    val shards =
      ReferenceVidDataGeneration.generateEvents(
          labeler,
          MarketEvent.getDefaultInstance(),
          loadPopulationSpec(),
          loadDataSpec(),
        )
        .toList()

    val allEvents = shards.flatMap { it.labeledEvents.toList() }
    val vidsInM1634Pool = allEvents.count { it.vid in 10000..10099 }
    val vidsInOtherPools = allEvents.count { it.vid !in 10000..10099 }

    assertThat(vidsInM1634Pool).isGreaterThan(0)
    assertThat(vidsInOtherPools).isGreaterThan(0)
  }

  @Test
  fun `all VIDs fall within PopulationSpec ranges`() {
    val labeler = buildLabeler()
    val populationSpec = loadPopulationSpec()
    val shards =
      ReferenceVidDataGeneration.generateEvents(
          labeler,
          MarketEvent.getDefaultInstance(),
          populationSpec,
          loadDataSpec(),
        )
        .toList()

    val allEvents = shards.flatMap { it.labeledEvents.toList() }
    val allVidRanges =
      populationSpec.subpopulationsList.flatMap { sub ->
        sub.vidRangesList.map { it.startVid..it.endVidInclusive }
      }

    allEvents.forEach { event ->
      val inRange = allVidRanges.any { event.vid in it }
    }
  }

  private fun buildLabeler(): Labeler {
    val modelPath =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "main",
          "resources",
          "testing",
          "labeler",
          "reference_test_model.textproto",
        )
      )!!
    return Labeler.build(parseTextProto(modelPath.toFile(), CompiledNode.getDefaultInstance()))
  }

  private fun loadPopulationSpec(): PopulationSpec {
    val path =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "main",
          "proto",
          "wfa",
          "measurement",
          "loadtest",
          "dataprovider",
          "reference_vid_population_spec.textproto",
        )
      )!!
    val typeRegistry = TypeRegistry.newBuilder().add(Common.getDescriptor()).build()
    return parseTextProto(path.toFile(), PopulationSpec.getDefaultInstance(), typeRegistry)
  }

  private fun loadDataSpec(): ReferenceVidEventGroupSpec {
    val path =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "main",
          "proto",
          "wfa",
          "measurement",
          "loadtest",
          "dataprovider",
          "reference_vid_data_spec.textproto",
        )
      )!!
    return parseTextProto(path.toFile(), ReferenceVidEventGroupSpec.getDefaultInstance())
  }

  @Test
  fun `exported SyntheticEventGroupSpec produces identical VIDs`() {
    val labeler = buildLabeler()
    val populationSpec = loadPopulationSpec()
    val spec = loadDataSpec()

    val refVidShards =
      ReferenceVidDataGeneration.generateEvents(
          labeler,
          MarketEvent.getDefaultInstance(),
          populationSpec,
          spec,
        )
        .toList()
    val refVidEvents = refVidShards.flatMap { it.labeledEvents.toList() }
    val refVids = refVidEvents.map { it.vid }.sorted()

    val labeledResults = ReferenceVidDataGeneration.labelAllInputs(labeler, spec)
    val validatedResults =
      ReferenceVidDataGeneration.validateAgainstPopulationSpec(labeledResults, populationSpec)

    val exportedSpec = ReferenceVidSpecExporter.exportSyntheticSpec(validatedResults, spec)
    val exportedPopSpec =
      ReferenceVidSpecExporter.exportPopulationSpec(validatedResults, populationSpec)

    val syntheticShards =
      SyntheticDataGeneration.generateEvents(
          MarketEvent.getDefaultInstance(),
          exportedPopSpec,
          exportedSpec,
        )
        .toList()
    val syntheticEvents = syntheticShards.flatMap { it.labeledEvents.toList() }
    val syntheticVids = syntheticEvents.map { it.vid }.sorted()

    assertThat(syntheticVids).isEqualTo(refVids)
  }

  companion object {
    @BeforeClass
    @JvmStatic
    fun registerTink() {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }
  }
}
