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
class GenerateVidDataIntegrationTest {

  @Test
  fun `exported SyntheticEventGroupSpec produces identical VIDs`() {
    val labeler: Labeler = buildLabeler()
    val populationSpec: PopulationSpec = loadPopulationSpec()
    val spec: ReferenceVidEventGroupSpec = loadDataSpec()

    // Generate labeled VIDs via the labeler path. Materialize once since
    // LabeledVidDateShard.labeledVids is a Sequence.
    val allLabeledVids: List<ReferenceVidDataGeneration.LabeledVid> =
      ReferenceVidDataGeneration.generateEvents(labeler, populationSpec, spec)
        .flatMap { it.labeledVids.toList() }
        .toList()
    val refVids: Set<Long> = allLabeledVids.map { it.vid }.toSet()
    val converted: ReferenceVidSpecConverter.ConvertedSpecs =
      ReferenceVidSpecConverter.convert(allLabeledVids, spec, populationSpec)

    // Generate via the standard synthetic path with the converted specs.
    val syntheticShards: List<LabeledEventDateShard<MarketEvent>> =
      SyntheticDataGeneration.generateEvents(
          MarketEvent.getDefaultInstance(),
          converted.populationSpec,
          converted.syntheticEventGroupSpec,
        )
        .toList()
    val syntheticVids: Set<Long> =
      syntheticShards.flatMap { it.labeledEvents.toList() }.map { it.vid }.toSet()

    assertThat(syntheticVids).isEqualTo(refVids)
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

  companion object {
    @BeforeClass
    @JvmStatic
    fun registerTink() {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }
  }
}
