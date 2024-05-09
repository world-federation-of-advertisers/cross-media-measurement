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

package org.wfanet.measurement.integration.common

import com.google.protobuf.Message
import java.nio.file.Paths
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto

/** Specifications for synthetic generation. */
object SyntheticGenerationSpecs {
  private val TEST_DATA_PATH =
    Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "data")
  private val TEST_DATA_RUNTIME_PATH = getRuntimePath(TEST_DATA_PATH)!!

  /** Population spec for synthetic generation of population ~34,000,000. */
  val SYNTHETIC_POPULATION_SPEC_LARGE: SyntheticPopulationSpec by lazy {
    loadTestData(
      "synthetic_population_spec_large.textproto",
      SyntheticPopulationSpec.getDefaultInstance(),
    )
  }

  /** EventGroup specs for synthetic generation based on [SYNTHETIC_POPULATION_SPEC_LARGE]. */
  val SYNTHETIC_DATA_SPECS_LARGE: List<SyntheticEventGroupSpec> by lazy {
    listOf(
      loadTestData(
        "synthetic_event_group_spec_large_1.textproto",
        SyntheticEventGroupSpec.getDefaultInstance(),
      ),
      loadTestData(
        "synthetic_event_group_spec_large_2.textproto",
        SyntheticEventGroupSpec.getDefaultInstance(),
      ),
    )
  }

  /**
   * EventGroup specs for synthetic generation based on [SYNTHETIC_POPULATION_SPEC_LARGE].
   *
   * The total reach is ~2,000,000.
   */
  val SYNTHETIC_DATA_SPECS_LARGE_2M: List<SyntheticEventGroupSpec> by lazy {
    listOf(
      loadTestData(
        "synthetic_event_group_spec_large_1.textproto",
        SyntheticEventGroupSpec.getDefaultInstance(),
      ),
      loadTestData(
        "synthetic_event_group_spec_large_2.textproto",
        SyntheticEventGroupSpec.getDefaultInstance(),
      ),
      loadTestData(
        "synthetic_event_group_spec_large_3.textproto",
        SyntheticEventGroupSpec.getDefaultInstance(),
      ),
    )
  }

  /** Population spec for synthetic generation of population ~100,000. */
  val SYNTHETIC_POPULATION_SPEC_SMALL: SyntheticPopulationSpec by lazy {
    loadTestData(
      "synthetic_population_spec_small.textproto",
      SyntheticPopulationSpec.getDefaultInstance(),
    )
  }

  /** EventGroup specs for synthetic generation based on [SYNTHETIC_POPULATION_SPEC_SMALL]. */
  val SYNTHETIC_DATA_SPECS_SMALL: List<SyntheticEventGroupSpec> by lazy {
    listOf(
      loadTestData(
        "synthetic_event_group_spec_small_1.textproto",
        SyntheticEventGroupSpec.getDefaultInstance(),
      ),
      loadTestData(
        "synthetic_event_group_spec_small_2.textproto",
        SyntheticEventGroupSpec.getDefaultInstance(),
      ),
    )
  }

  /** EventGroup specs for synthetic generation based on [SYNTHETIC_POPULATION_SPEC_SMALL]. */
  val SYNTHETIC_DATA_SPECS_SMALL_36K: List<SyntheticEventGroupSpec> by lazy {
    listOf(
      loadTestData(
        "synthetic_event_group_spec_small_1.textproto",
        SyntheticEventGroupSpec.getDefaultInstance(),
      ),
      loadTestData(
        "synthetic_event_group_spec_small_2.textproto",
        SyntheticEventGroupSpec.getDefaultInstance(),
      ),
      loadTestData(
        "synthetic_event_group_spec_small_3.textproto",
        SyntheticEventGroupSpec.getDefaultInstance(),
      ),
    )
  }

  private fun <T : Message> loadTestData(fileName: String, defaultInstance: T): T {
    return parseTextProto(TEST_DATA_RUNTIME_PATH.resolve(fileName).toFile(), defaultInstance)
  }
}
