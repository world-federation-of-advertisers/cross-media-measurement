/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.TypeRegistry
import java.nio.file.Paths
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto

/**
 * Guards `hashonly_model_population_spec.textproto`, which the EDP Aggregator cloud test deploys as
 * the VID Labeling pipeline's `population_spec_blob_uri`.
 *
 * Without this, a typo or a drift from the model's pools would only surface partway through a full
 * cloud-test run, as a reach mismatch rather than as a parse or validation error.
 */
@RunWith(JUnit4::class)
class HashonlyModelPopulationSpecTest {
  private val writer = PopulationAttributeWriter(TestEvent.getDescriptor(), SPEC)

  @Test
  fun `spec is accepted by PopulationAttributeWriter`() {
    // Constructing the writer runs PopulationSpecValidator: VID ranges must be disjoint and every
    // population attribute must be set on every subpopulation.
    assertThat(SPEC.subpopulationsList).hasSize(6)
  }

  @Test
  fun `each pool boundary resolves to the demo the model documents for it`() {
    // Mirrors the pool table in cloudtest_hashonly_model.textproto, which is compiled to the
    // `edp7/hashonly_model` blob the cloud test labels with.
    assertPool(10000, 24999, Common.Gender.MALE, Common.AgeGroup.YEARS_18_TO_34)
    assertPool(25000, 39999, Common.Gender.MALE, Common.AgeGroup.YEARS_35_TO_54)
    assertPool(40000, 54999, Common.Gender.MALE, Common.AgeGroup.YEARS_55_PLUS)
    assertPool(55000, 69999, Common.Gender.FEMALE, Common.AgeGroup.YEARS_18_TO_34)
    assertPool(70000, 84999, Common.Gender.FEMALE, Common.AgeGroup.YEARS_35_TO_54)
    assertPool(85000, 99999, Common.Gender.FEMALE, Common.AgeGroup.YEARS_55_PLUS)
  }

  private fun assertPool(
    startVid: Long,
    endVidInclusive: Long,
    gender: Common.Gender,
    ageGroup: Common.AgeGroup,
  ) {
    for (vid in listOf(startVid, endVidInclusive)) {
      val common =
        writer.apply(TestEvent.getDefaultInstance(), vid).unpack(TestEvent::class.java).common
      assertThat(common.gender).isEqualTo(gender)
      assertThat(common.ageGroup).isEqualTo(ageGroup)
    }
  }

  companion object {
    private val SPEC: PopulationSpec =
      parseTextProto(
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
              "hashonly_model_population_spec.textproto",
            )
          )!!
          .toFile(),
        PopulationSpec.getDefaultInstance(),
        TypeRegistry.newBuilder().add(Common.getDescriptor()).build(),
      )
  }
}
