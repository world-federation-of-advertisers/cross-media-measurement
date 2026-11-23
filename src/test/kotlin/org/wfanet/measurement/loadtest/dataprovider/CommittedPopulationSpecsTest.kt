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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import com.google.devtools.build.runfiles.Runfiles
import com.google.protobuf.TypeRegistry
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.common.parseTextProto

/** Keeps committed test populations compatible when the testing template adds person attributes. */
@RunWith(Parameterized::class)
class CommittedPopulationSpecsTest(private val resourcePath: String) {
  @Test
  fun `population fixture supplies every required person attribute`() {
    val spec =
      parseTextProto(
        java.io.File(checkNotNull(RUNFILES.rlocation("wfa_measurement_system/$resourcePath"))),
        PopulationSpec.getDefaultInstance(),
        TypeRegistry.newBuilder().add(Common.getDescriptor()).build(),
      )
    PopulationSpecValidator.validate(spec, TestEvent.getDescriptor())
    assertThat(spec.subpopulationsCount).isGreaterThan(0)
  }

  companion object {
    private val RUNFILES = Runfiles.preload().withSourceRepository("")

    @JvmStatic
    @Parameterized.Parameters(name = "{0}")
    fun fixtures(): List<Array<String>> =
      listOf(
        arrayOf("src/main/k8s/testing/data/synthetic_population_spec_large.textproto"),
        arrayOf("src/main/k8s/testing/data/synthetic_population_spec_small.textproto"),
        arrayOf("src/main/k8s/testing/data/synthetic_population_spec_us.textproto"),
        arrayOf(
          "src/main/proto/wfa/measurement/loadtest/dataprovider/360m_population_spec.textproto"
        ),
        arrayOf(
          "src/main/proto/wfa/measurement/loadtest/dataprovider/hashonly_model_population_spec.textproto"
        ),
        arrayOf(
          "src/main/proto/wfa/measurement/loadtest/dataprovider/small_population_spec.textproto"
        ),
        arrayOf(
          "src/main/proto/wfa/measurement/api/v2alpha/population_specs/360m_population_spec_us.textproto"
        ),
      )
  }
}
