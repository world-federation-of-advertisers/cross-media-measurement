/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.shareshuffle

import com.google.common.truth.Truth.assertThat
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpec

@RunWith(JUnit4::class)
class FrequencyVectorBuilderTest {
  @Ignore("SUT not implemented yet")
  @Test
  fun `demonstrate the usage of the libraries`() {
    // TODO(@kungfucraig): Update this once the SUT is complete.

    // Build the map
    val populationSpec = PopulationSpec.newBuilder().build()
    val index: VidIndexMap = InMemoryVidIndexMap(populationSpec)

    val samplingInterval = VidSamplingInterval.newBuilder().build()
    val builder = FrequencyVectorBuilder(index, samplingInterval)

    val vids = ArrayList<Long>()
    for (vid in vids) {
      builder.addVid(vid)
    }

    val anotherBuilder = FrequencyVectorBuilder(index, samplingInterval)
    val moreVids = ArrayList<Long>()
    anotherBuilder.addVids(moreVids)

    builder.addVids(anotherBuilder)

    val frequencyVector: FrequencyVector = builder.build()

    assertThat(frequencyVector.dataList).isEmpty()

    // TODO(kungfucraig): should we support a merge function?
    // If so, it seems lke it would be nice for FrequencyVector to contain it's
    // sampling interval. Or we could just do error checks on length.
    // If we want this should we have:
    // 1. return a third merged thing?
    // 2. modify v1 or v2?
    // In either case, it seems that this function should be in the any sketch library
    // nearby the FrequencyVector definition.
    // FrequencyProtos.merge(v1, v2)
  }
}
