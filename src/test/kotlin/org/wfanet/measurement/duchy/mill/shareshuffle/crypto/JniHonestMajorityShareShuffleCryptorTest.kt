// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.shareshuffle.crypto

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.protocol.CompleteAggregationPhaseRequestKt
import org.wfanet.measurement.internal.duchy.protocol.CompleteShufflePhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeAggregationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.shareShuffleFrequencyVectorParams

@RunWith(JUnit4::class)
class JniHonestMajorityShareShuffleCryptorTest {

  private val cryptor = JniHonestMajorityShareShuffleCryptor()

  @Test
  fun `check JNI lib is loaded successfully`() {
    val e1 =
      assertFailsWith(RuntimeException::class) {
        cryptor.completeReachAndFrequencyShufflePhase(
          CompleteShufflePhaseRequest.getDefaultInstance()
        )
      }
    assertThat(e1.message).contains("register count")
  }

  @Test
  fun `completeReachAndFrequencyAggregationPhase with no noise returns exact reach and frequency`() {
    // Share 1: [1, 0, 2, 1, 0, 3], Share 2: [0, 0, 0, 1, 0, 0]
    // Combined (mod 127): [1, 0, 2, 2, 0, 3]
    // Non-empty registers: 4 (indices 0, 2, 3, 5)
    // Frequency histogram (values < max_freq=3): {0: 2, 1: 1, 2: 2}
    // frequency_histogram[3+] = 6 - 5 = 1
    // adjusted_total = 6 - 2 = 4
    // reach = 4, distribution = {1: 0.25, 2: 0.5, 3: 0.25}
    val request = completeAggregationPhaseRequest {
      frequencyVectorParams = shareShuffleFrequencyVectorParams {
        registerCount = 6
        maximumCombinedFrequency = 30
        ringModulus = 127
      }
      maximumFrequency = 3
      vidSamplingIntervalWidth = 1.0f
      noiseMechanism = NoiseMechanism.NONE
      frequencyVectorShares +=
        CompleteAggregationPhaseRequestKt.shareData { shareVector += listOf(1, 0, 2, 1, 0, 3) }
      frequencyVectorShares +=
        CompleteAggregationPhaseRequestKt.shareData { shareVector += listOf(0, 0, 0, 1, 0, 0) }
    }

    val response = cryptor.completeReachAndFrequencyAggregationPhase(request)

    assertThat(response.reach).isEqualTo(4)
    assertThat(response.frequencyDistributionMap).containsExactly(1L, 0.25, 2L, 0.5, 3L, 0.25)
  }

  @Test
  fun `completeReachOnlyAggregationPhase with no noise returns exact reach`() {
    // Combined: [1, 0, 2, 2, 0, 3] -> 4 non-empty registers
    // reach = 4 / 1.0 = 4
    val request = completeAggregationPhaseRequest {
      frequencyVectorParams = shareShuffleFrequencyVectorParams {
        registerCount = 6
        maximumCombinedFrequency = 30
        ringModulus = 127
      }
      vidSamplingIntervalWidth = 1.0f
      noiseMechanism = NoiseMechanism.NONE
      frequencyVectorShares +=
        CompleteAggregationPhaseRequestKt.shareData { shareVector += listOf(1, 0, 2, 1, 0, 3) }
      frequencyVectorShares +=
        CompleteAggregationPhaseRequestKt.shareData { shareVector += listOf(0, 0, 0, 1, 0, 0) }
    }

    val response = cryptor.completeReachOnlyAggregationPhase(request)

    assertThat(response.reach).isEqualTo(4)
  }

  @Test
  fun `completeReachAndFrequencyAggregationPhase with vid sampling scales reach`() {
    // Same combined vector [1, 0, 2, 2, 0, 3], 4 non-empty registers
    // With vid_sampling_interval_width = 0.5: reach = 4 / 0.5 = 8
    // Frequency distribution is unchanged since it's a ratio
    val request = completeAggregationPhaseRequest {
      frequencyVectorParams = shareShuffleFrequencyVectorParams {
        registerCount = 6
        maximumCombinedFrequency = 30
        ringModulus = 127
      }
      maximumFrequency = 3
      vidSamplingIntervalWidth = 0.5f
      noiseMechanism = NoiseMechanism.NONE
      frequencyVectorShares +=
        CompleteAggregationPhaseRequestKt.shareData { shareVector += listOf(1, 0, 2, 1, 0, 3) }
      frequencyVectorShares +=
        CompleteAggregationPhaseRequestKt.shareData { shareVector += listOf(0, 0, 0, 1, 0, 0) }
    }

    val response = cryptor.completeReachAndFrequencyAggregationPhase(request)

    assertThat(response.reach).isEqualTo(8)
    assertThat(response.frequencyDistributionMap).containsExactly(1L, 0.25, 2L, 0.5, 3L, 0.25)
  }

  @Test
  fun `completeReachAndFrequencyAggregationPhase with all non-zero registers`() {
    // Share 1: [1, 2, 1, 2], Share 2: [0, 1, 0, 0]
    // Combined: [1, 3, 1, 2]
    // All registers non-zero -> non_empty = 4
    // Histogram (values < 3): {1: 2, 2: 1}, accumulated = 3
    // frequency_histogram[3+] = 4 - 3 = 1
    // adjusted_total = 4 - 0 = 4
    // reach = 4, distribution = {1: 0.5, 2: 0.25, 3: 0.25}
    val request = completeAggregationPhaseRequest {
      frequencyVectorParams = shareShuffleFrequencyVectorParams {
        registerCount = 4
        maximumCombinedFrequency = 30
        ringModulus = 127
      }
      maximumFrequency = 3
      vidSamplingIntervalWidth = 1.0f
      noiseMechanism = NoiseMechanism.NONE
      frequencyVectorShares +=
        CompleteAggregationPhaseRequestKt.shareData { shareVector += listOf(1, 2, 1, 2) }
      frequencyVectorShares +=
        CompleteAggregationPhaseRequestKt.shareData { shareVector += listOf(0, 1, 0, 0) }
    }

    val response = cryptor.completeReachAndFrequencyAggregationPhase(request)

    assertThat(response.reach).isEqualTo(4)
    assertThat(response.frequencyDistributionMap).containsExactly(1L, 0.5, 2L, 0.25, 3L, 0.25)
  }
}
