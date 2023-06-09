// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.privatemembership

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFails
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.bucketIdOf
import org.wfanet.panelmatch.client.common.shardIdOf

@RunWith(JUnit4::class)
class BucketingTest {
  @Test
  fun rejectsInvalidParameters() {
    assertFails { Bucketing(numShards = 0, numBucketsPerShard = 5) }
    assertFails { Bucketing(numShards = 0, numBucketsPerShard = 5) }
    assertFails { Bucketing(numShards = 5, numBucketsPerShard = 0) }
    assertFails { Bucketing(numShards = -5, numBucketsPerShard = 10) }
    assertFails { Bucketing(numShards = 5, numBucketsPerShard = -10) }
  }

  @Test
  fun `normal usage`() {
    val bucketing = Bucketing(numShards = 7, numBucketsPerShard = 3)
    assertThat(bucketing.apply(0)).isEqualTo(shardIdOf(0) to bucketIdOf(0))
    assertThat(bucketing.apply(1)).isEqualTo(shardIdOf(1) to bucketIdOf(0))

    // 9 % 7 == 2, (9 / 7) % 3 == 1
    assertThat(bucketing.apply(9)).isEqualTo(shardIdOf(2) to bucketIdOf(1))

    // 1000 % 7 == 6, (1000 / 7) % 3 == 1
    assertThat(bucketing.apply(1000)).isEqualTo(shardIdOf(6) to bucketIdOf(1))
  }

  @Test
  fun `math is unsigned`() {
    // Note: we treat inputs to `Bucketing::apply` as unsigned. However, for parity with AnySketch,
    // we use primitives (i.e. long) instead of Guava's UnsignedLong. See:
    //
    // https://github.com/world-federation-of-advertisers/any-sketch-java/blob/main/src/main/java/org/wfanet/anysketch/fingerprinters/Fingerprinter.java#L26

    val bucketing = Bucketing(numShards = 7, numBucketsPerShard = 3)

    // -2^63 is 2^63 when interpreted as unsigned.
    // 2^63 % 7 == 1, (2^63 / 7) % 3 == 1
    assertThat(bucketing.apply(Long.MIN_VALUE)).isEqualTo(shardIdOf(1) to bucketIdOf(1))

    // -1L is 2^64-1 when interpreted as unsigned.
    // (2^64-1) % 7 == 1, ((2^64-1) / 7) % 3 == 2
    assertThat(bucketing.apply(-1L)).isEqualTo(shardIdOf(1) to bucketIdOf(2))
  }

  @Test
  fun overflow() {
    // This test is for a specific regression encountered. In particular, there was a bug where one
    // of the mathematical operations did not treat a value as unsigned. This test case reproduces
    // the bug to ensure it's not later re-introduced.

    val bucketing = Bucketing(numShards = 1, numBucketsPerShard = 10)

    // -4228343866523403918 is 14218400207186147698 unsigned
    assertThat(bucketing.apply(-4228343866523403918)).isEqualTo(shardIdOf(0) to bucketIdOf(8))
  }

  @Test
  fun `single shard`() {
    val bucketing = Bucketing(numShards = 1, numBucketsPerShard = 3)
    assertThat(bucketing.apply(0)).isEqualTo(shardIdOf(0) to bucketIdOf(0))
    assertThat(bucketing.apply(1)).isEqualTo(shardIdOf(0) to bucketIdOf(1))
    assertThat(bucketing.apply(2)).isEqualTo(shardIdOf(0) to bucketIdOf(2))
    assertThat(bucketing.apply(3)).isEqualTo(shardIdOf(0) to bucketIdOf(0))
    assertThat(bucketing.apply(10)).isEqualTo(shardIdOf(0) to bucketIdOf(1))
  }

  @Test
  fun `single bucket`() {
    val bucketing = Bucketing(numShards = 50, numBucketsPerShard = 1)
    assertThat(bucketing.apply(0)).isEqualTo(shardIdOf(0) to bucketIdOf(0))
    assertThat(bucketing.apply(1)).isEqualTo(shardIdOf(1) to bucketIdOf(0))
    assertThat(bucketing.apply(53)).isEqualTo(shardIdOf(3) to bucketIdOf(0))
  }
}
