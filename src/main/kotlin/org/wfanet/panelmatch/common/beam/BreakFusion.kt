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

package org.wfanet.panelmatch.common.beam

import java.util.UUID
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.Values
import org.apache.beam.sdk.values.PCollection

/**
 * Breaks Dataflow fusion by forcing materialization of a [PCollection].
 *
 * This operation can be inserted between operations that have significantly different data sizes or
 * processing requirements to prevent Dataflow from fusing those operations together. Without this,
 * a heavy-processing stage that immediately follows a light-processing stage may be forced to
 * compute on a single worker. By inserting a [BreakFusion], the results of the light-processing
 * stage are materialized, allowing the heavy-processing stage to run with a different number of
 * workers.
 *
 * Kept as a PTransform for Scala Compatibility
 */
class BreakFusion<T : Any?> : PTransform<PCollection<T>, PCollection<T>>() {

  override fun expand(input: PCollection<T>): PCollection<T> {
    return input
      .keyBy("KeyByUUID") { UUID.randomUUID().toString() }
      .apply("GBK", GroupByKey.create())
      .apply("Values", Values.create())
      .apply("Flatten.iterables", Flatten.iterables())
      .setCoder(input.coder)
  }
}
