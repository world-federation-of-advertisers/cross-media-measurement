// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.loadtest

import com.google.protobuf.ByteString
import org.wfanet.anysketch.AnySketch
import org.wfanet.measurement.api.v1alpha.Sketch

/** Interface for E2E Correctness Test */
interface Correctness {

  /** Size of the unique reach set per campaign. */
  val setSize: Int

  /** Universe size to uniformly distribute numbers for reach set [0, universeSize). */
  val universeSize: Long

  /** Unique run id to log and specify output files. Use timestamp if not provided. */
  val runId: String

  /** Output directory to store sketches and estimates e.g. correctness/[runId]. */
  val outputDir: String

  /**
   * Generates a sequence of sets, each with [setSize] distinct values in [0, universeSize).
   * Each set is generated independently and may have non-empty intersections.
   *
   * @return Sequence of reach sets
   */
  fun generateReach(): Sequence<Set<Long>>

  /**
   * Creates an [AnySketch] object and calls insert() method with a set of reach given.
   * Returning [AnySketch] should have [setSize] number of registers.
   *
   * @param[reach] set of longs sized [setSize]
   * @return AnySketch object
   */
  fun generateSketch(reach: Set<Long>): AnySketch

  /** Encrypts the [Sketch] proto. */
  fun encryptSketch(sketch: Sketch): ByteString

  /**
   * Unions multiple [AnySketch] objects into one and runs Cardinality Estimation on it.
   *
   * @param[anySketches] List of AnySketch objects
   * @return Long value of Estimated Cardinality
   */
  fun estimateCardinality(anySketches: List<AnySketch>): Long

  /**
   * Stores raw Sketch proto into a local file and returns the path.
   *
   * @param[sketch] Sketch proto
   * @return String path of written file e.g. correctness/[runId]/sketches.txt
   */
  suspend fun storeSketch(sketch: Sketch): String

  /**
   * Stores encrypted Sketch proto into a local file and returns the path.
   *
   * @param[encryptedSketch] Encrypted Sketch proto in ByteString
   * @return String path of written file e.g. correctness/[runId]/encrypted_sketches.txt
   */
  suspend fun storeEncryptedSketch(encryptedSketch: ByteString): String

  /**
   * Stores estimation result into a local file and returns the path.
   *
   * @param[result] Long value of Estimated Cardinality
   * @return String path of written file e.g. correctness/[runId]/estimates.txt
   */
  suspend fun storeEstimationResult(result: Long): String

  /** Sends encrypted [Sketch] proto to Publisher Data Service. */
  suspend fun sendToServer(encryptedSketch: ByteString)

  /** Converts the [AnySketch] object into a [Sketch] proto. */
  fun AnySketch.toSketchProto(): Sketch
}
