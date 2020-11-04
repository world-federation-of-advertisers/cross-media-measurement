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
import org.wfanet.anysketch.SketchProtos
import org.wfanet.measurement.api.v1alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.internal.loadtest.TestResult
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.GlobalComputation

/** Interface for E2E Correctness Test */
interface Correctness {

  /** Number of data providers. */
  val dataProviderCount: Int

  /** Number of campaigns per Data Provider to generate reach and sketch for. */
  val campaignCount: Int

  /** Size of the unique reach set per campaign. */
  val generatedSetSize: Int

  /** Universe size to uniformly distribute numbers for reach set [0, universeSize). */
  val universeSize: Long

  /** Unique run id to log and specify output files. Use timestamp if not provided. */
  val runId: String

  /** [SketchConfig] with necessary parameters to generate [Sketch]. */
  val sketchConfig: SketchConfig

  /** Instance of [StorageClient] to store sketches, estimates, and test results. */
  val storageClient: StorageClient

  /** Instance of a [PublisherDataCoroutineStub] to send encrypted sketches to Publisher Data Service. */
  val publisherDataStub: PublisherDataCoroutineStub

  /**
   * Generates a sequence of sets, each with [setSize] distinct values in [0, universeSize).
   * Each set is generated independently and may have non-empty intersections.
   *
   * @return Sequence of reach sets
   */
  fun generateReach(): Sequence<Set<Long>>

  /**
   * Creates an [AnySketch] object and calls insert() method with a set of reach given.
   * Returning [AnySketch] should have at most [setSize] number of registers.
   *
   * @param reach set of longs sized [setSize]
   * @return AnySketch object
   */
  fun generateSketch(reach: Set<Long>): AnySketch

  /** Encrypts the [Sketch] proto. */
  fun encryptSketch(sketch: Sketch, combinedPublicKey: ElGamalPublicKey): ByteString

  /**
   * Runs Cardinality Estimation on the given [AnySketch].
   *
   * @param anySketch AnySketch object.
   * @return Long value of Estimated Cardinality
   */
  fun estimateCardinality(anySketch: AnySketch): Long

  /**
   * Runs Frequency Estimation on the given [AnySketch].
   *
   * @param anySketch AnySketch object.
   * @return Map<Long, Double> Value Histogram for Estimated Frequency
   */
  fun estimateFrequency(anySketch: AnySketch): Map<Long, Double>

  /**
   * Stores a binary-serialized [Sketch] message into a blob.
   *
   * @param Sketch proto
   * @return blob key of the stored [Sketch]
   */
  suspend fun storeSketch(sketch: Sketch): String

  /**
   * Stores an encrypted Sketch into a blob.
   *
   * @param encryptedSketch Encrypted Sketch proto in ByteString
   * @return blob key of the stored encryptedSketch
   */
  suspend fun storeEncryptedSketch(encryptedSketch: ByteString): String

  /**
   * Stores a binary-serialized [GlobalComputation] message with reach and frequency estimation results into a blob.
   *
   * @param reach Long value of Estimated Cardinality
   * @param frequency Map<Long, Double> value of Estimated Frequency
   * @return blob key of the stored [GlobalComputation]
   */
  suspend fun storeEstimationResults(
    reach: Long,
    frequency: Map<Long, Double>
  ): String

  /**
   * Stores a binary-serialized [TestResult] message with all the blob keys into a blob.
   *
   * @param TestResult proto
   * @return blob key of the stored [TestResult]
   */
  suspend fun storeTestResult(testResult: TestResult): String

  /** Sends encryptedSketch to Publisher Data Service. */
  suspend fun uploadMetricValue(
    metricValueKey: MetricRequisition.Key,
    encryptedSketch: ByteString
  )
}

fun AnySketch.toSketchProto(sketchConfig: SketchConfig): Sketch {
  return SketchProtos.fromAnySketch(this, sketchConfig)
}
