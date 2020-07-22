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

package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.SpannerOptions
import com.google.cloud.storage.StorageOptions
import java.math.BigInteger
import java.time.Clock
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.db.duchy.SketchAggregationComputationManager
import org.wfanet.measurement.db.duchy.SketchAggregationStageDetails
import org.wfanet.measurement.db.duchy.SketchAggregationStages

typealias GoogleCloudStorageOptions = StorageOptions
typealias GoogleCloudSpannerOptions = SpannerOptions

/**
 * Constructs a ComputationManager specific to combining Cascading Legions Cardinality Estimator
 * sketches implemented to run in Google Cloud Platform.
 *
 * @param duchyName name of the duchy running the computation manager
 * @param duchyPublicKeys mapping of the name of each duchy to its ECC El Gamal public key
 * @param spannerOptions Options specific to connecting with Google Cloud Spanner Instance
 * @param spannerDatabaseId id of the spanner database (project, instance, database name)
 * @param googleCloudStorageOptions Options specific to connecting with Google Cloud Storage
 * @param storageBucket name of the Google Cloud Storage bucket where computation stage input and
 *     output blobs are written.
 */
fun newCascadingLegionsSketchAggregationGcpComputationManager(
  duchyName: String,
  duchyPublicKeys: Map<String, BigInteger>,
  databaseClient: DatabaseClient,
  googleCloudStorageOptions: GoogleCloudStorageOptions =
    GoogleCloudStorageOptions.getDefaultInstance(),
  storageBucket: String,
  clock: Clock = Clock.systemUTC()
): SketchAggregationComputationManager {
  val duchyOrder = DuchyOrder(duchyPublicKeys.map { Duchy(it.key, it.value) }.toSet())
  val stageDetailsUtil = SketchAggregationStageDetails(
    otherDuchies = duchyPublicKeys.keys.filter { it != duchyName }
  )
  return SketchAggregationComputationManager(
    GcpSpannerComputationsDb(
      databaseClient = databaseClient,
      duchyName = duchyName,
      duchyOrder = duchyOrder,
      computationMutations = ComputationMutations(SketchAggregationStages, stageDetailsUtil),
      blobStorageBucket = storageBucket,
      clock = clock
    ),
    GcpStorageComputationsDb(
      googleCloudStorageOptions.service,
      bucket = storageBucket
    ),
    duchyPublicKeys.size
  )
}
