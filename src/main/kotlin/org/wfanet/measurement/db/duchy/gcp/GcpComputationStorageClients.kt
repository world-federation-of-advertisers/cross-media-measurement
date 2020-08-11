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

import com.google.cloud.storage.StorageOptions
import io.grpc.Channel
import org.wfanet.measurement.db.duchy.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub
import java.math.BigInteger

typealias GoogleCloudStorageOptions = StorageOptions

/**
 * Constructs [LiquidLegionsSketchAggregationComputationStorageClients] specific to combining
 * Liquid Legions Cardinality Estimator sketches implemented to run in Google Cloud Platform.
 *
 * @param duchyName name of the duchy running using the clients
 * @param duchyPublicKeys mapping of the name of each duchy to its ECC El Gamal public key
 * @param googleCloudStorageOptions Options specific to connecting with Google Cloud Storage
 * @param storageBucket name of the Google Cloud Storage bucket where computation stage input and
 *     output blobs are written.
 * @param computationStorageServiceChannel gRPC channel to communicate with the Computation Storage
 *  Service
 */
fun newLiquidLegionsSketchAggregationGcpComputationStorageClients(
  duchyName: String,
  duchyPublicKeys: Map<String, BigInteger>,
  googleCloudStorageOptions: GoogleCloudStorageOptions =
    GoogleCloudStorageOptions.getDefaultInstance(),
  storageBucket: String,
  computationStorageServiceChannel: Channel
): LiquidLegionsSketchAggregationComputationStorageClients {
  return LiquidLegionsSketchAggregationComputationStorageClients(
    ComputationStorageServiceCoroutineStub(computationStorageServiceChannel),
    GcpStorageComputationsDb(
      googleCloudStorageOptions.service,
      bucket = storageBucket
    ),
    duchyPublicKeys.keys.minus(duchyName).toList()
  )
}
