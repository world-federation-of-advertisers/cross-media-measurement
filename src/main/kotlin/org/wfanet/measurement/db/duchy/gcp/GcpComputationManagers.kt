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
