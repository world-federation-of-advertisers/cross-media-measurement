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

package org.wfanet.panelmatch.client.storage.gcloud.gcs

import com.google.cloud.storage.StorageOptions
import com.google.protobuf.kotlin.toByteString
import java.security.MessageDigest
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetails.BucketType
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.withPrefix

class GcsStorageFactory(
  private val storageDetails: StorageDetails,
  private val exchangeDateKey: ExchangeDateKey,
) : StorageFactory {
  override fun build(): StorageClient {
    val gcs = storageDetails.gcs
    val bucketId: String =
      when (val bucketType: BucketType = gcs.bucketType) {
        BucketType.STATIC_BUCKET -> gcs.bucket
        BucketType.ROTATING_BUCKET -> {
          val storageDetailsFingerprint =
            fingerprint("${gcs.bucket}-${gcs.projectName}-${storageDetails.visibility.name}")
          val exchangeFingerprint = fingerprint(exchangeDateKey.path)
          // Maximum bucket size is 63 characters. 512 bits in hex is 32 characters, so we drop one.
          storageDetailsFingerprint + exchangeFingerprint.take(31)
        }
        BucketType.UNKNOWN_TYPE,
        BucketType.UNRECOGNIZED -> error("Invalid bucket_type: $bucketType")
      }
    return GcsStorageClient(
        StorageOptions.newBuilder().setProjectId(gcs.projectName).build().service,
        bucketId,
      )
      .withPrefix(exchangeDateKey.path)
  }
}

private fun fingerprint(data: String): String {
  val digest = MessageDigest.getInstance("SHA-512").digest(data.toByteArray(Charsets.UTF_8))
  return HexString(digest.toByteString()).value.lowercase()
}
