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

package org.wfanet.panelmatch.client.storage.aws.s3

import org.wfanet.measurement.aws.s3.S3StorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.withPrefix
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

/** [StorageFactory] for [S3StorageClient]. */
class S3StorageFactory(
  private val storageDetails: StorageDetails,
  private val exchangeDateKey: ExchangeDateKey
) : StorageFactory {

  override fun build(): StorageClient {
    return S3StorageClient(
        S3Client.builder().region(Region.of(storageDetails.aws.region)).build(),
        storageDetails.aws.bucket
      )
      .withPrefix(exchangeDateKey.path)
  }
}
