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

package org.wfanet.panelmatch.client.storage.gcloud

import org.junit.After
import org.junit.ClassRule
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.testing.VerifiedStorageClientTest

class GcsVerifiedStorageClientTest : VerifiedStorageClientTest() {
  override val underlyingClient: StorageClient by lazy {
    storageEmulator.createBucket(BUCKET)
    GcsStorageClient(storageEmulator.storage, BUCKET)
  }

  @After
  fun deleteBucket() {
    storageEmulator.deleteBucketRecursive(BUCKET)
  }

  companion object {
    private const val BUCKET = "some-test-bucket"

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()
  }
}
