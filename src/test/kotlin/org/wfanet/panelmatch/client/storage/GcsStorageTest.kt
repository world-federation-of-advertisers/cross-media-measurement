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

package org.wfanet.panelmatch.client.storage

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_FILE
import org.wfanet.measurement.common.crypto.testing.KEY_ALGORITHM
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.panelmatch.client.storage.testing.AbstractStorageTest

class GcsStorageTest : AbstractStorageTest() {

  override val privateStorage by lazy {
    VerifiedStorageClient(
      GcsStorageClient(LocalStorageHelper.getOptions().service, PRIVATE_BUCKET),
      readCertificate(FIXED_SERVER_CERT_PEM_FILE),
      readCertificate(FIXED_SERVER_CERT_PEM_FILE),
      readPrivateKey(FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)
    )
  }

  override val sharedStorage by lazy {
    VerifiedStorageClient(
      GcsStorageClient(LocalStorageHelper.getOptions().service, SHARED_BUCKET),
      readCertificate(FIXED_SERVER_CERT_PEM_FILE),
      readCertificate(FIXED_SERVER_CERT_PEM_FILE),
      readPrivateKey(FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)
    )
  }

  companion object {
    private const val PRIVATE_BUCKET = "private-test-bucket"
    private const val SHARED_BUCKET = "shared-test-bucket"
  }
}
