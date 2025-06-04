// Copyright 2025 The Cross-Media Measurement Authors
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


package org.wfanet.measurement.common.edpaggregator

import com.google.cloud.storage.StorageOptions
import com.google.protobuf.ByteString
import java.io.File
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

suspend fun getConfig(fileSystemEnvVar: String, configBlobKey: String): String {
  val fileSystemPath = System.getenv(fileSystemEnvVar)
  // 'FileSystemStorageClient' is used for testing purposes only
  // in order to pull config from local storage.
  val configStorageClient =
    if (!fileSystemPath.isNullOrEmpty()) {
      FileSystemStorageClient(File(EnvVars.checkIsPath(fileSystemEnvVar)))
    } else {
      val configGcsBucket = System.getenv("EDPA_CONFIG_STORAGE_BUCKET")
      val googleProjectId = checkNotNull(System.getenv("GOOGLE_PROJECT_ID"))
      GcsStorageClient(
        StorageOptions.newBuilder()
          .setProjectId(googleProjectId)
          .build()
          .service,
        configGcsBucket,
      )
    }
  val configBlob = checkNotNull(configStorageClient.getBlob(configBlobKey)) {
    "Configuration file ${configBlobKey} not found"
  }
  val configBlobByteString: ByteString = configBlob.read().flatten()
  return configBlobByteString.toStringUtf8()
}
