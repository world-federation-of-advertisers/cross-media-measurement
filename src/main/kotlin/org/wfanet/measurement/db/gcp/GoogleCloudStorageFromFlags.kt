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

package org.wfanet.measurement.db.gcp

import com.google.cloud.storage.StorageOptions
import picocli.CommandLine

/**
 * Flags used to interact with a single bucket in Google Cloud Storage.
 */
class GoogleCloudStorageFromFlags(
  flags: Flags
) {

  class Flags {
    @CommandLine.Option(
      names = ["--google-cloud-storage-project"],
      description = ["Name of the Google Cloud Storage project to use."],
      required = true
    )
    lateinit var projectName: String
      private set
    @CommandLine.Option(
      names = ["--google-cloud-storage-bucket"],
      description = ["Name of the Google Cloud Storage project to use."],
      required = true
    )
    lateinit var storageBucket: String
      private set
  }

  /** [StorageOptions] created from flag values. */
  val cloudStorageOptions: StorageOptions by lazy {
    StorageOptions.newBuilder()
      .setProjectId(flags.projectName)
      .build()
  }

  val bucket: String by lazy { flags.storageBucket }
}
