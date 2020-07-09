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
