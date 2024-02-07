// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.tools

import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.integration.awskms.AwsKmsClient
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import java.io.File
import org.wfanet.measurement.aws.s3.S3StorageClient
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.storage.FileSystemStorageFactory
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.aws.s3.S3StorageFactory
import org.wfanet.panelmatch.client.storage.gcloud.gcs.GcsStorageFactory
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.storage.StorageFactory
import picocli.CommandLine
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

// TODO: Add flags to support other storage clients
class CustomStorageFlags {

  @CommandLine.Option(
    names = ["--storage-type"],
    description = ["Type of destination storage: \${COMPLETION-CANDIDATES}"],
    required = true,
  )
  lateinit var storageType: StorageDetails.PlatformCase
    private set

  @CommandLine.Option(
    names = ["--private-storage-root"],
    description = ["Private storage root directory"],
  )
  private lateinit var privateStorageRoot: File

  @CommandLine.Option(
    names = ["--s3-storage-bucket"],
    description = ["The name of the s3 bucket used for default private storage."],
  )
  lateinit var s3Bucket: String
    private set

  @CommandLine.Option(
    names = ["--s3-region"],
    description = ["The region the s3 bucket is located in."],
  )
  lateinit var s3Region: String
    private set

  @CommandLine.Option(names = ["--tink-key-uri"], description = ["URI for tink"], required = true)
  private lateinit var tinkKeyUri: String

  @CommandLine.Mixin private lateinit var gcsFlags: GcsFromFlags.Flags

  private val rootStorageClient: StorageClient by lazy {
    when (storageType) {
      StorageDetails.PlatformCase.GCS -> GcsStorageClient.fromFlags(GcsFromFlags(gcsFlags))
      StorageDetails.PlatformCase.AWS ->
        S3StorageClient(S3AsyncClient.builder().region(Region.of(s3Region)).build(), s3Bucket)
      StorageDetails.PlatformCase.FILE -> {
        require(privateStorageRoot.exists() && privateStorageRoot.isDirectory)
        FileSystemStorageClient(privateStorageRoot)
      }
      else -> throw IllegalArgumentException("Unsupported storage type")
    }
  }

  private val defaults by lazy {
    val kmsClient: KmsClient =
      when (storageType) {
        StorageDetails.PlatformCase.GCS ->
          // Register GcpKmsClient before setting storage folders.
          GcpKmsClient()
        StorageDetails.PlatformCase.AWS ->
          // Register AwsKmsClient before setting storage folders.
          AwsKmsClient()
        else -> throw IllegalArgumentException("Unsupported default private storage type.")
      }
    DaemonStorageClientDefaults(rootStorageClient, tinkKeyUri, TinkKeyStorageProvider(kmsClient))
  }

  val addResource by lazy { ConfigureResource(defaults) }

  /** This should be customized per deployment. */
  val privateStorageFactories:
    Map<StorageDetails.PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory> by lazy {
    when (storageType) {
      StorageDetails.PlatformCase.GCS ->
        mapOf(StorageDetails.PlatformCase.GCS to ::GcsStorageFactory)
      StorageDetails.PlatformCase.FILE ->
        mapOf(StorageDetails.PlatformCase.FILE to ::FileSystemStorageFactory)
      StorageDetails.PlatformCase.AWS ->
        mapOf(StorageDetails.PlatformCase.AWS to ::S3StorageFactory)
      else -> throw IllegalArgumentException("Unsupported storage type")
    }
  }
}
