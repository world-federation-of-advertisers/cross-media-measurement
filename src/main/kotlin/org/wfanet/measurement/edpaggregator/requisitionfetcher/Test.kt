package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.cloud.storage.StorageOptions
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import java.nio.file.Files
import java.nio.file.Path
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient

class Test {

  fun main(args: Array<String>) {
    val storageClient = GcsStorageClient(
      StorageOptions.newBuilder()
        .also {
          it.setProjectId("halo-cmm-dev")
        }
        .build()
        .service,
      "secure-computation-storage-dev-bucket",
    )
    val tempFile: Path = Files.createTempFile("hello", ".txt")
    Files.writeString(tempFile, "hello world!")

    // Read back into ByteString
    val byteString = ByteString.readFrom(Files.newInputStream(tempFile))
    storageClient.writeBlob(blobKey, byteString)
  }

}
