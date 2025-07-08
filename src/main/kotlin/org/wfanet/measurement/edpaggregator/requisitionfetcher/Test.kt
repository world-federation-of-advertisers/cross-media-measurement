package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.cloud.storage.StorageOptions
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient

class Test {

  companion object {
    @JvmStatic
    fun main(args: Array<String>) {
      runBlocking {
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
        println("~~~~~~~~~~~~~~~~~~ writing file")
        storageClient.writeBlob("test-file", byteString)
        println("~~~~~~~~~~~~~~~~~~ file written")
      }
    }
  }

}
