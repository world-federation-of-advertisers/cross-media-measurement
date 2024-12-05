// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation

import com.google.cloud.functions.CloudEventsFunction
import com.google.events.cloud.storage.v1.StorageObjectData
import com.google.protobuf.ByteString
import com.google.protobuf.util.JsonFormat
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import java.net.URI
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.storage.StorageClient
import com.google.protobuf.Timestamp

/** Used for local testing of Cloud Run function triggered by upload to Cloud Storage. */
class GcsSubscribingStorageClient(private val storageClient: StorageClient) : StorageClient {
  private var subscribingFunctions = mutableListOf<CloudEventsFunction>()

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val blob = storageClient.writeBlob(blobKey.removePrefix("gs://$FAKE_BUCKET/"), content)
    // Get the current time in milliseconds
    val millis = System.currentTimeMillis()

    // Create a Timestamp object
    val timestamp = Timestamp.newBuilder()
        .setSeconds(millis / 1000)
        .setNanos(((millis % 1000) * 1000000).toInt())
        .build()

    val dataBuilder = StorageObjectData.newBuilder()
        .setName(blobKey.removePrefix("gs://$FAKE_BUCKET/"))
        .setBucket(FAKE_BUCKET)
        .setMetageneration(10)
        .setTimeCreated(timestamp)
        .setUpdated(timestamp)

    val jsonData = JsonFormat.printer().print(dataBuilder)

    val event: CloudEvent =
      CloudEventBuilder.v1()
        .withId("some-id")
        .withSource(URI.create("some-uri"))
        .withType("google.storage.object.finalize")
        .withData("application/json", jsonData.toByteArray())
        .build()
    subscribingFunctions.forEach { subscribingFunction ->
      logger.fine { "Sending $blobKey to function $subscribingFunction" }
      subscribingFunction.accept(event)
    }
    return blob
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    return storageClient.getBlob(blobKey.removePrefix("gs://$FAKE_BUCKET/"))
  }

  fun subscribe(function: CloudEventsFunction) {
    subscribingFunctions.add(function)
  }

  companion object {
    internal val logger = Logger.getLogger(this::class.java.name)
    val FAKE_BUCKET = "fake-bucket"
  }
}
