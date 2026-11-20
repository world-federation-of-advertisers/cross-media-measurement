/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.protobuf.ByteString
import java.security.MessageDigest
import java.time.Instant
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient

/** Durable timestamps for actual Kingdom publication, outside the labeled date folders. */
object DataAvailabilityPublicationStatus {
  fun blobKey(impressionPath: String, dataProvider: String, modelLine: String): String {
    val identity = "$dataProvider\u0000$modelLine".toByteArray(Charsets.UTF_8)
    val hash =
      MessageDigest.getInstance("SHA-256").digest(identity).joinToString("") { "%02x".format(it) }
    return listOf(impressionPath.trimEnd('/'), ".availability-publication", hash)
      .filter { it.isNotEmpty() }
      .joinToString("/")
  }

  /** Called under the provider synchronization lease after Kingdom publication succeeds. */
  suspend fun record(
    storageClient: StorageClient,
    impressionPath: String,
    dataProvider: String,
    modelLine: String,
    publishedAt: Instant,
  ) {
    storageClient.writeBlob(
      blobKey(impressionPath, dataProvider, modelLine),
      ByteString.copyFromUtf8(publishedAt.toString()),
    )
  }

  suspend fun read(
    storageClient: StorageClient,
    impressionPath: String,
    dataProvider: String,
    modelLine: String,
  ): Instant? {
    val blob =
      storageClient.getBlob(blobKey(impressionPath, dataProvider, modelLine)) ?: return null
    return Instant.parse(blob.read().flatten().toStringUtf8())
  }
}
