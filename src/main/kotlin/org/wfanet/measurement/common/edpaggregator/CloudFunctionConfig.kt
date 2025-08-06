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

import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import java.io.StringReader
import org.wfanet.measurement.common.parseTextProto

/** Function to get Cloud Functions' configurations from Storage */
object CloudFunctionConfig {

  private const val CONFIG_STORAGE_BUCKET_ENV = "EDPA_CONFIG_STORAGE_BUCKET"
  private const val GOOGLE_PROJECT_ID_ENV = "GOOGLE_PROJECT_ID"

  /**
   * Loads a UTF-8-encoded configuration proto from the storage backend specified by environment
   * variables.
   *
   * Environment variables consumed:
   * - EDPA_CONFIG_STORAGE_BUCKET: URI prefix where config blobs live. • gs://my-bucket/base-path
   *   for Google Cloud Storage • file:///absolute/local/path for local-filesystem testing This
   *   value must be set and must not end with a slash.
   * - GOOGLE_PROJECT_ID: (optional) GCP project ID to use for Cloud Storage.
   *
   * @param configBlobKey The name of the config blob to load.
   * @param defaultInstance A default instance of the protobuf message type to parse into.
   * @param typeRegistry Optional registry for handling `Any` fields during parsing.
   * @return A protobuf message of type [T] populated from the parsed config.
   * @throws IllegalArgumentException if required environment variables are not set.
   * @throws IllegalStateException if the config blob is not found.
   */
  suspend fun <T : Message> getConfig(
    configBlobKey: String,
    defaultInstance: T,
    typeRegistry: TypeRegistry? = null,
  ): T {
    val bucket = checkNotNull(System.getenv(CONFIG_STORAGE_BUCKET_ENV)) {
      "Environment variable EDPA_CONFIG_STORAGE_BUCKET must be set."
    }.removeSuffix("/")
    val projectId = System.getenv(GOOGLE_PROJECT_ID_ENV)
    val loader = BlobLoader()
    val bytes = loader.getBytes(bucket, configBlobKey, projectId)
    val text = bytes.toStringUtf8()
    return if (typeRegistry != null) {
      parseTextProto(StringReader(text), defaultInstance, typeRegistry)
    } else {
      parseTextProto(StringReader(text), defaultInstance)
    }
  }
}
