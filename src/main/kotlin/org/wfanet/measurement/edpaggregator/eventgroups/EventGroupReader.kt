// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.eventgroups

import com.google.gson.JsonParser
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import com.google.protobuf.util.JsonFormat
import java.io.InputStreamReader
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.MesosRecordIoStorageClient

/** Reads EventGroups from the JSON and Mesos RecordIO formats accepted by EDPA. */
class EventGroupReader(private val storageClient: ConditionalOperationStorageClient) {
  /** Thrown when an EventGroup file is well-formed but contains no EventGroups. */
  class EmptyEventGroupFileException :
    IllegalStateException("Uploads with zero event groups are not supported.")

  /**
   * Reads EventGroups from [blobKey].
   *
   * JSON files must contain an `event_groups` or `eventGroups` array. Binary protobuf files must
   * use Mesos RecordIO framing and the `.binpb` extension.
   *
   * @throws IllegalStateException if the format is unsupported or the blob is missing
   * @throws EmptyEventGroupFileException if the file contains no EventGroups
   */
  fun read(blobKey: String): Flow<EventGroup> {
    return when {
      blobKey.endsWith(PROTO_FILE_SUFFIX) -> readProto(blobKey)
      blobKey.endsWith(JSON_FILE_SUFFIX) -> readJson(blobKey)
      else -> error("Unsupported EventGroup file format: $blobKey")
    }
  }

  private fun readProto(blobKey: String): Flow<EventGroup> = flow {
    val blob =
      MesosRecordIoStorageClient(storageClient).getBlob(blobKey)
        ?: error("Blob not found for key: $blobKey")
    var count = 0
    blob.read().collect { bytes ->
      emit(EventGroup.parseFrom(bytes))
      count++
    }
    if (count == 0) {
      throw EmptyEventGroupFileException()
    }
  }

  private fun readJson(blobKey: String): Flow<EventGroup> = flow {
    val blob = storageClient.getBlob(blobKey) ?: error("Blob not found for key: $blobKey")
    val parser = JsonFormat.parser()
    var count = 0
    val jsonInput = InputStreamReader(blob.read().flatten().newInput(), Charsets.UTF_8)
    JsonReader(jsonInput).use { jsonReader ->
      jsonReader.beginObject()
      while (jsonReader.hasNext()) {
        when (jsonReader.nextName()) {
          EVENT_GROUPS_FIELD_NAME,
          EVENT_GROUPS_FIELD_JSON_NAME -> {
            jsonReader.beginArray()
            while (jsonReader.hasNext()) {
              val element = JsonParser.parseReader(jsonReader)
              val builder = EventGroup.newBuilder()
              parser.merge(element.toString(), builder)
              emit(builder.build())
              count++
            }
            jsonReader.endArray()
          }
          else -> jsonReader.skipValue()
        }
      }
      jsonReader.endObject()
      check(jsonReader.peek() == JsonToken.END_DOCUMENT) {
        "Unexpected trailing content after EventGroup JSON wrapper"
      }
    }
    if (count == 0) {
      throw EmptyEventGroupFileException()
    }
  }

  private companion object {
    const val PROTO_FILE_SUFFIX = ".binpb"
    const val JSON_FILE_SUFFIX = ".json"
    const val EVENT_GROUPS_FIELD_NAME = "event_groups"
    const val EVENT_GROUPS_FIELD_JSON_NAME = "eventGroups"
  }
}
