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

package org.wfanet.measurement.edpaggregator.eventgroupactivities

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.time.Instant
import java.time.format.DateTimeParseException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * Parser for "spot" activity data in JSON form.
 *
 * The expected format is a JSON array of objects, each with:
 * - `parent`: the EventGroup resource name (String).
 * - `event_group_activity_date`: an ISO-8601 instant (String).
 */
object SpotDataParser {
  private const val PARENT_FIELD = "parent"
  private const val DATE_FIELD = "event_group_activity_date"

  /**
   * Parses a JSON array of activity records from [inputStream] into a streaming [Flow].
   *
   * The array is read incrementally with a streaming [JsonReader], so the whole file is never
   * materialized in memory at once; records are emitted one at a time as they are parsed.
   *
   * @return a [Flow] of the parsed records, in document order.
   * @throws IllegalArgumentException when the flow is collected, if the input is not a JSON array
   *   of the expected shape, is malformed/truncated, or any record is missing `parent` or
   *   `event_group_activity_date`, has a blank value for either, or has an unparseable date.
   */
  fun parseJson(inputStream: InputStream): Flow<SpotRecord> = flow {
    try {
      JsonReader(InputStreamReader(inputStream, Charsets.UTF_8)).use { reader ->
        reader.beginArray()
        var index = 0
        while (reader.hasNext()) {
          emit(parseRecord(reader, index))
          index++
        }
        reader.endArray()
      }
    } catch (e: IllegalArgumentException) {
      // Per-field validation failures are already clear; let them through unwrapped.
      throw e
    } catch (e: IOException) {
      throw IllegalArgumentException("Malformed spot-data JSON: ${e.message}", e)
    } catch (e: IllegalStateException) {
      // Gson surfaces wrong-typed/non-array tokens as IllegalStateException.
      throw IllegalArgumentException("Malformed spot-data JSON: ${e.message}", e)
    }
  }

  private fun parseRecord(reader: JsonReader, index: Int): SpotRecord {
    var parent: String? = null
    var dateString: String? = null
    reader.beginObject()
    while (reader.hasNext()) {
      when (reader.nextName()) {
        PARENT_FIELD -> parent = reader.nextStringStrict(index, PARENT_FIELD)
        DATE_FIELD -> dateString = reader.nextStringStrict(index, DATE_FIELD)
        else -> reader.skipValue()
      }
    }
    reader.endObject()

    require(!parent.isNullOrBlank()) { "Record at index $index is missing '$PARENT_FIELD'" }
    require(!dateString.isNullOrBlank()) { "Record at index $index is missing '$DATE_FIELD'" }
    val instant: Instant =
      try {
        Instant.parse(dateString)
      } catch (e: DateTimeParseException) {
        throw IllegalArgumentException(
          "Record at index $index has invalid '$DATE_FIELD': '$dateString'",
          e,
        )
      }
    return SpotRecord(parent = parent, eventGroupActivityDate = instant)
  }

  /**
   * Reads the next value as a String, requiring it to actually be a JSON string token (Gson would
   * otherwise silently coerce numbers/booleans). Throws [IllegalStateException] for the wrong type,
   * which [parseJson] surfaces as a clear [IllegalArgumentException].
   */
  private fun JsonReader.nextStringStrict(index: Int, field: String): String {
    check(peek() == JsonToken.STRING) {
      "Record at index $index has wrong type for '$field': expected a string"
    }
    return nextString()
  }
}
