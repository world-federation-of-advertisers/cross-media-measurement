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

package org.wfanet.measurement.gcloud.common

import com.google.cloud.Timestamp
import com.google.protobuf.Timestamp as ProtobufTimestamp
import java.time.Clock
import java.time.Instant

/** Converts this [Timestamp] to an [Instant]. */
fun Timestamp.toInstant(): Instant {
  return Instant.ofEpochSecond(seconds, nanos.toLong())
}

/**
 * Converts this [Timestamp] to the number of milliseconds since the epoch of
 * 1970-01-01T00:00:00Z.
 */
fun Timestamp.toEpochMilli(): Long {
  return toInstant().toEpochMilli()
}

/** Converts this [Instant] to a [Timestamp]. */
fun Instant.toGcloudTimestamp(): Timestamp {
  return Timestamp.ofTimeSecondsAndNanos(epochSecond, nano)
}

/** Converts this [ProtobufTimestamp] to a [Timestamp]. */
fun ProtobufTimestamp.toGcloudTimestamp(): Timestamp {
  return Timestamp.fromProto(this)
}

/** Returns the current instant as a [Timestamp]. */
fun Clock.gcloudTimestamp(): Timestamp {
  return instant().toGcloudTimestamp()
}
