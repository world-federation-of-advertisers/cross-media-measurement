package org.wfanet.measurement.db.gcp

import com.google.cloud.Timestamp
import java.time.Clock
import java.time.Instant
import org.wfanet.measurement.common.toInstant

/** Converts a [Timestamp] to milliseconds since the epoch. */
fun Timestamp.toMillis(): Long = toSqlTimestamp().time

/**
 * Converts a [java.time.Instant] to a Spanner Timestamp.
 */
fun Instant.toGcpTimestamp(): Timestamp = Timestamp.ofTimeSecondsAndNanos(epochSecond, nano)

/**
 * Converts a protocol buffers Timestamp to a Spanner Timestamp.
 */
fun com.google.protobuf.Timestamp.toGcpTimestamp(): Timestamp = toInstant().toGcpTimestamp()

/** Get the current time of a [Clock] as a GCP [Timestamp]. */
fun Clock.gcpTimestamp(): Timestamp = Timestamp.ofTimeMicroseconds(millis() * 1000)
