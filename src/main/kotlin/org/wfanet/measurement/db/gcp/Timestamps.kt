package org.wfanet.measurement.db.gcp

import com.google.cloud.Timestamp
import java.sql.Date
import java.time.Instant

/** Converts a [Timestamp] to milliseconds since the epoch. */
fun Timestamp.toMillis(): Long = toSqlTimestamp().time

/** Converts a long of milliseconds since the epoch to a [Timestamp]. */
fun Long.toGcpTimestamp(): Timestamp = Timestamp.of(Date.from(Instant.ofEpochMilli(this)))
