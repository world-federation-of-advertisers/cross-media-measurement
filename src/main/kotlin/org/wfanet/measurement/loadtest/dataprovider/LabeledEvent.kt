package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.Message
import java.time.Instant

/** An event [message] with [timestamp] and [vid] labels. */
data class LabeledEvent<T : Message>(val timestamp: Instant, val vid: Long, val message: T)
