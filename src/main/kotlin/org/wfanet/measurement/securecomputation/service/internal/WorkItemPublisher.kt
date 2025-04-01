package org.wfanet.measurement.securecomputation.service.internal

import com.google.protobuf.Message

interface WorkItemPublisher {
  suspend fun publishMessage(queueName: String, message: Message)
}
