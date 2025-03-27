package org.wfanet.measurement.securecomputation.service.internal

import com.google.protobuf.Message

interface WorkItemsPublisher {
  suspend fun publishMessage(queueName: String, message: Message)
}
