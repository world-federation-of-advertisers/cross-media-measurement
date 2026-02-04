# org.wfanet.measurement.securecomputation.deploy.gcloud.deadletter

## Overview
This package provides infrastructure for handling failed work items in a distributed secure computation system. It monitors a Google PubSub dead letter queue for messages that failed processing after multiple retry attempts, then marks the corresponding work items as FAILED in the control plane database via the WorkItems API.

## Components

### DeadLetterQueueListener
Service that listens to a dead letter queue and marks failed work items as FAILED in the database.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| run | - | `suspend Unit` | Starts the listener by subscribing to the dead letter queue |
| close | - | `Unit` | Closes the queue subscriber connection |

#### Companion Object Methods

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| isAlreadyFailedError | `e: StatusRuntimeException` | `Boolean` | Checks if exception indicates work item is already in FAILED state |

#### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| subscriptionId | `String` | Subscription ID for the dead letter queue |
| queueSubscriber | `QueueSubscriber` | Client that manages connections and interactions with the queue |
| parser | `Parser<WorkItem>` | Parser for deserializing queue messages into WorkItem instances |
| workItemsStub | `WorkItemsGrpcKt.WorkItemsCoroutineStub` | gRPC stub for calling the WorkItems service |

## Key Functionality

### Message Processing Flow
1. **Subscription**: Subscribes to the configured dead letter queue subscription
2. **Message Reception**: Continuously receives messages from the queue
3. **Work Item Extraction**: Parses each message to extract the WorkItem
4. **Validation**: Skips messages with empty work item names
5. **API Call**: Invokes `failWorkItem` on the WorkItems API
6. **Acknowledgment**:
   - Acknowledges message if successfully processed
   - Acknowledges if work item not found (already cleaned up)
   - Acknowledges if work item already in FAILED state
   - Nacks message for retry on other errors

### Error Handling
- **Empty Work Item Names**: Acknowledged and skipped
- **NOT_FOUND**: Work item already removed, message acknowledged
- **FAILED_PRECONDITION** (already failed): Message acknowledged to prevent reprocessing
- **Other Errors**: Message nacked for redelivery

## Dependencies

- `com.google.protobuf` - Protocol buffer parsing for WorkItem deserialization
- `io.grpc` - gRPC communication with WorkItems service
- `kotlinx.coroutines.channels` - Asynchronous message channel consumption
- `org.wfanet.measurement.common.grpc` - gRPC utilities for error information extraction
- `org.wfanet.measurement.internal.securecomputation.controlplane` - WorkItems API client and request builders
- `org.wfanet.measurement.queue` - Queue subscriber abstraction for PubSub integration
- `org.wfanet.measurement.securecomputation.controlplane.v1alpha` - WorkItem protocol buffer definitions
- `org.wfanet.measurement.securecomputation.service` - Error code constants for state validation

## Usage Example

```kotlin
import com.google.protobuf.Parser
import org.wfanet.measurement.queue.gcloud.GCloudQueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt

// Initialize dependencies
val queueSubscriber = GCloudQueueSubscriber(projectId, credentialsProvider)
val workItemsStub = WorkItemsGrpcKt.WorkItemsCoroutineStub(channel)
val parser: Parser<WorkItem> = WorkItem.parser()

// Create and run listener
val listener = DeadLetterQueueListener(
    subscriptionId = "projects/my-project/subscriptions/work-items-dlq",
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsStub
)

try {
    listener.run() // Blocks and processes messages
} finally {
    listener.close()
}
```

## Class Diagram

```mermaid
classDiagram
    class DeadLetterQueueListener {
        -String subscriptionId
        -QueueSubscriber queueSubscriber
        -Parser~WorkItem~ parser
        -WorkItemsCoroutineStub workItemsStub
        +run() suspend Unit
        +close() Unit
        -receiveAndProcessMessages() suspend Unit
        -processMessage(QueueMessage) suspend Unit
    }

    class QueueSubscriber {
        <<interface>>
        +subscribe(String, Parser) ReceiveChannel
        +close() Unit
    }

    class WorkItemsCoroutineStub {
        +failWorkItem(FailWorkItemRequest) FailWorkItemResponse
    }

    class AutoCloseable {
        <<interface>>
        +close() Unit
    }

    DeadLetterQueueListener ..|> AutoCloseable
    DeadLetterQueueListener --> QueueSubscriber
    DeadLetterQueueListener --> WorkItemsCoroutineStub
    DeadLetterQueueListener --> "Parser~WorkItem~"
```
