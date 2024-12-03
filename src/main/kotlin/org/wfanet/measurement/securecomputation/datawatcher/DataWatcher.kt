package org.wfanet.measurement.securecomputation

import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsService
import com.google.events.cloud.storage.v1.StorageObjectData
import org.wfanet.measurement.securecomputation.DataWatcherConfig.DiscoveredWork
import org.wfanet.measurement.securecomputation.DataWatcherConfig
import com.google.cloud.functions.CloudEventsFunction
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import java.nio.charset.StandardCharsets
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.common.pack
import com.google.protobuf.Any
import io.cloudevents.CloudEvent
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import java.util.UUID
import kotlinx.coroutines.runBlocking

class DataWatcher(
    private val workItemsService: GooglePubSubWorkItemsService,
    private val projectId: String,
    private val dataWatcherConfigs: List<DataWatcherConfig>
) : CloudEventsFunction {

    override fun accept(event: CloudEvent) {
        val cloudEventData = String(event.getData().toBytes(), StandardCharsets.UTF_8)
        val builder = StorageObjectData.newBuilder()
        JsonFormat.parser().merge(cloudEventData, builder)
        val data = builder.build()
        val bucket = data.getBucket()
        val blobKey = data.getName()
        val path = "gs://" + bucket + "/" + blobKey
        dataWatcherConfigs.forEach { config ->
            val regex = config.sourcePathRegex.toRegex()
            if (regex.matches(path)) {
                val queueConfig = config.queue
                val workItemId = UUID.randomUUID().toString()
                val workItemParams = DataWatcherConfig.DiscoveredWork.newBuilder()
                    .setType(queueConfig.appConfig.pack())
                    .setPath(path)
                    .build()
                    .pack()
                val workItem = WorkItem.newBuilder()
                    .setName("workItems/" + workItemId)
                    .setQueue(queueConfig.queueName)
                    .setWorkItemParams(workItemParams)
                    .build()
                val createWorkItemRequest = CreateWorkItemRequest.newBuilder()
                    .setWorkItemId(workItemId)
                    .setWorkItem(workItem)
                    .build()
                runBlocking {
                    workItemsService.createWorkItem(createWorkItemRequest)
                }
            }
        }
    }
}
