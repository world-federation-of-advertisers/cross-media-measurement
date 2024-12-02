package com.google.cloud.functions

import com.google.cloud.functions.CloudEventsFunction
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import wfa.measurement.securecomputation.DataWatcherConfig
import io.cloudevents.CloudEvent
import com.google.events.cloud.storage.v1.StorageObjectData
import com.google.protobuf.Any
import java.util.UUID
import java.util.logging.Logger
import java.util.regex.Pattern

class DataWatcher(
  private val workItemsService: GooglePubSubWorkItemsService,
  private val topicId: String,
  private val dataWacherConfigs: List<DataWatcherConfig>
) : CloudEventsFunction {
  override fun accept(event: CloudEvent) {
    val data = event.data ?: return
    val storageObjectData = StorageObjectData.parseFrom(data.toBytes())
    val blobKey = storageObjectData.name
    val bucket = storageObjectData.bucket
    val blobUrl = "gs://\\${bucket}/\\${blobKey}"
    val logger = Logger.getLogger(DataWatcher::class.java.name)
    dataWacherConfigs.forEach { config ->
      val pattern = Pattern.compile(config.sourcePathRegex)
      if (pattern.matcher(blobUrl).matches()) {
        val workItemId = UUID.randomUUID().toString()
        val workItemParams = config.queue.appConfig
        val request = createTestRequest(workItemId, workItemParams, topicId)
        try {
          workItemsService.createWorkItem(request)
        } catch (e: Exception) {
          logger.severe("Failed to publish work item: \\${e.message}")
        }
      }
    }
  }
}