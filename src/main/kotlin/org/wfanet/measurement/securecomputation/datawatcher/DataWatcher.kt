package org.wfanet.measurement.securecomputation

import com.google.cloud.functions.CloudEventsFunction
import org.wfanet.measurement.securecomputation.DataWatcherConfig
import com.google.protobuf.InvalidProtocolBufferException
import com.google.events.cloud.storage.v1.StorageObjectData
import io.cloudevents.CloudEvent
import java.util.logging.Logger

class DataWatcher(val dataWatcherConfigs: List<DataWatcherConfig>) : CloudEventsFunction {
    override fun accept(event: CloudEvent) {
        try {
            val storageData = StorageObjectData.parseFrom(event.data!!.toBytes())
            val blobKey = storageData.name
            val bucket = storageData.bucket
            val blobUrl = "gs://\$bucket/\$blobKey"
    
            dataWatcherConfigs.forEach { config ->
                val regex = config.sourcePathRegex.toRegex()
                if (regex.matches(blobUrl)) {
                    config.sinkConfig.queue?.let {
                        println(it.queueName)
                    }
                }
            }
        } catch (e: InvalidProtocolBufferException) {
            Logger.getLogger(DataWatcher::class.java.name).severe("Failed to parse StorageObjectData: \${e.message}")
        }
    }
}
