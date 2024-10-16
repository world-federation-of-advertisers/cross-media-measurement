package org.wfanet.measurement.secure_computation.tee_sdk.cloud_storage.v1alpha

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import com.google.cloud.ReadChannel
import com.google.cloud.storage.StorageOptions
import com.google.cloud.storage.BlobInfo
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel
import java.nio.ByteBuffer
import java.io.InputStream
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class GcsStorageClient : CloudStorage() {
    companion object {
        val multipartUploads: ConcurrentHashMap<String, WritableByteChannel> = ConcurrentHashMap()
    }

    override fun getData(bucketName: String, objectName: String): InputStream {
        try {
            val storage: Storage = StorageOptions.getDefaultInstance().service
            val blob: Blob = storage.get(bucketName, objectName)
            val readChannel: ReadChannel = blob.reader()
            return Channels.newInputStream(readChannel)
        } catch (e: Exception) {
            println("An error occurred while accessing Google Cloud Storage: \${e.message}")
            throw e
        }
    }

    override fun startMultipartUpload(bucketName: String, objectName: String): String {
        val storage: Storage = StorageOptions.getDefaultInstance().service
        val uploadId = UUID.randomUUID().toString()
        val blobInfo = BlobInfo.newBuilder(bucketName, objectName).build()
        val writeChannel = storage.writer(blobInfo) as WritableByteChannel
        multipartUploads[uploadId] = writeChannel
        return uploadId
    }

    override fun uploadPart(byteArray: ByteArray, uploadId: String) {
        val writeChannel = multipartUploads[uploadId]
        writeChannel?.write(ByteBuffer.wrap(byteArray))
    }

    override fun completeMultipart(uploadId: String) {
        multipartUploads[uploadId]?.let {
            it.close()
            multipartUploads.remove(uploadId)
        }
    }
}
