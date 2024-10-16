package org.wfanet.measurement.secure_computation.tee_sdk.cloud_storage.v1alpha

import kotlin.jvm.JvmStatic
import java.io.InputStream

abstract class CloudStorage {

    abstract fun getData(bucketName: String, objectName: String): InputStream

    abstract fun startMultipartUpload(bucketName: String, objectName: String): String

    abstract fun uploadPart(byteArray: ByteArray, uploadId: String)

    abstract fun completeMultipart(uploadId: String)

}
