// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.secure_computation.tee_sdk.cloud_storage.v1alpha

import kotlin.jvm.JvmStatic
import java.io.InputStream

abstract class CloudStorage {

    abstract fun getData(bucketName: String, objectName: String): InputStream

    abstract fun startMultipartUpload(bucketName: String, objectName: String): String

    abstract fun uploadPart(byteArray: ByteArray, uploadId: String)

    abstract fun completeMultipart(uploadId: String)

}
