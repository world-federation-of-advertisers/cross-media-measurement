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

package org.wfanet.measurement.secure_computation.tee_sdk.crypto.v1alpha

import com.google.cloud.kms.v1.KeyManagementServiceClient
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import com.google.cloud.kms.v1.DecryptResponse
import com.google.cloud.kms.v1.DecryptRequest
import com.google.protobuf.ByteString

class Crypto {
    private val transformation: String 

    init {
        transformation = "AES/ECB/PKCS5Padding"
    }

    fun getKeyManagementServiceClient(): KeyManagementServiceClient {
        return KeyManagementServiceClient.create()
    }

    fun encryptData(data: ByteArray, symmetricKey: String): ByteArray {
        try {
            val decodedKey = Base64.getDecoder().decode(symmetricKey)
            val secretKey = SecretKeySpec(decodedKey, "AES")
            val cipher = Cipher.getInstance(this.transformation)
            cipher.init(Cipher.ENCRYPT_MODE, secretKey)
            return cipher.doFinal(data)
        } catch (e: Exception) {
            println("Error during encryption: \\${e.message}")
            throw e
        }
    }

    fun decryptData(encryptedData: ByteArray, symmetricKey: String): ByteArray {
        try {
            val decodedKey = Base64.getDecoder().decode(symmetricKey)
            val secretKeySpec = SecretKeySpec(decodedKey, "AES")
            val cipher = Cipher.getInstance(this.transformation)
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec)
            return cipher.doFinal(encryptedData)
        } catch (e: Exception) {
            println(e.message)
            throw e
        }
    }

    fun decryptDEK(encryptedDEK: ByteArray, keyName: String): ByteArray {
        try {
            val keyManagementServiceClient = this.getKeyManagementServiceClient()
            val request = DecryptRequest.newBuilder()
                .setName(keyName)
                .setCiphertext(ByteString.copyFrom(encryptedDEK))
                .build()
            val response: DecryptResponse = keyManagementServiceClient.decrypt(request)
            return response.plaintext.toByteArray()
        } catch (e: Exception) {
            println("Error during decryption: \\${e.message}")
            throw e
        }
    }
}
