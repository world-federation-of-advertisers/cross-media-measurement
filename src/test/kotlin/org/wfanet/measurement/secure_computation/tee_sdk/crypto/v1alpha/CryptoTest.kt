package org.wfanet.measurement.secure_computation.tee_sdk.crypto.v1alpha

import org.wfanet.measurement.secure_computation.tee_sdk.crypto.v1alpha.Crypto
import com.google.cloud.kms.v1.KeyManagementServiceClient
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.rules.ExpectedException
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.junit.Rule
import java.util.Base64
import javax.crypto.IllegalBlockSizeException
import javax.crypto.spec.SecretKeySpec
import com.google.cloud.kms.v1.DecryptRequest
import com.google.cloud.kms.v1.DecryptResponse
import com.google.protobuf.ByteString

@RunWith(JUnit4::class)
class CryptoTest {

    private lateinit var crypto: Crypto
    private lateinit var keyManagementServiceClient: KeyManagementServiceClient

    @Before
    fun setUp() {
        crypto = Mockito.spy(Crypto())
        keyManagementServiceClient = mock(KeyManagementServiceClient::class.java)
        Mockito.doReturn(keyManagementServiceClient).`when`(crypto).getKeyManagementServiceClient()
    }

    @Test
    fun test_encryptData_SuccessfulEncryption() {
        val data = "Hello World".toByteArray()
        val symmetricKey = Base64.getEncoder().encodeToString("1234567890123456".toByteArray())
        val encryptedData = crypto.encryptData(data, symmetricKey)
        assertNotNull(encryptedData)
        assertEquals(16, encryptedData.size)
    }

    @Test(expected = java.security.InvalidKeyException::class)
    fun test_encryptData_InvalidSymmetricKey() {
        val data = "Hello World".toByteArray()
        val invalidSymmetricKey = "invalidKey"
        crypto.encryptData(data, invalidSymmetricKey)
    }

    @Test
    fun test_encryptData_EmptyDataArray() {
        val data = "".toByteArray()
        val symmetricKey = Base64.getEncoder().encodeToString("1234567890123456".toByteArray()) 
        val encryptedData = crypto.encryptData(data, symmetricKey)
        assertNotNull(encryptedData)
        assertEquals(16, encryptedData.size)
    }

    @Test(expected = IllegalArgumentException::class)
    fun test_encryptData_EmptySymmetricKey() {
        val data = "Hello World".toByteArray()
        crypto.encryptData(data, "")
    }

    @Test
    fun test_decryptData_SuccessfulDecryption() {
        val originalMessage = "Hello World".toByteArray()
        val symmetricKey = Base64.getEncoder().encodeToString("1234567890123456".toByteArray()) 
        val encryptedData = crypto.encryptData(originalMessage, symmetricKey)
        val decryptedData = crypto.decryptData(encryptedData, symmetricKey)
        assertArrayEquals(originalMessage, decryptedData)
    }

    @Test(expected = java.security.InvalidKeyException::class)
    fun test_decryptData_InvalidBase64Key() {
        val encryptedData = "encryptedMessage".toByteArray()
        val invalidBase64Key = "invalidKey"
        crypto.decryptData(encryptedData, invalidBase64Key)
    }

    @Test
    fun test_decryptData_EmptyEncryptedData() {
        val encryptedData = ByteArray(0)
        val symmetricKey = Base64.getEncoder().encodeToString("1234567890123456".toByteArray()) 
        val result = crypto.decryptData(encryptedData, symmetricKey)
        assertTrue(result.isEmpty())
    }

    @Test
    fun test_decryptDEK_SuccessfulDecryption() {
        val encryptedDEK = "VGhpcyBpcyBhIHRlc3QgZW5jcnlwdGVkIGRhdGE=".toByteArray()
        val expectedPlaintext = "This is a test decrypted data".toByteArray()
        val response = DecryptResponse.newBuilder()
            .setPlaintext(ByteString.copyFrom(expectedPlaintext))
            .build()
        `when`(keyManagementServiceClient.decrypt(Mockito.any(DecryptRequest::class.java))).thenReturn(response)
        val decryptedData = crypto.decryptDEK(encryptedDEK, "validKeyName")
        assertArrayEquals(expectedPlaintext, decryptedData)
    }

    @Test(expected = IllegalArgumentException::class)
    fun test_decryptDEK_InvalidKeyName() {
        val encryptedDEK = "VGhpcyBpcyBhIHRlc3QgZW5jcnlwdGVkIGRhdGE=".toByteArray()
        `when`(keyManagementServiceClient.decrypt(Mockito.any(DecryptRequest::class.java)))
            .thenThrow(IllegalArgumentException("Invalid key name"))
        crypto.decryptDEK(encryptedDEK, "invalidKeyName")
    }

    @Test(expected = Exception::class)
    fun test_decryptDEK_CorruptedEncryptedDEK() {
        val encryptedDEK = "corrupted-encrypted-data".toByteArray()
        `when`(keyManagementServiceClient.decrypt(Mockito.any(DecryptRequest::class.java)))
            .thenThrow(Exception("Decryption failed due to corrupted data"))
        crypto.decryptDEK(encryptedDEK, "validKeyName")
    }

    @Test(expected = NullPointerException::class)
    fun test_decryptDEK_EmptyKeyName() {
        val encryptedDEK = "VGhpcyBpcyBhIHRlc3QgZW5jcnlwdGVkIGRhdGE=".toByteArray()
        crypto.decryptDEK(encryptedDEK, "")
    }
}
