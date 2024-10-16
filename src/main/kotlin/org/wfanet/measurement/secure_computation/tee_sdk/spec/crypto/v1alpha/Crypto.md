# CLASS
## Crypto
* This class is responsible for encrypting and decrypting data using a symmetric key and decrypting a Data Encryption Key (DEK) using Google Cloud Key Management Service (KMS).
* Utilizes Java Cryptography Architecture (JCA) with the AES algorithm.
* Integrates Google Cloud KMS for DEK decryption, using default Google libraries for Kotlin or Java.

# CONSTRUCTORS
## INITIALIZE `()`
### USAGE
* Initialized KeyManagementServiceClient and transformation
### IMPL
* Initialized keyManagementServiceClient: KeyManagementServiceClient
* Initialize transformation: String = "AES/ECB/PKCS5Padding"
*
# METHODS
## `encryptData(data: ByteArray, symmetricKey: String) -> ByteArray`
### USAGE
* `data`: ByteArray to be encrypted.
* `symmetricKey`: A base64 encoded string representing the symmetric Data Encryption Key (DEK).
* Use this method to encrypt data using the specified symmetric key.
### IMPL
* Decode the base64 symmetric key to its byte representation.
* Initialize AES encryption with the decoded key.
* Encrypt the provided data byte array.
* Return the encrypted data as a byte array.
* Handle any exceptions, print them to the console, and re-raise them.

## `decryptData(encryptedData: ByteArray, symmetricKey: String) -> ByteArray`
### USAGE
* `encryptedData`: ByteArray representing encrypted data to be decrypted.
* `symmetricKey`: A base64 encoded string of the symmetric Data Encryption Key (DEK).
* Use this method to decrypt data using the specified symmetric key.
### IMPL
* Decode the base64 symmetric key to its byte representation.
* Initialize AES decryption with the decoded key.
* Decrypt the provided encrypted data byte array.
* Return the decrypted data as a byte array.
* Handle any exceptions, print them to the console, and re-raise them.

## `decryptDEK(encryptedDEK: ByteArray, keyName: String) -> ByteArray`
### USAGE
* `encryptedDEK`: ByteArray of the encrypted Data Encryption Key to be decrypted.
* `keyName`: String representing the name of the key in Google Cloud KMS used for decryption.
* Use this method to decrypt a DEK using Google Cloud KMS.
### IMPL
* Utilize Google Cloud KMS client to access the specified key using `keyName`.
* Decrypt the `encryptedDEK` using the KMS client.
* Return the decrypted DEK as a byte array.
* Handle any exceptions, print them to the console, and re-raise them.
