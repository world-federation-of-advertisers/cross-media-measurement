# CLASS
## Crypto
* The `Crypto` class provides cryptographic functionalities including data encryption and decryption using symmetric keys and decryption of Data Encryption Keys (DEK) using Google Cloud's Key Management Service (KMS).
* Located in the `core.security.crypto` package.
* Utilizes Google Cloud's KMS for DEK decryption and Java's `Cipher` for symmetric encryption and decryption.

# METHODS
## `encryptData(data: ByteArray, symmetricKey: String) -> ByteArray`
### USAGE
* Encrypts the input `data` using the provided Base64 encoded `symmetricKey`.
* The method returns an encrypted `ByteArray`.
### IMPL
* **Successful encryption with valid data and symmetric key:**
  1. Initialize a `Crypto` instance.
  2. Convert the string "Hello World" to a `ByteArray`.
  3. Call `encryptData()` with the `ByteArray` and a valid Base64 encoded symmetric key.
  4. Assert that the returned encrypted data is not null.
  5. Assert that the length of the encrypted data is 16 bytes.

* **Encryption fails with invalid symmetric key:**
  1. Initialize a `Crypto` instance.
  2. Convert the string "Hello World" to a `ByteArray`.
  3. Call `encryptData()` with the `ByteArray` and an invalid symmetric key.
  4. Assert that an `IllegalArgumentException` is thrown.

* **Successful encryption with empty data array:**
  1. Initialize a `Crypto` instance.
  2. Convert an empty string to a `ByteArray`.
  3. Call `encryptData()` with the empty `ByteArray` and a valid Base64 encoded symmetric key.
  4. Assert that the returned encrypted data is not null.
  5. Assert that the length of the encrypted data is 16 bytes.

* **Encryption fails with null symmetric key:**
  1. Initialize a `Crypto` instance.
  2. Convert the string "Hello World" to a `ByteArray`.
  3. Call `encryptData()` with the `ByteArray` and a null symmetric key.
  4. Assert that an `IllegalArgumentException` is thrown.

* **Encryption fails with empty symmetric key:**
  1. Initialize a `Crypto` instance.
  2. Convert the string "Hello World" to a `ByteArray`.
  3. Call `encryptData()` with the `ByteArray` and an empty symmetric key.
  4. Assert that an `IllegalArgumentException` is thrown.

## `decryptData(encryptedData: ByteArray, symmetricKey: String) -> ByteArray`
### USAGE
* Decrypts the input `encryptedData` using the provided Base64 encoded `symmetricKey`.
* The method returns the original `ByteArray`.
### IMPL
* **Decrypts data successfully with valid inputs:**
  1. Initialize a `Crypto` instance.
  2. Use a `ByteArray` representing "encryptedMessage".
  3. Call `decryptData()` with the `ByteArray` and a valid Base64 encoded symmetric key.
  4. Assert that the returned decrypted data matches the original message "originalMessage".

* **Fails to decrypt with invalid Base64 key:**
  1. Initialize a `Crypto` instance.
  2. Use a `ByteArray` representing "encryptedMessage".
  3. Call `decryptData()` with the `ByteArray` and an invalid Base64 encoded key.
  4. Assert that an `IllegalArgumentException` is thrown.

* **Fails to decrypt with incorrect key size:**
  1. Initialize a `Crypto` instance.
  2. Use a `ByteArray` representing "encryptedMessage".
  3. Call `decryptData()` with the `ByteArray` and a Base64 encoded key with invalid size.
  4. Assert that an `InvalidKeyException` is thrown.

* **Fails to decrypt with empty encrypted data:**
  1. Initialize a `Crypto` instance.
  2. Use an empty `ByteArray`.
  3. Call `decryptData()` with the empty `ByteArray` and a valid Base64 encoded symmetric key.
  4. Assert that an `IllegalBlockSizeException` is thrown.

## `decryptDEK(encryptedDEK: ByteArray, keyName: String) -> ByteArray`
### USAGE
* Decrypts the input `encryptedDEK` using the specified `keyName` in Google Cloud KMS.
* The method returns the decrypted `ByteArray`.
### IMPL
* **Successful decryption of a valid DEK:**
  1. Initialize a `Crypto` instance with a mocked `KeyManagementServiceClient`.
  2. Prepare a `ByteArray` representing "VGhpcyBpcyBhIHRlc3QgZW5jcnlwdGVkIGRhdGE=".
  3. Mock the `decrypt` method of `KeyManagementServiceClient` to return a `DecryptResponse` with the plaintext "This is a test decrypted data".
  4. Call `decryptDEK()` with the `ByteArray` and a valid key name.
  5. Assert that the returned decrypted data matches "This is a test decrypted data".

* **Decryption with an invalid keyName:**
  1. Initialize a `Crypto` instance with a mocked `KeyManagementServiceClient`.
  2. Prepare a `ByteArray` representing "VGhpcyBpcyBhIHRlc3QgZW5jcnlwdGVkIGRhdGE=".
  3. Mock the `decrypt` method of `KeyManagementServiceClient` to throw an `InvalidArgumentException` with the message "Invalid key name".
  4. Call `decryptDEK()` with the `ByteArray` and an invalid key name.
  5. Assert that an `InvalidArgumentException` with the message "Invalid key name" is thrown.

* **Decryption with corrupted encryptedDEK:**
  1. Initialize a `Crypto` instance with a mocked `KeyManagementServiceClient`.
  2. Prepare a `ByteArray` representing "corrupted-encrypted-data".
  3. Mock the `decrypt` method of `KeyManagementServiceClient` to throw a `DecryptionException` with the message "Decryption failed due to corrupted data".
  4. Call `decryptDEK()` with the `ByteArray` and a valid key name.
  5. Assert that a `DecryptionException` with the message "Decryption failed due to corrupted data" is thrown.

* **Decryption with null encryptedDEK:**
  1. Initialize a `Crypto` instance.
  2. Call `decryptDEK()` with null `encryptedDEK` and a valid key name.
  3. Assert that a `NullPointerException` with the message "Encrypted DEK cannot be null" is thrown.

* **Decryption with empty keyName:**
  1. Initialize a `Crypto` instance.
  2. Prepare a `ByteArray` representing "VGhpcyBpcyBhIHRlc3QgZW5jcnlwdGVkIGRhdGE=".
  3. Call `decryptDEK()` with the `ByteArray` and an empty key name.
  4. Assert that an `IllegalArgumentException` with the message "Key name cannot be empty" is thrown.