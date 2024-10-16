# CLASS
## GcsStorageClient
* The `GcsStorageClient` class is a concrete implementation of the `CloudStorage` abstract class, specifically designed to interact with Google Cloud Storage.
* It provides methods to retrieve data, manage multipart uploads, and complete upload sessions using Google Cloud's API.
* The class also uses a concurrent hash map to manage multipart upload sessions.

# METHODS
## `getData(bucketName: String, objectName: String) -> InputStream`
### USAGE
* `bucketName`: The name of the Google Cloud Storage bucket to access.
* `objectName`: The name of the object within the bucket to retrieve.
* This method returns an `InputStream` for reading data from the specified object.

### IMPL
#### Test Case: Successful retrieval of data from a valid bucket and object.
1. **Mock Setup**: 
   - Mock the `Storage` dependency to return a `Blob` when the `get` method is called with `bucketName` = "valid-bucket" and `objectName` = "valid-object".
   - Mock the `Blob` dependency to return a `ReadChannel` when the `reader` method is called.
   - Mock the `Channels` dependency to return an `InputStream` when the `newInputStream` method is called with the `ReadChannel`.

2. **Execution**: 
   - Call the `getData` method with `bucketName` = "valid-bucket" and `objectName` = "valid-object".

3. **Assertions**: 
   - Verify that the returned `InputStream` matches the mocked `InputStream`.

#### Test Case: Attempt to retrieve data from a non-existent bucket.
1. **Mock Setup**: 
   - Mock the `Storage` dependency to return `None` when the `get` method is called with `bucketName` = "non-existent-bucket".

2. **Execution**: 
   - Call the `getData` method with `bucketName` = "non-existent-bucket" and `objectName` = "any-object".

3. **Assertions**: 
   - Ensure that a `StorageException` is thrown.

#### Test Case: Attempt to retrieve data from a non-existent object in a valid bucket.
1. **Mock Setup**: 
   - Mock the `Storage` dependency to return `None` when the `get` method is called with `bucketName` = "valid-bucket" and `objectName` = "non-existent-object".

2. **Execution**: 
   - Call the `getData` method with `bucketName` = "valid-bucket" and `objectName` = "non-existent-object".

3. **Assertions**: 
   - Ensure that a `StorageException` is thrown.

#### Test Case: Error during data retrieval process.
1. **Mock Setup**: 
   - Mock the `Storage` dependency to throw a `RuntimeException` when the `get` method is called with `bucketName` = "error-bucket" and `objectName` = "error-object".

2. **Execution**: 
   - Call the `getData` method with `bucketName` = "error-bucket" and `objectName` = "error-object".

3. **Assertions**: 
   - Ensure that a `RuntimeException` is thrown.

## `startMultipartUpload(bucketName: String, objectName: String) -> String`
### USAGE
* `bucketName`: The name of the bucket where the upload will occur.
* `objectName`: The name of the object to be uploaded.
* This method initiates a multipart upload and returns an upload ID.

### IMPL
#### Test Case: Successful start of multipart upload with valid bucket and object names.
1. **Mock Setup**: 
   - Mock the `Storage` dependency to return a `WritableByteChannel` when the `writer` method is called with valid `bucketName` and `objectName`.
   - Mock the `UUID` dependency to return a specific UUID.

2. **Execution**: 
   - Call `startMultipartUpload` with `bucketName` = "valid-bucket" and `objectName` = "valid-object".

3. **Assertions**: 
   - Verify that the returned upload ID matches the mocked UUID.
   - Check if the `multipartUploads` map contains the mocked UUID as a key.
   - Ensure the associated value in the map is the mocked `WritableByteChannel`.

#### Test Case: Test with invalid bucket name.
1. **Mock Setup**: 
   - Mock the `Storage` dependency to throw a `StorageException` when the `writer` method is called with `bucketName` = "invalid-bucket".

2. **Execution**: 
   - Call `startMultipartUpload` with `bucketName` = "invalid-bucket" and `objectName` = "valid-object".

3. **Assertions**: 
   - Ensure that a `StorageException` is thrown.

#### Test Case: Test with invalid object name.
1. **Mock Setup**: 
   - Mock the `Storage` dependency to throw a `StorageException` when the `writer` method is called with `objectName` = "invalid-object".

2. **Execution**: 
   - Call `startMultipartUpload` with `bucketName` = "valid-bucket" and `objectName` = "invalid-object".

3. **Assertions**: 
   - Ensure that a `StorageException` is thrown.

#### Test Case: Test with empty bucket name.
1. **Mock Setup**: 
   - Mock the `Storage` dependency to throw a `StorageException` when the `writer` method is called with an empty `bucketName`.

2. **Execution**: 
   - Call `startMultipartUpload` with `bucketName` = "" and `objectName` = "valid-object".

3. **Assertions**: 
   - Ensure that a `StorageException` is thrown.

#### Test Case: Test with empty object name.
1. **Mock Setup**: 
   - Mock the `Storage` dependency to throw a `StorageException` when the `writer` method is called with an empty `objectName`.

2. **Execution**: 
   - Call `startMultipartUpload` with `bucketName` = "valid-bucket" and `objectName` = "".

3. **Assertions**: 
   - Ensure that a `StorageException` is thrown.

## `uploadPart(byteArray: ByteArray, uploadId: String) -> void`
### USAGE
* `byteArray`: The data to upload as a part of the multipart upload.
* `uploadId`: The identifier of the multipart upload session.

### IMPL
#### Test Case: Successfully upload a part with valid uploadId and byteArray.
1. **Mock Setup**: 
   - Mock the `multipartUploads` map to return a `WritableByteChannel` for a valid `uploadId`.
   - Mock the `WritableByteChannel` to simulate writing a byte array and return the correct number of bytes written.

2. **Execution**: 
   - Call `uploadPart` with `byteArray` representing "Hello World" and `uploadId` = "validUploadId".

3. **Assertions**: 
   - Verify that the `write` method on the `WritableByteChannel` was called and returned 11 (length of "Hello World").

#### Test Case: Fail to upload a part with invalid uploadId.
1. **Execution**: 
   - Call `uploadPart` with `byteArray` = "Hello World" and `uploadId` = "invalidUploadId".

2. **Assertions**: 
   - Ensure that no write operation occurs (as the channel is `null`).

#### Test Case: Successfully upload an empty byteArray with valid uploadId.
1. **Mock Setup**: 
   - Mock the `multipartUploads` map to return a `WritableByteChannel` for a valid `uploadId`.
   - Mock the `WritableByteChannel` to simulate writing an empty byte array and return 0.

2. **Execution**: 
   - Call `uploadPart` with an empty `byteArray` and `uploadId` = "validUploadId".

3. **Assertions**: 
   - Verify that the `write` method on the `WritableByteChannel` was called and returned 0.

#### Test Case: Upload with null byteArray should fail.
1. **Execution**: 
   - Call `uploadPart` with `byteArray` = `null` and `uploadId` = "validUploadId".

2. **Assertions**: 
   - Ensure that a `NullPointerException` is thrown.

#### Test Case: Upload with null uploadId should fail.
1. **Execution**: 
   - Call `uploadPart` with `byteArray` = "Hello World" and `uploadId` = `null`.

2. **Assertions**: 
   - Ensure that no write operation occurs (as the channel is `null`).

## `completeMultipart(uploadId: String) -> void`
### USAGE
* `uploadId`: The identifier of the multipart upload session to complete.

### IMPL
#### Test Case: Successful completion of a multipart upload with a valid uploadId.
1. **Mock Setup**: 
   - Mock the `multipartUploads` map to contain a valid `uploadId` associated with a `WritableByteChannel`.
   - Mock the `WritableByteChannel` to simulate close operation.

2. **Execution**: 
   - Call `completeMultipart` with `uploadId` = "abc123".

3. **Assertions**: 
   - Verify that the `close` method on the `WritableByteChannel` was called.
   - Ensure the `multipartUploads` map no longer contains the `uploadId`.

#### Test Case: Attempt to complete a multipart upload with a non-existent uploadId.
1. **Mock Setup**: 
   - Ensure the `multipartUploads` map does not contain the `uploadId`.

2. **Execution**: 
   - Call `completeMultipart` with `uploadId` = "nonExistentId".

3. **Assertions**: 
   - Ensure the `multipartUploads` map does not contain the `uploadId`.

#### Test Case: Ensure no exception occurs when completing a multipart upload with a null uploadId.
1. **Mock Setup**: 
   - Mock the `multipartUploads` map to contain some active upload sessions.

2. **Execution**: 
   - Call `completeMultipart` with `uploadId` = `null`.

3. **Assertions**: 
   - Verify that the size of `multipartUploads` map remains unchanged.

#### Test Case: Complete multipart upload with an uploadId that is already closed.
1. **Mock Setup**: 
   - Mock the `multipartUploads` map to contain a `closed` `WritableByteChannel`.
   - Mock the `WritableByteChannel` to simulate close operation.

2. **Execution**: 
   - Call `completeMultipart` with `uploadId` = "alreadyClosedId".

3. **Assertions**: 
   - Verify that the `close` method was called without throwing an exception.
   - Ensure the `multipartUploads` map no longer contains the `uploadId`.