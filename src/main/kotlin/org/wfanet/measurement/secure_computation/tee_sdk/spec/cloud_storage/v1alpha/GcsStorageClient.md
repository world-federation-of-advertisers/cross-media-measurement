# CLASS
## GcsStorageClient
* This class implements the `CloudStorage` abstract class to manage multipart uploads in Google Cloud Storage.
* It uses a static `HashMap` to associate multipart upload IDs with their corresponding `WriteChannel` instances.
* Package: `cloud.storage.management`
* ATTRIBUTES: *
* * `multipart_uploads` is a STATIC map that stores `upload_id` as string and `WriteChannel` objects.

# METHODS
## `getData(bucketName: String, objectName: String): InputStream`
### USAGE
* `bucketName`: The name of the bucket where the object will be read.
* `objectName`: The name of the object to be downloaded.
* Returns an `InputStream` that can be used to read byte from.
### IMPL
* Uses the google cloud libraries to fetch an object from Google Cloud Storage.
* The method returns an `InputStream` for the desired object key.
* Uses the `bucketName` and `ObjectName` to retrieve an `InputStream` that can be used to read bytes from the remote file.
* If the `bucketName` or `ObjectName` does not exist, raise an appropriate exception.
* Handle any exceptions from the Google Cloud libraries related to network issues by printing to console the error and re-raising the exception.


## `startMultipartUpload(bucketName: String, objectName: String) -> String`
### USAGE
* `bucketName`: The name of the bucket where the object will be stored.
* `objectName`: The name of the object to be uploaded.
* Returns a `String` representing the generated multipart upload ID.
### IMPL
* Generate a unique identifier, using UUID for the multipart upload.
* Create a `WriteChannel` for the specified bucket and object.
* Store the `WriteChannel` in the static map `multipart_uploads` associated with the generated ID.
* Return the generated multipart upload ID.

## `uploadPart(byteArray: ByteArray, uploadId: String)`
### USAGE
* `byteArray`: The data to be uploaded as part of the object.
* `uploadId`: The identifier of the multipart upload session.
### IMPL
* Retrieve the `WriteChannel` corresponding to the given `uploadId` from the `multipart_uploads` static map.
* If the `WriteChannel` does not exist, raise an appropriate exception.
* Write the `byteArray` to the `WriteChannel`.
* Handle any exceptions from the Google Cloud libraries related to network issues by printing to console the error and re-raising the exception.

## `completeMultipart(uploadId: String)`
### USAGE
* `uploadId`: The identifier of the multipart upload to be completed.
### IMPL
* Retrieve the `WriteChannel` corresponding to the given `uploadId` from the static `multipart_uploads` map.
* If the `WriteChannel` does not exist, raise an appropriate exception.
* Close the `WriteChannel` to finalize the upload.
* Remove the entry from the map associated with the `uploadId`.
* Handle any exceptions from the Google Cloud libraries related to network issues by printing to console the error and re-raising the exception.
