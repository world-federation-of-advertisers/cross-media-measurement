# CLASS
## CloudProvider
* This is an enum class.
* Params are:
** GOOGLE

# CLASS
## CloudStorage
* This abstract class serves as a base for cloud storage services, providing a set of abstract methods to interact with cloud storage systems like Google Cloud Storage.
* Part of the package or module designed for cloud storage management.
* Provides foundational methods for handling data retrieval and multipart uploads.

# METHODS
## `getData(bucketName: String, objectName: String) -> InputStream`
### USAGE
* `bucketName`: The name of the bucket from which to retrieve data.
* `objectName`: The name of the object to retrieve from the bucket.
* Used to obtain an input stream for reading data from a specified object in the cloud storage.
### IMPL
* Abstract method.
* Implementations should interact with the cloud storage API to access the specified object and return an `InputStream` for reading.

## `startMultipartUpload(bucketName: String, objectName: String) -> String`
### USAGE
* `bucketName`: The name of the bucket where the upload will occur.
* `objectName`: The name of the object to be uploaded.
* Starts a multipart upload process and returns the multipart upload ID.
### IMPL
* Abstract method.
* Implementations should initiate a multipart upload session with the cloud storage API and return an identifier for the session.

## `uploadPart(byteArray: ByteArray, uploadId: String) -> void`
### USAGE
* Abstract method.
* `byteArray`: The data to upload as a part of the multipart upload.
* `uploadId`: The identifier of the multipart upload session.
* Used to upload a part of data to an ongoing multipart upload session.
### IMPL
* Implementations should upload the provided data as a part of the specified multipart upload session, handling the part number correctly.

## `completeMultipart(uploadId: String) -> void`
### USAGE
* Abstract method.
* `uploadId`: The identifier of the multipart upload session to complete.
* Completes the multipart upload process, finalizing the uploaded parts into a single object.
### IMPL
* Implementations should finalize the multipart upload session with the cloud storage API, ensuring all parts are correctly assembled.

## `getCloudStorage(cloudProvider: CloudProvider) -> CloudStorage`
### USAGE
* `cloudProvider`: Indicates the type of cloud storage service to instantiate, such as "GoogleCloudStorage".
* Returns an instance of a concrete subclass of `CloudStorage`, such as `GoogleCloudStorage`.
* Utilizes a factory pattern to instantiate the appropriate cloud storage service based on the provided `cloudProvider`.
### IMPL
* Use the `CloudProvider` enum, to determine the appropriate concrete class and return its instance based on the `cloudProvider`.
