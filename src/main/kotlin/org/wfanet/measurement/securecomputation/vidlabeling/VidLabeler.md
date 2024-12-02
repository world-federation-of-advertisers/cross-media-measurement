# CLASS
## VidLabelerApp
* Package name is org.wfanet.measurement.securecomputation.vidlabeling
* Labels events using a VID model
* Extends BaseTeeApplication: https://raw.githubusercontent.com/world-federation-of-advertisers/cross-media-measurement/marcopremier/tee-sdk-base-class/src/test/kotlin/org/wfanet/measurement/securecomputation/teesdk/BaseTeeApplicationTest.kt
* Use BaseTeeApplicationImpl as an example
* Instead of TestWork, accepts CmmWork
* Here are some imports I know you will need:
* org.wfanet.virtualpeople.common.LabelerInput
* com.google.protobuf.ByteString
* kotlinx.coroutines.flow.chunked
* org.wfanet.measurement.securecomputation.CmmWork
* com.google.protobuf.kotlin.toByteString
* org.wfanet.measurement.queue.QueueSubscriber
* org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication

# CONSTRUCTORS
## VidLabelerApp `(storageClient: StorageClient, queueName: String, queueSubscriber: QueueSubscriber, parser: Parser<CmmWork>)
### USAGE
* for the labeler see https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/refs/heads/main/src/test/kotlin/org/wfanet/virtualpeople/core/labeler/LabelerTest.kt
* storage client see https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/refs/heads/main/src/test/kotlin/org/wfanet/measurement/storage/MesosRecordIoStorageClientTest.kt
### IMPL
* all the input params should be private val

# METHODS
## labelPath(inputBlobKey: String, outputBlobKey: String, labeler: Labeler, storageClient: MesoRecordIOStorageClient)
### USAGE
* Private
### IMPL
1. Read in the input data using the storageClient
2. Chunk the input data into groups of size CHUNK using kotlinx.coroutines.flow.chunked
3. Use multi-threading to process groups of the flows in parallel using flatMapMerge with concurrency CONCURRENCY
   .flatMapMerge(CONCURRENCY) { chunk ->
        chunk.map { byteString ->
        val labelerInput = LabelerInput.parseFrom(byteString)
        val labelerOutput = labeler.label(labelerInput)
        labelerOutput.toByteString()
   }.asFlow()
4. Write the blob using the storageClient
* Always keep things in flows. Do not convert to lists.
* Always be explicit with typing. Declare the type of all variables unless its completely obvious what the type is.
* For example, for all lambdas, please explicitly declare all types

## override runWork(message: CmmWork)
### Usage
### IMPL
1. Get the blob from the vidModelPath using the storageClient from the constructor, reduce the flow of bytestring (kotlinx.coroutines.flow.reduce) and parse it as a CompiledNode
2. Construct a Labeler using the CompiledNode - https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/refs/heads/main/src/main/kotlin/org/wfanet/virtualpeople/core/labeler/Labeler.kt
3. Reads input and output paths from proto work (CmmWork)
4. Construct a MesoRecordIOStorageClient using storageClient from constructor - https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/refs/heads/main/src/test/kotlin/org/wfanet/measurement/storage/MesosRecordIoStorageClientTest.kt
5. Call labelPath(inputBlobKey: String, outputBlobKey: String, labeler: Labeler, storageClient: MesoRecordIOStorageClient)
* Make sure to add imports for all the 