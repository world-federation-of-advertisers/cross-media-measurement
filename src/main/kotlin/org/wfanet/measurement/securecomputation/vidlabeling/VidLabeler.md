# CLASS
## VidLabelerApp
* Labels events using a VID model
* Extends BaseTeeApplication: https://raw.githubusercontent.com/world-federation-of-advertisers/cross-media-measurement/765788c615d0405c4927aabdc74e05995925aeb3/src/test/kotlin/org/wfanet/measurement/securecomputation/teesdk/BaseTeeApplicationTest.kt
* Use BaseTeeApplicationImpl as an example
* Instead of TestWork, accepts CmmWork

# CONSTRUCTORS
## VidLabelerApp `(queueName: String, queueClient: QueueClient, parser: Parser<CmmWork>)
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
1. Read in the data using the MesosRecordIoStorageClient
2. Chunk the flows into groups of size CHUNK
3. Parse the input from Flow<ByteString> to LabelerInput
4. Use multi-threading to process groups of the flows in parallel using flatMapMerge with concurrency CONCURRENCY 
5. Use the label function to get the corresponding LabelerOutput for each LabelerInput
6. Output using the same MesosRecordIoStorageClient
* Always keep things in flows. Do not convert to lists.

## runWork(work: CmmWork)
### Usage
### IMPL
* Reads input and output paths from proto
* Construct a MesoRecordIOStorageClient using InMemoryStorageClient - https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/refs/heads/main/src/test/kotlin/org/wfanet/measurement/storage/MesosRecordIoStorageClientTest.kt
* Read the vidModelPath using the InMemoryStorageClient, flatten all the bytes and parse it as a CompiledNode 
* Construct a Labeler using the compiled Node - https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/refs/heads/main/src/main/kotlin/org/wfanet/virtualpeople/core/labeler/Labeler.kt
* Call labelPath(inputBlobKey: String, outputBlobKey: String, labeler: Labeler, storageClient: MesoRecordIOStorageClient)

