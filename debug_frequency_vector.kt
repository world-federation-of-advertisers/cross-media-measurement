import org.wfanet.measurement.edpaggregator.resultsfulfiller.*
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap
import org.mockito.kotlin.*
import com.google.protobuf.DynamicMessage
import java.time.Instant
import com.google.type.Interval

fun main() {
    // Create mock VidIndexMap
    val mockVidIndexMap = mock<VidIndexMap> {
        on { get(any()) } doReturn 0
        on { size } doReturn 10
    }
    
    // Create FrequencyVectorBuilder
    val builder = StripedFrequencyVectorBuilder(mockVidIndexMap)
    
    // Create FilterSpec
    val filterSpec = FilterSpec(
        celExpression = "",
        collectionInterval = Interval.getDefaultInstance(),
        vidSamplingStart = 0L,
        vidSamplingWidth = 1000000L,
        eventGroupReferenceId = "reference-id-1"
    )
    
    // Create FrequencyVectorSink
    val sink = FrequencyVectorSink(filterSpec, builder)
    
    // Create test event
    val mockMessage = mock<DynamicMessage>()
    val event = LabeledEvent(
        timestamp = Instant.now(),
        vid = 12345L,
        message = mockMessage
    )
    
    // Process event
    sink.processMatchedEvents(listOf(event), 1)
    
    // Get frequency vector
    val frequencyVector = sink.getFrequencyVector()
    
    println("Reach: ${frequencyVector.getReach()}")
    println("Total frequency: ${frequencyVector.getTotalFrequency()}")
    println("VIDs: ${frequencyVector.getVids()}")
}