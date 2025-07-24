# Direct Measurement Pipeline-Fulfiller Integration Strategy

## Overview

This document outlines the complete architecture and implementation strategy for integrating the EventProcessingPipeline with the measurement fulfillment system. The goal is to enable a single pipeline run to process multiple requisitions concurrently while maintaining the existing demo functionality.

## Core Design Principles

1. **Single Pipeline Run**: One `EventProcessingPipeline` execution processes all requisitions simultaneously
2. **Filter Deduplication**: Event groups with identical filters are deduplicated to avoid redundant processing
3. **FrequencyVector Interface**: Pass frequency vectors to fulfillers for flexible computation
4. **Concurrent Fulfillment**: Multiple `DirectMeasurementFulfiller`s execute concurrently without blocking
5. **Filter-Based Mapping**: Use filter specifications as keys for deduplication and sink lookup
6. **Unified Orchestrator**: Single orchestrator handles both demo and production modes

## Architecture Components

### 1. Filter Data Class

**New Component**: Encapsulates filter specifications as a unique key

```kotlin
data class FilterSpec(
  val celExpression: String,
  val collectionInterval: Interval,
  val vidSamplingStart: Long,
  val vidSamplingWidth: Long,
  val eventGroupReferenceId: String // NEW: Reference ID for event group filtering
) {
  // Used as key in maps for deduplication and sink lookup
  // Immutable and hashable for efficient map operations
}
```

**Usage**:
- Key for deduplicating event groups across requisitions
- Key for looking up frequency vector sinks
- Includes event group reference ID for precise filtering
- Replaces string-based `filterId` in pipeline processing

### 2. Enhanced LabeledEvent

**Current**: Contains `timestamp`, `vid`, and `message`
**New**: Adds `eventGroupReferenceId` for event filtering

```kotlin
data class LabeledEvent<T : Message>(
  val timestamp: Instant,
  val vid: Long,
  val message: T,
  val eventGroupReferenceId: String // NEW: Reference ID for filtering
)
```

**Usage**:
- Event sources populate this field when creating events
- Demo mode uses default value 'reference-id-1'
- Production mode extracts from actual event data
- FilterProcessor uses this for efficient filtering

### 3. FrequencyVector Interface

**New Component**: Abstraction for frequency vector data

```kotlin
interface FrequencyVector {
  // Core operations
  fun getFrequency(vid: Long): Int
  fun getReach(): Long
  fun getFrequencyDistribution(): Map<Int, Long>
  fun getTotalFrequency(): Long
  fun getMaxFrequency(): Int
  fun getVids(): Set<Long>
  
  // Aggregation operations
  fun merge(other: FrequencyVector): FrequencyVector
  fun sample(rate: Double, random: SecureRandom): FrequencyVector
}

// Implementation based on existing StripedByteFrequencyVector
class StripedFrequencyVector(
  private val stripedVector: StripedByteFrequencyVector
) : FrequencyVector {
  // Implementation details...
}
```

### 4. Enhanced FilterConfiguration

**Current**: Simple data class with `filterId`, `celExpression`, `timeInterval`
**New**: Uses `FilterSpec` as the primary key

```kotlin
data class FilterConfiguration(
  val filterSpec: FilterSpec,
  val requisitionNames: Set<String> // Track which requisitions use this filter
)
```

### 5. Enhanced EventProcessingPipeline Interface

**Current**: Returns `Map<String, SinkStatistics>`
**New**: Returns `Map<FilterSpec, FrequencyVector>`

```kotlin
interface EventProcessingPipeline {
  suspend fun processEventBatches(
    eventBatchFlow: Flow<List<LabeledEvent<DynamicMessage>>>,
    vidIndexMap: VidIndexMap,
    filters: List<FilterConfiguration>, // Now contains FilterSpec
    typeRegistry: TypeRegistry
  ): Map<FilterSpec, FrequencyVector> // Key change: Returns frequency vectors
}
```

### 6. Enhanced FrequencyVectorSink

**Current**: Builds StripedByteFrequencyVector and returns statistics
**New**: Uses FrequencyVector interface throughout

```kotlin
class FrequencyVectorSink(
  private val sinkId: FilterSpec, // Changed from String
  private val frequencyVector: FrequencyVector // Use interface, not implementation
) : EventSink {
  
  // Statistics tracking
  private val processedEventCount = AtomicLong(0)
  private val matchedEventCount = AtomicLong(0)
  private val errorCount = AtomicLong(0)
  
  override suspend fun process(event: LabeledEvent<DynamicMessage>) {
    processedEventCount.incrementAndGet()
    try {
      // Process event and update frequency vector
      matchedEventCount.incrementAndGet()
      // Note: Actual implementation depends on FrequencyVector being mutable
      // or using a builder pattern
    } catch (e: Exception) {
      errorCount.incrementAndGet()
    }
  }
  
  // Return the frequency vector interface
  fun getFrequencyVector(): FrequencyVector = frequencyVector
  
  // Generate statistics from the frequency vector for demo/monitoring
  fun getStatistics(): SinkStatistics {
    return SinkStatistics(
      processedEvents = processedEventCount.get(),
      matchedEvents = matchedEventCount.get(),
      reach = frequencyVector.getReach(),
      totalFrequency = frequencyVector.getTotalFrequency(),
      averageFrequency = if (frequencyVector.getReach() > 0) 
        frequencyVector.getTotalFrequency().toDouble() / frequencyVector.getReach() else 0.0,
      errorCount = errorCount.get()
    )
  }
}
```

### 7. FrequencyVector Builder Pattern

**New Component**: Builder for constructing frequency vectors during processing

```kotlin
interface FrequencyVectorBuilder {
  fun addEvent(vid: Long)
  fun build(): FrequencyVector
}

class StripedFrequencyVectorBuilder(
  private val maxVid: Long
) : FrequencyVectorBuilder {
  private val stripedVector = StripedByteFrequencyVector.build(maxVid)
  
  override fun addEvent(vid: Long) {
    stripedVector.add(vid)
  }
  
  override fun build(): FrequencyVector {
    return StripedFrequencyVector(stripedVector)
  }
}

// Updated FrequencyVectorSink to use builder
class FrequencyVectorSink(
  private val sinkId: FilterSpec,
  private val vectorBuilder: FrequencyVectorBuilder
) : EventSink {
  // ... implementation using builder
}
```

### 8. Refactored DirectMeasurementFulfiller

**Current**: Takes `Flow<Long>` sampledVids
**New**: Takes `FrequencyVector` for flexible computation

```kotlin
class DirectMeasurementFulfiller(
  private val requisitionName: String,
  private val requisitionDataProviderCertificateName: String,
  private val frequencyVector: FrequencyVector, // NEW: Replace Flow<Long>
  private val measurementSpec: MeasurementSpec, // For privacy params and config
  private val directProtocolConfig: ProtocolConfig.Direct,
  private val directNoiseMechanism: DirectNoiseMechanism,
  // ... other existing parameters
) : MeasurementFulfiller {
  
  override suspend fun fulfillRequisition() {
    // Build measurement result from frequency vector
    val measurementResult = buildMeasurementResultFromVector()
    // ... existing encryption/signing/fulfillment logic
  }
}
```

### 9. Enhanced DirectReachAndFrequencyResultBuilder

**Current**: Processes `Flow<Long>` sampledVids
**New**: Processes `FrequencyVector` data

```kotlin
class DirectReachAndFrequencyResultBuilder(
  private val directProtocolConfig: ProtocolConfig.Direct,
  private val frequencyVector: FrequencyVector, // NEW: Replace Flow<Long>
  private val maxFrequency: Int,
  private val reachPrivacyParams: DifferentialPrivacyParams,
  private val frequencyPrivacyParams: DifferentialPrivacyParams,
  private val samplingRate: Float,
  private val directNoiseMechanism: DirectNoiseMechanism,
  private val random: SecureRandom,
) : MeasurementResultBuilder {
  
  override suspend fun buildMeasurementResult(): Measurement.Result {
    // Use frequencyVector methods for computations
    val reach = frequencyVector.getReach()
    val frequencyDistribution = frequencyVector.getFrequencyDistribution()
    
    // Apply noise and build result
    // ... existing noise mechanism logic
  }
}
```

### 10. Enhanced EventProcessingOrchestrator

**Current**: Hardcoded demo filters, single-purpose
**New**: Dual-mode orchestrator supporting both demo and production

```kotlin
class EventProcessingOrchestrator {
  
  // NEW: Production mode with requisitions
  suspend fun runWithRequisitions(
    requisitions: List<Requisition>,
    config: PipelineConfiguration,
    typeRegistry: TypeRegistry
  ): Map<String, FrequencyVector> // Requisition name -> FrequencyVector
  
  // EXISTING: Demo mode (preserved)
  suspend fun run(config: PipelineConfiguration, typeRegistry: TypeRegistry)
  
  // Demo mode filter generation with default reference ID
  private fun generateDemoFilters(): List<FilterConfiguration> {
    val demographics = listOf(
      "" to "All demographics",
      "demographics.age_group.value == \"AGE_18_34\"" to "Age 18-34",
      // ... other demographic filters
    )
    
    return demographics.flatMap { (celExpression, name) ->
      timeIntervals.map { interval ->
        FilterConfiguration(
          FilterSpec(
            celExpression = celExpression,
            collectionInterval = interval,
            vidSamplingStart = 0L, // Default for demo
            vidSamplingWidth = Long.MAX_VALUE, // Default for demo (no sampling)
            eventGroupReferenceId = "reference-id-1" // Default for demo
          ),
          setOf("demo") // Requisition name for demo mode
        )
      }
    }
  }
  
  private fun generateFiltersFromRequisitions(
    requisitions: List<Requisition>
  ): Pair<List<FilterConfiguration>, Map<String, Set<FilterSpec>>>
  // Returns: (deduplicated filters, requisition -> filter mapping)
  
  private fun aggregateFrequencyVectors(
    filterSpecs: Set<FilterSpec>,
    pipelineResults: Map<FilterSpec, FrequencyVector>
  ): FrequencyVector {
    // Aggregate multiple frequency vectors for a requisition
    return filterSpecs
      .mapNotNull { pipelineResults[it] }
      .reduce { acc, vector -> acc.merge(vector) }
  }
}
```

### 11. Refactored ResultsFulfiller

**Current**: Sequential processing with VID flows
**New**: Pipeline-integrated with concurrent fulfillment

```kotlin
class ResultsFulfiller(
  // ... existing parameters
  private val eventProcessingOrchestrator: EventProcessingOrchestrator // NEW
) {
  
  suspend fun fulfillRequisitions() {
    val groupedRequisitions = getRequisitions()
    val requisitions = groupedRequisitions.requisitionsList.map { /* ... */ }
    
    // NEW: Single pipeline run for all requisitions
    val frequencyVectors = eventProcessingOrchestrator.runWithRequisitions(
      requisitions, 
      buildPipelineConfig(), 
      typeRegistry
    )
    
    // NEW: Concurrent fulfillment
    val fulfillmentJobs = requisitions.map { requisition ->
      async {
        fulfillSingleRequisition(requisition, frequencyVectors[requisition.name]!!)
      }
    }
    
    fulfillmentJobs.awaitAll()
  }
  
  private suspend fun fulfillSingleRequisition(
    requisition: Requisition,
    frequencyVector: FrequencyVector
  ) {
    // Apply sampling if needed
    val sampledVector = if (needsSampling(requisition)) {
      frequencyVector.sample(samplingRate, secureRandom)
    } else {
      frequencyVector
    }
    
    // Create fulfiller with frequency vector
    val fulfiller = DirectMeasurementFulfiller(
      requisition.name,
      requisition.dataProviderCertificate,
      sampledVector, // NEW: Pass frequency vector
      // ... other parameters
    )
    fulfiller.fulfillRequisition()
  }
}
```

### 12. Enhanced FilterProcessor

**Current**: Filters by CEL expression and time range
**New**: Also filters by event group reference ID

```kotlin
class FilterProcessor(
  val filterId: String,
  val celExpression: String,
  private val eventMessageDescriptor: Descriptors.Descriptor,
  private val typeRegistry: TypeRegistry,
  private val collectionInterval: Interval? = null,
  private val eventGroupReferenceId: String // NEW: Reference ID for filtering
) {
  
  suspend fun processBatch(batch: EventBatch): List<LabeledEvent<DynamicMessage>> {
    return batch.events.filter { event ->
      // First check reference ID match
      if (event.eventGroupReferenceId != eventGroupReferenceId) {
        return@filter false
      }
      
      // Then apply time range filter if collection interval is specified
      val timeMatches = if (collectionInterval != null) {
        isEventInTimeRange(event, collectionInterval)
      } else {
        true
      }
      
      if (!timeMatches) {
        return@filter false
      }
      
      // Finally apply CEL filter
      try {
        EventFilters.matches(event.message, program)
      } catch (e: Exception) {
        logger.warning("CEL filter evaluation failed for event with VID ${event.vid}: ${e.message}")
        false
      }
    }
  }
}
```

**Key Changes**:
- Adds reference ID check as first filter (most efficient)
- Maintains existing CEL and time range filtering
- Ensures events only match their designated event groups

## Data Flow Architecture

### Phase 1: Filter Extraction & Deduplication

```
Requisitions → Extract FilterSpecs → Deduplicate → FilterConfigurations
     ↓
Requisition A: [FilterSpec1, FilterSpec2]
Requisition B: [FilterSpec2, FilterSpec3]  
Requisition C: [FilterSpec1, FilterSpec4]
     ↓
Deduplicated: [FilterSpec1, FilterSpec2, FilterSpec3, FilterSpec4]
     ↓
Mapping: {
  "ReqA": [FilterSpec1, FilterSpec2],
  "ReqB": [FilterSpec2, FilterSpec3],
  "ReqC": [FilterSpec1, FilterSpec4]
}
```

### Phase 2: Single Pipeline Processing

```
EventBatches → Pipeline → Map<FilterSpec, FrequencyVector>
     ↓
{
  FilterSpec1: FrequencyVector{reach: 1000, maxFreq: 5, ...},
  FilterSpec2: FrequencyVector{reach: 1500, maxFreq: 7, ...},
  FilterSpec3: FrequencyVector{reach: 2000, maxFreq: 10, ...},
  FilterSpec4: FrequencyVector{reach: 500, maxFreq: 3, ...}
}
```

### Phase 3: Vector Aggregation & Fulfillment

```
For each Requisition:
  Aggregate FrequencyVectors from relevant FilterSpecs
  Apply sampling if needed
  Create DirectMeasurementFulfiller with aggregated vector
  Execute fulfillment concurrently
```

## Implementation Specifications

### Filter Generation Algorithm

```kotlin
fun generateFiltersFromRequisitions(
  requisitions: List<Requisition>
): Pair<List<FilterConfiguration>, Map<String, Set<FilterSpec>>> {
  
  val filterSpecToRequisitions = mutableMapOf<FilterSpec, MutableSet<String>>()
  
  for (requisition in requisitions) {
    val requisitionSpec = decryptAndUnpack(requisition)
    val measurementSpec = unpack(requisition.measurementSpec)
    
    for (eventGroup in requisitionSpec.events.eventGroupsList) {
      val filterSpec = FilterSpec(
        celExpression = eventGroup.value.filter,
        collectionInterval = eventGroup.value.collectionInterval,
        vidSamplingStart = measurementSpec.vidSamplingInterval.start,
        vidSamplingWidth = measurementSpec.vidSamplingInterval.width,
        eventGroupReferenceId = eventGroup.key // Use event group key as reference ID
      )
      
      filterSpecToRequisitions.getOrPut(filterSpec) { mutableSetOf() }
        .add(requisition.name)
    }
  }
  
  val filterConfigurations = filterSpecToRequisitions.map { (filterSpec, reqNames) ->
    FilterConfiguration(filterSpec, reqNames.toSet())
  }
  
  val requisitionToFilters = requisitions.associate { req ->
    req.name to filterConfigurations
      .filter { it.requisitionNames.contains(req.name) }
      .map { it.filterSpec }
      .toSet()
  }
  
  return Pair(filterConfigurations, requisitionToFilters)
}
```

### Frequency Vector Aggregation

```kotlin
class AggregatedFrequencyVector(
  private val vectors: List<FrequencyVector>
) : FrequencyVector {
  
  override fun getFrequency(vid: Long): Int {
    return vectors.sumOf { it.getFrequency(vid) }
  }
  
  override fun getReach(): Long {
    // Union of all VIDs across vectors
    return vectors.flatMap { it.getVids() }.toSet().size.toLong()
  }
  
  override fun getFrequencyDistribution(): Map<Int, Long> {
    // Compute combined frequency distribution
    val vidToTotalFreq = mutableMapOf<Long, Int>()
    for (vector in vectors) {
      for (vid in vector.getVids()) {
        vidToTotalFreq[vid] = vidToTotalFreq.getOrDefault(vid, 0) + vector.getFrequency(vid)
      }
    }
    
    return vidToTotalFreq.values
      .groupingBy { it }
      .eachCount()
      .mapValues { it.value.toLong() }
  }
  
  override fun merge(other: FrequencyVector): FrequencyVector {
    return AggregatedFrequencyVector(vectors + other)
  }
  
  // ... other interface methods
}
```

### Pipeline Integration Points

1. **FrequencyVectorSink Enhancement**: 
   - Use `FilterSpec` as sink ID
   - Work with `FrequencyVector` interface instead of implementation
   - Use `FrequencyVectorBuilder` pattern for construction
   - Return `FrequencyVector` interface
   - Maintain statistics for demo/monitoring

2. **FilterProcessor Enhancement**: 
   - Work with `FilterSpec` instead of string IDs
   - Add event group reference ID filtering
   - Extract reference ID from `FilterSpec` when creating processor
   - Efficient early filtering by reference ID

3. **Pipeline Implementations**: 
   - Update return type to `Map<FilterSpec, FrequencyVector>`
   - Create appropriate `FrequencyVectorBuilder` for each sink
   - Collect frequency vectors instead of statistics
   - Pass reference ID through filter chain

4. **Event Source Updates**:
   - **SyntheticEventGenerator**: Populate `eventGroupReferenceId` field
   - **StorageEventSource**: Extract reference ID from stored events
   - **Demo Mode**: Use default 'reference-id-1' for all events

## Migration Strategy

### Phase 1: Core Data Structures
- Implement `FilterSpec` data class
- Implement `FrequencyVector` interface
- Create `StripedFrequencyVector` wrapper
- Implement `FrequencyVectorBuilder` pattern

### Phase 2: Pipeline Layer
- Update `EventProcessingPipeline` interface
- Modify `FrequencyVectorSink` to use `FrequencyVector` interface
- Update pipeline implementations

### Phase 3: Fulfillment Layer
- Refactor `DirectMeasurementFulfiller` to use `FrequencyVector`
- Update `DirectReachAndFrequencyResultBuilder`
- Implement vector aggregation logic

### Phase 4: Orchestration Layer
- Enhance `EventProcessingOrchestrator` with dual-mode support
- Implement filter deduplication and mapping
- Add concurrent fulfillment coordination

### Phase 5: Integration
- Refactor `ResultsFulfiller` to use new architecture
- Update configuration and initialization logic
- Add comprehensive testing

## Testing Strategy

### Unit Tests
- `FilterSpec` equality and hashing
- `FrequencyVector` implementations
- `FrequencyVectorBuilder` implementations
- Vector aggregation and sampling
- Filter deduplication algorithm

### Integration Tests
- End-to-end pipeline with multiple requisitions
- Concurrent fulfillment scenarios
- Demo mode preservation
- Error handling and isolation

### Performance Tests
- Pipeline throughput with multiple requisitions
- Memory usage with large frequency vectors
- Concurrent fulfillment scalability
- Vector aggregation performance

## Backwards Compatibility

- **Demo Mode**: Existing `EventProcessingOrchestrator.run()` preserved
- **Statistics**: Still available via `FrequencyVectorSink.getStatistics()`
- **Configuration**: Existing `PipelineConfiguration` extended, not replaced
- **Pipeline Interfaces**: New methods added alongside existing ones

## Error Handling

- **Filter Extraction**: Continue processing other requisitions if one fails
- **Pipeline Processing**: Isolate filter failures, don't block entire run
- **Vector Aggregation**: Handle missing vectors gracefully
- **Fulfillment**: Individual fulfiller failures don't affect others
- **Reporting**: Comprehensive error logging and statistics

## Key Benefits

1. **Flexibility**: Fulfillers can compute any metric from frequency vectors
2. **Interface-based Design**: Clean abstraction over implementation details
3. **Performance**: Single pipeline run for all requisitions
4. **Scalability**: Concurrent fulfillment without blocking
5. **Maintainability**: Clean separation of concerns
6. **Extensibility**: Easy to add new measurement types and frequency vector implementations

## Event Group Reference ID Implications

### Benefits of Reference ID Filtering

1. **Precise Event Routing**: Events are matched only to their designated event groups
2. **Efficient Filtering**: Reference ID check happens first, reducing CEL evaluation overhead
3. **Clear Data Isolation**: Different event groups never mix, even with identical filters
4. **Simplified Debugging**: Easy to trace which events match which requisitions

### Implementation Considerations

1. **Event Source Responsibilities**:
   - Must populate `eventGroupReferenceId` for every event
   - Demo mode uses hardcoded 'reference-id-1'
   - Production mode extracts from event data or configuration

2. **Filter Deduplication**:
   - Two filters with same CEL/time but different reference IDs are unique
   - This ensures proper isolation between event groups
   - May increase total number of filters processed

3. **Performance Impact**:
   - Minimal: Reference ID comparison is O(1)
   - Actually improves performance by reducing CEL evaluations
   - Slightly increases memory per event (one string field)

4. **Backwards Compatibility**:
   - Existing demo functionality preserved with default reference ID
   - Can handle legacy events by defaulting to a known reference ID
   - Easy migration path for existing event sources

This strategy provides a complete blueprint for integrating the event processing pipeline with the measurement fulfillment system using frequency vectors as the core data exchange format, with proper event group isolation via reference IDs.
