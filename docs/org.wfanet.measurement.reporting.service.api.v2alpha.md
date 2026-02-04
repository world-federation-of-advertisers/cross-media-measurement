# org.wfanet.measurement.reporting.service.api.v2alpha

## Overview

This package provides the v2alpha API layer for the Cross-Media Measurement reporting service. It implements gRPC service endpoints for creating and managing reports, metrics, reporting sets, event groups, and related resources. The package handles authorization, request validation, proto conversions between public and internal APIs, CEL filtering, and metric specification defaults.

## Components

### DataProvidersService
gRPC service for retrieving data provider information from the Kingdom API.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| getDataProvider | `request: GetDataProviderRequest` | `DataProvider` | Retrieves data provider details with authorization check |

### EventGroupMetadataDescriptorsService
gRPC service for managing event group metadata descriptors.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| getEventGroupMetadataDescriptor | `request: GetEventGroupMetadataDescriptorRequest` | `EventGroupMetadataDescriptor` | Retrieves a single metadata descriptor |
| batchGetEventGroupMetadataDescriptors | `request: BatchGetEventGroupMetadataDescriptorsRequest` | `BatchGetEventGroupMetadataDescriptorsResponse` | Retrieves multiple metadata descriptors |

### EventGroupsService
gRPC service for listing event groups with metadata decryption and CEL filtering.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| listEventGroups | `request: ListEventGroupsRequest` | `ListEventGroupsResponse` | Lists event groups with pagination, filtering, and decryption |

**Key Features:**
- Decrypts encrypted event group metadata using private keys
- Supports CEL-based filtering on event group fields
- Handles pagination with deadline management
- Converts CMMS event groups to reporting API format

### MetricCalculationSpecsService
gRPC service for managing metric calculation specifications.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| createMetricCalculationSpec | `request: CreateMetricCalculationSpecRequest` | `MetricCalculationSpec` | Creates a new metric calculation spec |
| getMetricCalculationSpec | `request: GetMetricCalculationSpecRequest` | `MetricCalculationSpec` | Retrieves a metric calculation spec |
| listMetricCalculationSpecs | `request: ListMetricCalculationSpecsRequest` | `ListMetricCalculationSpecsResponse` | Lists metric calculation specs with filtering |

**Key Features:**
- Validates metric specs with defaults from config
- Expands grouping predicates to Cartesian product
- Supports daily, weekly, and monthly frequencies
- Validates model line associations

### MetricsService
Complex service for creating and managing metrics with measurement orchestration.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| createMetric | `request: CreateMetricRequest` | `Metric` | Creates metric and orchestrates measurements |
| getMetric | `request: GetMetricRequest` | `Metric` | Retrieves metric details |
| batchGetMetrics | `request: BatchGetMetricsRequest` | `BatchGetMetricsResponse` | Retrieves multiple metrics |
| listMetrics | `request: ListMetricsRequest` | `ListMetricsResponse` | Lists metrics with pagination |

**Key Features:**
- Creates CMMS measurements based on metric specs
- Computes metric results from measurement outcomes
- Handles reach, frequency, impression, duration, and population metrics
- Manages differential privacy and VID sampling
- Signs measurement requests with private keys

### ModelLinesService
gRPC service for enumerating valid model lines.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| enumerateValidModelLines | `request: EnumerateValidModelLinesRequest` | `EnumerateValidModelLinesResponse` | Lists model lines valid for time interval and data providers |

### ReportsService
gRPC service for managing reports containing multiple metrics.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| createReport | `request: CreateReportRequest` | `Report` | Creates report with reporting metrics |
| getReport | `request: GetReportRequest` | `Report` | Retrieves report details |
| listReports | `request: ListReportsRequest` | `ListReportsResponse` | Lists reports with pagination |

**Key Features:**
- Creates multiple metrics from report template
- Manages report schedules and iterations
- Supports trailing windows and fixed windows
- Handles time interval calculations

### BasicReportsService
gRPC service for creating and managing basic reports with simplified API.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| createBasicReport | `request: CreateBasicReportRequest` | `BasicReport` | Creates basic report with validation |
| getBasicReport | `request: GetBasicReportRequest` | `BasicReport` | Retrieves basic report |
| listBasicReports | `request: ListBasicReportsRequest` | `ListBasicReportsResponse` | Lists basic reports |

### ReportSchedulesService
gRPC service for managing recurring report schedules.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| createReportSchedule | `request: CreateReportScheduleRequest` | `ReportSchedule` | Creates report schedule |
| getReportSchedule | `request: GetReportScheduleRequest` | `ReportSchedule` | Retrieves schedule details |
| listReportSchedules | `request: ListReportSchedulesRequest` | `ListReportSchedulesResponse` | Lists schedules |
| stopReportSchedule | `request: StopReportScheduleRequest` | `ReportSchedule` | Stops active schedule |

### ReportScheduleIterationsService
gRPC service for listing iterations of report schedules.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| listReportScheduleIterations | `request: ListReportScheduleIterationsRequest` | `ListReportScheduleIterationsResponse` | Lists iterations with reports |

### ReportingSetsService
gRPC service for managing reporting sets (collections of event groups).

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| createReportingSet | `request: CreateReportingSetRequest` | `ReportingSet` | Creates primitive or composite set |
| getReportingSet | `request: GetReportingSetRequest` | `ReportingSet` | Retrieves reporting set |
| listReportingSets | `request: ListReportingSetsRequest` | `ListReportingSetsResponse` | Lists reporting sets |

**Key Features:**
- Supports primitive sets (event group collections)
- Supports composite sets (set expressions)
- Compiles set expressions to weighted subset unions
- Validates campaign group associations

### ImpressionQualificationFiltersService
gRPC service for managing impression qualification filters.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| getImpressionQualificationFilter | `request: GetImpressionQualificationFilterRequest` | `ImpressionQualificationFilter` | Retrieves filter details |
| listImpressionQualificationFilters | `request: ListImpressionQualificationFiltersRequest` | `ListImpressionQualificationFiltersResponse` | Lists filters |

## Data Structures

### Resource Keys

| Class | Properties | Description |
|-------|------------|-------------|
| BasicReportKey | `parentKey: MeasurementConsumerKey`, `basicReportId: String` | Resource key for basic reports |
| EventGroupKey | `cmmsMeasurementConsumerId: String`, `cmmsEventGroupId: String` | Resource key for event groups |
| ImpressionQualificationFilterKey | `impressionQualificationFilterId: String` | Resource key for filters |
| MetricCalculationSpecKey | `parentKey: MeasurementConsumerKey`, `metricCalculationSpecId: String` | Resource key for metric calc specs |
| MetricKey | `parentKey: MeasurementConsumerKey`, `metricId: String` | Resource key for metrics |
| ReportKey | `parentKey: MeasurementConsumerKey`, `reportId: String` | Resource key for reports |
| ReportScheduleKey | `parentKey: MeasurementConsumerKey`, `reportScheduleId: String` | Resource key for schedules |
| ReportScheduleIterationKey | `parentKey: ReportScheduleKey`, `reportScheduleIterationId: String` | Resource key for iterations |
| ReportingSetKey | `parentKey: MeasurementConsumerKey`, `reportingSetId: String` | Resource key for reporting sets |

### Configuration and Credentials

| Class | Properties | Description |
|-------|------------|-------------|
| MeasurementConsumerCredentials | `resourceKey: MeasurementConsumerKey`, `callCredentials: ApiKeyCredentials`, `signingCertificateKey: MeasurementConsumerCertificateKey`, `signingPrivateKeyPath: String` | Credentials for measurement consumer |

### Exceptions

| Class | Description |
|-------|-------------|
| MetricSpecDefaultsException | Exception for metric spec default validation errors |
| NoiseMechanismUnspecifiedException | Noise mechanism not specified |
| NoiseMechanismUnrecognizedException | Noise mechanism not recognized |
| MeasurementVarianceNotComputableException | Measurement variance cannot be computed |
| MetricResultNotComputableException | Metric result cannot be computed |

## Utility Components

### CelFilteringMethods
Provides CEL-based filtering utilities.

| Function | Parameters | Returns | Description |
|----------|------------|---------|-------------|
| buildCelEnvironment | `message: Message` | `Env` | Builds CEL environment from protobuf message |
| filterList | `env: Env`, `items: List<T>`, `filter: String` | `List<T>` | Filters list using CEL expression |

### MetricSpecDefaults
Applies default values to metric specifications from configuration.

| Function | Parameters | Returns | Description |
|----------|------------|---------|-------------|
| MetricSpec.withDefaults | `metricSpecConfig: MetricSpecConfig`, `secureRandom: Random`, `allowSamplingIntervalWrapping: Boolean` | `MetricSpec` | Applies defaults to metric spec |
| MetricSpecConfig.validate | - | `Unit` | Validates metric spec config |

### ProtoConversions
Extensive conversion functions between public and internal proto representations.

**Key Conversions:**
- `InternalMetricSpec.toMetricSpec()` - Internal to public metric spec
- `MetricSpec.toInternal()` - Public to internal metric spec
- `InternalReportingSet.toReportingSet()` - Internal to public reporting set
- `ReportingSet.toInternal()` - Public to internal reporting set
- `InternalMetric.State.toPublic()` - Internal to public metric state
- `Measurement.Result.toInternal()` - CMMS measurement result to internal

### SetExpressionCompiler
Compiles set expressions for composite reporting sets into weighted subset unions.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| compileSetExpression | `setOperationExpression: SetOperationExpression`, `numReportingSets: Int` | `List<WeightedSubsetUnion>` | Compiles expression to weighted unions |

**Key Features:**
- Decomposes set expressions into primitive regions
- Converts regions to union set coefficients
- Uses Venn diagram decomposition
- Caches computation results

### ReportScheduleInfoServerInterceptor
gRPC interceptor for extracting report schedule context.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| interceptCall | `call: ServerCall`, `headers: Metadata`, `next: ServerCallHandler` | `ServerCall.Listener` | Extracts schedule info from metadata |

### IdVariable
Internal enum for resource ID variables used in resource name parsing.

**Values:**
- BASIC_REPORT
- IMPRESSION_QUALIFICATION_FILTER
- DATA_PROVIDER
- EVENT_GROUP
- MEASUREMENT_CONSUMER
- REPORTING_SET
- METRIC
- METRIC_CALCULATION_SPEC
- REPORT
- REPORT_SCHEDULE
- REPORT_SCHEDULE_ITERATION

## Dependencies

### Internal Dependencies
- `org.wfanet.measurement.internal.reporting.v2` - Internal reporting API proto definitions
- `org.wfanet.measurement.reporting.v2alpha` - Public reporting API proto definitions
- `org.wfanet.measurement.api.v2alpha` - Kingdom API proto definitions
- `org.wfanet.measurement.config.reporting` - Configuration protos
- `org.wfanet.measurement.access.client.v1alpha` - Authorization client
- `org.wfanet.measurement.common` - Common utilities
- `org.wfanet.measurement.consent.client.measurementconsumer` - Metadata decryption
- `org.wfanet.measurement.measurementconsumer.stats` - Statistical computations

### External Dependencies
- `io.grpc` - gRPC framework
- `com.google.protobuf` - Protocol buffers
- `org.projectnessie.cel` - Common Expression Language
- `kotlinx.coroutines` - Kotlin coroutines
- `com.github.benmanes.caffeine` - Caching library
- `com.google.gson` - JSON serialization

## Usage Example

```kotlin
// Create a MetricCalculationSpecsService
val service = MetricCalculationSpecsService(
  internalMetricCalculationSpecsStub = internalStub,
  kingdomModelLinesStub = modelLinesStub,
  metricSpecConfig = config,
  authorization = authClient,
  secureRandom = secureRandom,
  measurementConsumerConfigs = configs
)

// Create a metric calculation spec
val request = createMetricCalculationSpecRequest {
  parent = "measurementConsumers/mc123"
  metricCalculationSpecId = "spec456"
  metricCalculationSpec = metricCalculationSpec {
    displayName = "Daily Reach"
    metricSpecs += metricSpec {
      reach = reachParams {
        privacyParams = differentialPrivacyParams {
          epsilon = 0.1
          delta = 1e-12
        }
      }
    }
    metricFrequencySpec = metricFrequencySpec {
      daily = Daily.getDefaultInstance()
    }
  }
}

val spec = service.createMetricCalculationSpec(request)
```

## Class Diagram

```mermaid
classDiagram
    class DataProvidersService {
        +getDataProvider(request) DataProvider
    }
    class EventGroupsService {
        +listEventGroups(request) ListEventGroupsResponse
        -filterEventGroups(eventGroups, filter) List~EventGroup~
        -decryptMetadata(cmmsEventGroup, principalName) Metadata
    }
    class MetricCalculationSpecsService {
        +createMetricCalculationSpec(request) MetricCalculationSpec
        +getMetricCalculationSpec(request) MetricCalculationSpec
        +listMetricCalculationSpecs(request) ListMetricCalculationSpecsResponse
    }
    class MetricsService {
        +createMetric(request) Metric
        +getMetric(request) Metric
        +batchGetMetrics(request) BatchGetMetricsResponse
        +listMetrics(request) ListMetricsResponse
    }
    class ReportsService {
        +createReport(request) Report
        +getReport(request) Report
        +listReports(request) ListReportsResponse
    }
    class ReportingSetsService {
        +createReportingSet(request) ReportingSet
        +getReportingSet(request) ReportingSet
        +listReportingSets(request) ListReportingSetsResponse
    }
    class SetExpressionCompiler {
        +compileSetExpression(expression, numSets) List~WeightedSubsetUnion~
    }
    class ProtoConversions {
        <<utility>>
        +toInternal()
        +toPublic()
    }
    class MetricSpecDefaults {
        <<utility>>
        +withDefaults(config, random) MetricSpec
        +validate()
    }
    class CelFilteringMethods {
        <<utility>>
        +buildCelEnvironment(message) Env
        +filterList(env, items, filter) List
    }

    ReportingSetsService --> SetExpressionCompiler
    EventGroupsService --> CelFilteringMethods
    MetricCalculationSpecsService --> MetricSpecDefaults
    MetricsService --> ProtoConversions
    ReportsService --> MetricsService
