# API & Protobuf Standards

## What Is This?

This guide covers protobuf definitions and API design conventions for WFA
repositories, including Google AIP compliance, resource naming, field
annotations, enum design, and documentation conventions. It supplements the
general API guidance in [Code Style](code-style.md).

Per the code-style guide: "All public service APIs (those called by a different
component) follow the [AIPs](https://aip.dev/), with specific exceptions where
noted."

Key terminology conventions from the code-style guide:

*   "Reference ID" refers to an ID from an external system.
*   Names of protobuf messages for the serialized portion of a database row end
    in `Details`.
*   Database internal IDs must not be exposed outside of internal API servers.

## Standard Methods

Standard CRUD methods must follow their respective AIP patterns exactly. The
API lint workflow should catch violations, but manual review is still necessary.

*   [AIP-133 (Create)](https://google.aip.dev/133): Request must have `parent`
    string field and the resource message field. Response is the created
    resource.
*   [AIP-134 (Update)](https://google.aip.dev/134): Request must have the
    resource message field and `update_mask`.
*   [AIP-131 (Get)](https://google.aip.dev/131): Request must have `name`
    string field.
*   [AIP-132 (List)](https://google.aip.dev/132): Request must have `parent`,
    `page_size`, `page_token`. Response must have the repeated resource and
    `next_page_token`.
*   [AIP-135 (Delete)](https://google.aip.dev/135): Request must have `name`
    string field.

Custom methods ([AIP-136](https://google.aip.dev/136)) must not use Get, List,
Create, Update, or Delete as verbs. Use alternatives such as `Read`, `Search`,
or `Query`.

## Resource Design

### Canonical Parents

The `parent` field in resource creation must reference the canonical parent in
the resource hierarchy, not an operator or intermediary. Regardless of which
pattern is used to get the resource, the resulting `name` field will contain the
canonical resource name.

### Resource IDs

Resource IDs should be short, human-readable identifiers that conform to
RFC-1034 label conventions. Never use URIs, file paths, or other long strings
as resource IDs. See [AIP-122](https://google.aip.dev/122).

Per [AIP-122](https://google.aip.dev/122), child collections may omit parent
prefixes in their collection identifiers.

### Internal Database IDs

Database internal IDs must not be exposed outside of internal API servers.
API resource names use resource IDs (external identifiers). Internal database
primary keys are private to the implementation.

Maintain separate columns for the database primary key and the API resource ID.
For example, `RequisitionMetadata` has both `RequisitionMetadataId INT64` and
`RequisitionMetadataResourceId STRING(63)`.

Database access is restricted to servers hosting internal APIs. No other
component should directly access the database.

### DB Row Serialization Messages

Names of protobuf messages for the serialized portion of a database row must
end in `Details`.

## Field Design

### Reserved Field Numbers

When removing a field from a proto message, always use the `reserved`
directive to prevent the field number from being reused.

```protobuf
message EventGroup {
    reserved 5;
    string name = 6;
}
```

### Oneof Annotations

Individual fields inside a `oneof` are not individually required. Document
the requirement on the `oneof` itself using a comment, not field behavior
annotations.

### Field Behavior Annotations

If you start using `[(google.api.field_behavior)]` annotations, they must be
applied consistently across the entire API.

### Wrapper Messages for Extensibility

When you might need to add fields alongside a value in the future, create a
wrapper message now rather than using the bare type directly.

```protobuf
// Prefer this:
message AggregatedActivity {
    DateInterval date_interval = 1;
}
repeated AggregatedActivity aggregated_activities = 3;

// Over this:
repeated DateInterval active_intervals = 3;
```

### Empty Messages for Future Oneofs

Define empty message types within a oneof even if they have no fields yet, so
the oneof can eventually be marked required.

## Enum Design

Do not create enums where code has to interpret what `UNSPECIFIED` means. If
the absence of a value is meaningful, use an optional message type instead.

`UNSPECIFIED` must always be the 0/default value.

## Breaking Changes

Never renumber fields or change field semantics in released protos. Add new
fields and deprecate old ones. Field renumbering is only safe if the message
has never been used in released code.

## API vs. Configuration

Messages defined in the versioned API should only be used for data that
travels over the wire in API calls. Static process configuration belongs in
separate, unversioned config protos.

For components that need both API and config representations, keep the two as
separate messages and convert between them. Options in order of preference:

1.  Use a separate message in the config and convert to the appropriate
    versioned API message.
2.  Use an `Any` field.
3.  Have a separate field for each API version.

Importing proto definitions from external packages creates tight version
coupling. Prefer local definitions or serialized versions.

## Structured Filters

When using structured filter fields instead of [AIP-160](https://google.aip.dev/160)
filter strings, document this explicitly in the field comment.

## Documentation Conventions

### Field Comments

Field comments should be noun phrases (summary fragments). Method comments
should be verb phrases.

```protobuf
// Resource name of the `Report`.
string name = 1;

// Metadata about the use of this `Measurement` in the Reporting system.
string reporting_metadata = 5;
```

References to protobuf symbols in comments should be wrapped in backticks.

### Deprecation Timelines

Never say "will be removed in a future release." Specify the exact version
and which repository's release the timeline refers to.

## Naming Conventions

*   The API uses the full term `DataProvider`, not the abbreviation "EDP".
*   Google's cloud offering is referred to as "Google Cloud", not "GCP".
*   Cryptographic abbreviations DEK (Data Encryption Key) and KEK (Key
    Encryption Key) are always uppercase.
*   The cryptographic concept is "envelope encryption" regardless of whether
    the implementation focuses on encryption or decryption.
*   Use "params" for runtime parameters. "Configuration" or "config" is
    reserved for process configuration.
*   Field names within a message already have the message context. Do not
    repeat the message name as a prefix on fields.

## See Also

*   [Code Style](code-style.md) — general style rules, API conventions, and
    protobuf message naming
*   [Dev Standards](dev-standards.md) — commit message format and PR
    requirements
