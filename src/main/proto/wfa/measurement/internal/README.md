# Internal API Service Definitions

gRPC service definitions for internal APIs, which are those that have access to
underlying databases. All database access is done through these services.

## Conventions

### Keys and IDs

Service implementations may use separate "internal" IDs for database primary
keys. These are not exposed in the API, which is why the ID fields tend to be
prefixed with `external`. Within the API, the unique key to identify an entity
may be made of up multiple external IDs. Unless otherwise stated, each ID
component of the key is only guaranteed to be unique within the scope of its
parent key.

For example, the key of a `Measurement` in the Kingdom internal API is
(`external_measurement_consumer_id`, `external_measurement_id`). The
`external_measurement_id` is only guaranteed to be unique within the parent
`MeasurementConsumer`.

### Details message types

Message types which have a name ending in `Details` may be persisted in the
underlying database using binary serialization. Care must be taken to ensure
that any changes to these message types remain binary-compatible with existing
persisted values.
