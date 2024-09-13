# Kingdom Spanner Changelog

Liquibase changelog for the Kingdom Spanner database.

## Protobuf Changes

If the definition of any
[Details message](../../../proto/wfa/measurement/internal/README.md#details-message-types)
in the API changes, it must be accompanied by a changelog entry with the
appropriate `PROTO BUNDLE` DDL statements. See
[Work with protocol buffers in GoogleSQL](https://cloud.google.com/spanner/docs/reference/standard-sql/protocol-buffers).

The serialized `FileDescriptorSet` can be obtained from the
`//src/main/proto/wfa/measurement/internal/kingdom:details_descriptor_set` Bazel
build target. The base64-encoded value of the output can be passed in a `SET
PROTO_DESCRIPTORS` statement, which will apply to the following DDL batch. See
[`create-proto-bundle.sql`](create-proto-bundle.sql) for an example.
