# Access API Definition

This API definition attempts to follow the
[API Improvement Proposals (AIPs)](https://google.aip.dev/) with the following
notable exceptions:

*   This is a gRPC-only API with no HTTP annotations.

## Resource IDs

The format of a resource ID is documented either on the `name` field of the
resource type or on the `{resource}_id` field of the corresponding Create
request. Resource IDs may never be UUIDs nor appear to look like UUIDs.

Resource IDs are immutable; they cannot be changed after resource creation.

## Errors

Following [AIP-193](https://google.aip.dev/193), methods result in a gRPC Status
when an API error occurs. In addition to the canonical status code, there may be
an `ErrorInfo` with a reason within the `access.halo-cmm.org` domain.

### General Reasons

Any method may return an error with one of the following reasons even if not
listed in the specific method documentation:

*   `REQUIRED_FIELD_NOT_SET`
*   `INVALID_FIELD_VALUE`
