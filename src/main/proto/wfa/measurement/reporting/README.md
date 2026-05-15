# Reporting API Definition

This API definition attempts to follow the
[API Improvement Proposals (AIPs)](https://google.aip.dev/) with the following
notable exceptions:

*   List methods which provide filtering use a structured message rather than
    the filtering language defined in
    [AIP-160: Filtering](https://google.aip.dev/160)
*   List methods which provide ordering use a structured message rather than the
    language defined in
    [AIP-132: Standard methods: List](https://google.aip.dev/132#ordering)

## Resource IDs

The format of a resource ID is documented either on the `name` field of the
resource type or on the `{resource}_id` field of the corresponding Create
request.

## Errors

Following [AIP-193](https://google.aip.dev/193), methods result in a gRPC Status
when an API error occurs. In addition to the canonical status code, there may be
an `ErrorInfo` with a reason within the `reporting.halo-cmm.org` domain.

### General Reasons

Any method may return an error with one of the following reasons even if not
listed in the specific method documentation:

*   `REQUIRED_FIELD_NOT_SET`
*   `INVALID_FIELD_VALUE`
