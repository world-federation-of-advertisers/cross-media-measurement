# EventGroup hierarchy mutations

## First child, re-parenting, and deletion

When a matching ordinary EventGroup gains its first child, Kingdom derives the
ancestor in the existing row. The external ID, provider, and creation time stay
the same. The ancestor takes its media types and availability from its direct
children and uses the metadata defined by its hierarchy path. A conflicting path
or shared metadata value rejects the entire write. A derived ancestor rejects
direct writes while it has children.

Re-parenting updates the source and destination ancestor chains in the same
transaction as the leaf move. Deletion recomputes from the remaining direct
children. When the last child leaves, the former ancestor becomes writable and
retains its last derived fields. The walk stops on a chain once its contribution
to the next parent is unchanged.

## Shared metadata rename design

Use the same stage-and-publish operation for any subtree size, including more
than 50 leaves. Starting a rename takes a stable operation ID, the ancestor
resource name, and replacement values for fields owned by that hierarchy
level. Validate the ancestor, its hierarchy state, and those fields in the
transaction that locks the subtree. Preserve omitted fields, entity keys, and
paths.

The worker stages the ancestor and descendants in bounded transactions. Each
transaction writes its checkpoint with its staged rows. A single publish
transaction makes the completed version visible to Get, List, and metadata
search, then releases the lock. Fresh reads see the old values until publish
commits. The same snapshot determines search membership and returned metadata.
The lock rejects overlapping writes and moves across either boundary of the
subtree. Unrelated subtrees remain writable. Hierarchy activation and metadata
publication for the same provider exclude each other.

Reuse the operation ID after a lost response and reject reuse for a different
request. Claim running work with a new worker token and resume from the
committed checkpoint. Return the saved publication sequence for completed
work. Cancellation or terminal failure before publication preserves the old
version and releases the lock. Start a new operation after either outcome.

### Read and search pagination

Start a strong Spanner read for the first List or search page. Use the same
read timestamp and the last sort key for each continuation so a rename
published between pages preserves result membership and metadata.
Bind the timestamp and cursor to the parent, query, filters, view, and sort
order in an integrity-protected token. Reauthorize each continuation and
require a fresh listing once the snapshot expires.

[Spanner search pagination](https://docs.cloud.google.com/spanner/docs/full-text-search/paginate-search-results)
