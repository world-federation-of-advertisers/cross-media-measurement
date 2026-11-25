# Entity metadata search rollout

`metadata_search_query` can search the existing brand/campaign metadata,
flattened `entity_metadata` values, and `EntityType`. The latter two sources are
enabled by the Kingdom data server's `--enable-entity-metadata-search` flag,
which defaults to false. The prototype combines these tokens with ancestor
entity-metadata tokens in active hierarchies, within the same DataProvider and
MeasurementConsumer. Search applies to the combined values before ordering and
pagination; ancestor entity types and legacy metadata are not inherited.

The values representation recursively includes string, number, and boolean
values from structs and lists. It excludes field names, nulls, and unset
values. Each scalar is a separate array element; booleans use lowercase text
and numbers use the protobuf double's string representation. Struct fields are
traversed in sorted key order and list order is preserved.

## Inherited-search prototype

`SpannerInheritedMetadataSearchTest` seeds normalized metadata and hierarchy
paths, then calls the EventGroups service. It covers cross-level terms, native
search operators, isolation, pagination, and ancestor renames without leaf
rewrites. Existing schema columns are reused.

The query is marked `DO_NOT_SUBMIT`: emulator results do not establish that
Spanner can execute or index cross-row `TOKENLIST_CONCAT`. Validate the query
plan and performance on real Spanner before adopting this approach. Normalized
writes, inherited read responses, and migration remain separate work. Keep the
search flag disabled for this prototype; the rollout below is not sufficient
for enabling inherited search.

## Deployment order

1. Apply the additive schema from [PR #4639](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/4639)
   and wait for its DDL operation, including the search index, to complete.
   `EntityMetadataValues`, `EntityMetadata_Tokens`, `EntityType_Tokens`, and
   `EventGroupsByEntityMetadata` must exist before deploying these writers.
   Keep `Metadata_Tokens` and `EventGroupsByMetadata` available.
2. Deploy dual writes to **all** Kingdom data server replicas, keeping
   `--enable-entity-metadata-search=false`. Single and batch creates/updates
   write the values array in the same mutation as `EntityMetadata`. Absent or
   cleared metadata produces an empty array rather than NULL.
3. Run `BackfillEntityMetadataValues` against the same database, first with
   `--dry-run` to count missing rows in a single read-only query, then without
   it to write the values. The binary target is
   `//src/main/kotlin/org/wfanet/measurement/kingdom/deploy/gcloud/spanner/tools:BackfillEntityMetadataValues`.
   It requires `--spanner-project`, `--spanner-instance`, and
   `--spanner-database`; obtain the exact deployment identifiers before
   constructing an execution command. `--batch-size` defaults to 100; lower it
   if large rows cause transaction-size limits or contention. Progress and
   completion counts are appended to `entity-metadata-values-backfill.log` by
   default; use `--log-file` to choose a different local path.
4. Verify that no rows have NULL `EntityMetadataValues`, including deleted
   rows, using the following read-only SQL. A second backfill run must report
   zero eligible rows.

   ```sql
   SELECT COUNT(*) AS MissingEntityMetadataValues
   FROM EventGroups
   WHERE EntityMetadataValues IS NULL;
   ```

5. Enable `--enable-entity-metadata-search=true` on the data servers. Check
   existing campaign/brand queries, entity value queries, entity type queries,
   and key-only queries. Existing metadata must still match; a key alone must
   not match unless it also appears as a searchable value or entity type.

Local entity-metadata search does not require hierarchy activation. Inherited
search requires an active hierarchy and normalized metadata on its owners.

## Backfill and rollback behavior

The backfill selects only NULL arrays. It includes rows with absent/empty
metadata and deleted rows, and leaves populated arrays untouched. It does not
change the protobuf, state, create time, or update time. Each bounded batch
reads the current protobuf and writes its values in one read-write transaction;
concurrent updates cause transaction conflict handling rather than stale writes.
The primary-key cursor advances only after a successful commit. An interrupted
run can be restarted from the beginning; completed rows are skipped. Concurrent
backfill instances are safe but increase contention.

All writers must support dual writes before the backfill starts: an older
writer can otherwise change a protobuf while leaving a populated array stale.
To roll back expanded reads, set the flag to false and retain dual writes and
the additive schema. If old writers must be restored, reconcile every affected
row before enabling expanded reads again; the NULL-only backfill does not
repair populated stale arrays.

## Cost estimate

No new provisioned compute is required by this implementation. Incremental
compute is approximately **$0/month if the existing capacity is sufficient**;
the backfill temporarily consumes that capacity. Arrays and the additive search
index increase storage and write work. As an illustrative estimate, **10 GiB of
additional total SSD storage in us-central1 costs about $3/month**, using the
published $0.30/GiB-month base configuration example. Backups and cross-region
replication can add charges. The deployment's region, edition, measured index
size, backup policy, and any capacity increase are needed for its actual monthly
estimate. See [Spanner pricing](https://cloud.google.com/spanner/pricing).
