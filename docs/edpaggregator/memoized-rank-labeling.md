# Saved ranks for memoized VID labeling

Phase 1 registers two encrypted rank-index blobs per upload, model line, and subpool:

| Blob | Role |
| --- | --- |
| DAY_ONLY | Saved assignments for the fingerprints processed in this upload. Phase 2 reads these assignments. |
| SNAPSHOT | Cumulative assignments and last-seen dates. Phase 1 reads this state to renew, allocate, or free ranks and recover historical assignments. |

For a supported backfill, the same fingerprint can have historical rank 7 in DAY_ONLY and current rank 42 in SNAPSHOT. The backfill labels from 7; later forward processing preserves 42. Backfill eligibility and historical-snapshot selection still use the upload's maximum event date.

## Input selection and failure

The VidLabelingJob identifies the exact RawImpressionUpload. The labeler selects its nondeleted DAY_ONLY pointers for the requested model line and checks every expected subpool persisted on RawImpressionUploadModelLine. File-batch jobs reuse a cached index only for the same upload and selected subpool blobs.

Missing pointers, incomplete subpool coverage, unreadable objects, invalid encryption metadata, or checksum failures stop the attempt. There is no cumulative-snapshot fallback. An existing, checksum-valid empty DAY_ONLY is valid when the subpool assigned no ranks, for example after overflow; those impressions use the existing unranked path.

Index entry-count metrics describe the upload's saved assignments. They do not measure cumulative pool occupancy. A zero-record blob supplies no ranked-size header; the labeler does not invent a configured size for that observation.

## Retention and recovery

Ordinary retention deletes aged DAY_ONLY only after its model-line execution completes or is invalidated with EVICTED_OUTPUT. Unfinished executions and processing failures retain their labeling inputs. Snapshots continue to supply rank expiry and historical lookup.

Cleanup removes object bytes before soft-deleting metadata so a failed deletion can be retried. Missing or unavailable owner metadata stops deletion. Processing failures retain files until recovery or nonretryable invalidation, which can increase storage for stalled work.

A redelivered, already-SUCCEEDED labeling job skips index reads and resumes its existing completion or availability finalization, even if its daily files have since aged out.

## Upgrade and rollback

1. Check pending and retryable uploads have readable DAY_ONLY pointers for their expected subpools.
2. Hold affected backfill work and drain old Phase-2 executions.
3. Upgrade all VidRankBuilder workers to lifecycle-aware retention before upgrading VidLabeler.
4. Replace and drain old labeler workers before resuming backfills.

The API, rank-file format, and configuration remain compatible. Old and new labelers can emit different VIDs for a backfill, so do not mix them while processing that upload.

Keep retention protection during rollback. Reverting the reader restores the historical-labeling bug; hold backfills until the corrected reader is restored.

Already-completed output is not automatically relabeled. Replaying a SUCCEEDED job preserves its existing output. Use the approved eviction, replacement-upload, and healing workflow to repair identified affected outputs after checking their raw inputs and historical state.

## Scope

This change does not add anchored snapshots, daily replay, or arbitrary out-of-window historical reconstruction. Existing retention-window rejection and historical-rank collision handling remain unchanged.

Issue: [#4011](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/4011).
