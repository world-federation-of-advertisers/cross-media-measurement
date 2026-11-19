# Operations

Guides for operating a Kingdom or Duchy.

*   [Creating Resources](creating-resources.md)
*   [Healing VID-labeling uploads](healing-vid-labeling-uploads.md)
*   [Validating Reporting writes](validate-reporting-writes.md)

## Operator EventGroup synchronization

Apply the Kingdom schema migration before enabling `SyncEventGroups`. Give the
public API server `--event-group-sync-operator=<principal resource name>` and
`--event-group-sync-token-keyset-file=<binary Tink AEAD keyset>`. Map the operator's
client certificate to that principal in the existing authority-key map. An
ordinary client has no global access without this explicit grant.

Share the keyset across replicas and preserve old keys during rotation. Page
tokens expire after 15 minutes; completed sync tokens expire after 30 days.
Deleted EventGroup rows must remain available for that checkpoint lifetime.
