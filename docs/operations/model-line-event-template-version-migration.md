# Model-line event-template version migration

`ModelLine.event_template_version` is the immutable population-field contract
shared by the PDP, EDPA VID labeling and the EDPA results fulfiller. It selects
the fields active in that version from a descriptor set that may contain newer
fields. A descriptor must support the requested version.

The version selects population fields within the CMMS instance's configured
event-message schema. It does not select an event-message type or a descriptor
set for each model line.

## Version resolution

| Repository field | Local config | Result |
| --- | --- | --- |
| Present | Omitted or equal | Use repository version |
| Absent | Positive value | Use local version |
| Absent | Omitted | Fail with a missing-version error |
| Present | Different value | Fail with a configuration mismatch |

A repository read failure does not select the local config. The PDP uses
`PopulationRequisitionFulfillerConfig.model_line_event_template_versions`; the
EDPA dispatcher and monitor use `VidLabelingConfig.model_line_configs[*].event_template_version`.
EDPA pins the resolved version into each WorkItem and carries it through the
memoized labeling phases. The dispatcher and monitor validate descriptor support
before claiming work and must have read access to each configured event-template
descriptor blob.

### EDPA results fulfiller

The results fulfiller resolves the version once per requisition group, for the
model line whose events it reads (the selected model line during a cutover). It
fails the group, which is then retried, if a requisition filter references a
population field that is not present in that version. Its local fallback is the
Terraform map `edpa_model_line_event_template_versions`, which is passed to the
fulfiller as `--event-template-version` in that line's `--model-line` group and
validated against the event descriptor at startup.

The Kingdom `ModelLine.event_template_version` (the repository value) is the
source of truth. Unlike the PDP and VID labeling, the fulfiller does not fail
when a local value differs from the repository version; it logs a warning and
uses the repository version.

| Repository field | Local config | Result |
| --- | --- | --- |
| Present | Omitted, equal or different | Use repository version |
| Absent, or model line not found | Positive value | Use local version |
| Absent, or model line not found | Omitted | Fail with a missing-version error |

Other repository read failures fail the group without selecting the local
config.

## Deployment order

1. Apply the Kingdom Spanner migration adding the nullable `ModelLines.EventTemplateVersion`
   column, then deploy the Kingdom API supporting the field and initialization RPC.
2. Update scripts and clients creating model lines to supply a positive version.
   New public model-line creation requires it. The `ModelRepository` and
   `MeasurementSystem` create commands require `--event-template-version`.
   `ModelRepository populations create` also requires this flag to validate a
   historical population with a newer descriptor set.
3. Configure the PDP fallback for all historical lines it serves. Keep the EDPA
   fallback versions consistent in both dispatcher and monitor configurations.
   Set `edpa_model_line_event_template_versions` for every historical line the
   results fulfiller serves, in the same Terraform change that deploys a results
   fulfiller image supporting `--event-template-version`; an older image rejects
   the flag. Deploy the updated PDP and EDPA consumers.
4. As the owning ModelProvider, initialize each historical line using its
   established population and labeling version:

   ```shell
   ModelRepository <connection-and-TLS-flags> model-lines set-event-template-version \
     --name=<model-line-resource-name> --event-template-version=<established-version>
   ```

   Repeating the same version is idempotent; changing an initialized value fails
   with `FAILED_PRECONDITION`. Use a new model line for a new population contract.
5. Once a line's repository value is verified to match, remove its PDP fallback
   entry, omit its EDPA config version, and remove its
   `edpa_model_line_event_template_versions` entry. Other per-line EDPA settings
   remain required. Existing queued WorkItems retain their resolved versions.

Population validation requires every population attribute active in the selected
version. Population filters referencing population attributes outside that
version are rejected, so newer fields cannot be silently treated as default
values for a historical population.
