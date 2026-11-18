# Model-line event-template version migration

`ModelLine.event_template_version` is the immutable population-field contract
shared by the PDP and EDPA VID labeling. It selects the fields active in that
version from a descriptor set that may contain newer fields. A descriptor must
support the requested version.

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
memoized labeling phases.

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
   Deploy the updated PDP and EDPA consumers.
4. As the owning ModelProvider, initialize each historical line using its
   established population and labeling version:

   ```shell
   ModelRepository <connection-and-TLS-flags> model-lines set-event-template-version \
     --name=<model-line-resource-name> --event-template-version=<established-version>
   ```

   Repeating the same version is idempotent; changing an initialized value fails
   with `FAILED_PRECONDITION`. Use a new model line for a new population contract.
5. Once a line's repository value is verified to match, remove its PDP fallback
   entry and omit its EDPA config version. Other per-line EDPA settings remain
   required. Existing queued WorkItems retain their resolved versions.

Population validation requires every population attribute active in the selected
version. Population filters referencing population attributes outside that
version are rejected, so newer fields cannot be silently treated as default
values for a historical population.
