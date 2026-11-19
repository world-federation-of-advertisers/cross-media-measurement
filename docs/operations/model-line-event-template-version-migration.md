# Model-line event-template version migration

`ModelLine.event_template_version` records the immutable event-template version
under which a model line's population and VID labels were defined.
Historical model lines may omit it until the owning ModelProvider initializes it.

## Deploy the repository API

1. Apply the Kingdom Spanner migration adding the nullable
   `ModelLines.EventTemplateVersion` column, then deploy the Kingdom API
   supporting the field and initialization RPC.
2. Update scripts and clients creating model lines to supply a positive version.
   New public model-line creation requires it. The `ModelRepository` and
   `MeasurementSystem` create commands require `--event-template-version`.
3. Historical lines remain readable without a version. Consumers can migrate
   separately while preserving the population contract established for each line.

## Initialize historical model lines

As the owning ModelProvider, initialize a historical line with the version
under which its PopulationSpec and VID labels were defined:

```shell
ModelRepository <connection-and-TLS-flags> model-lines set-event-template-version \
  --name=<model-line-resource-name> --event-template-version=<established-version>
```

Repeating the same version is idempotent; changing an initialized value fails
with `FAILED_PRECONDITION`. Use a new model line for a new population contract.
When migrating consumers that have local per-line versions, initialize repository
metadata with the same established version before removing those local entries.
