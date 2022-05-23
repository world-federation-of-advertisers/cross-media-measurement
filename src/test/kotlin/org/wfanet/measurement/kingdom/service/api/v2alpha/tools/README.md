# Simple Report CLI tool

The Simple Report Tool can be used to create, list or get a `Measurement` by 
calling the public Kingdom API.

## Simple Report

Running the tool with the `help` command will provide more information on
command-line options. You can also pass another command name to the `help`
command for usage information for that particular command. For example, for help
on the `create` command used for creating a `Measurement`, pass
`help create`.

You'll need to specify the public API target using the `--api-target` option.

You'll also need to specify a TLS client certificate and key using the
`--tls-cert-file` and `--tls-key-file` options, respectively. The issuer of this
certificate must be trusted by the Kingdom server, i.e. the issuer
certificate must be in that server's trusted certificate collection file.

### Create

To create a `Measurement`, the user needs to provide values of the `Measurement` 
by arguments

`Measurement` is a nested structure contains a list of `DataProviderEntry`,
while `DataProviderEntry` contains a list of `EventGroupEntry`. To specify these
value via CLI arguments, the inputs should be grouped.

See the [Examples](##Examples) section below.

### List


### Get

## Examples

This assumes that you have built the `SimpleReport` target, which outputs to
`bazel-bin` by default. For brevity, the examples to not include the full path
to the executable.

* Create

  Assuming you have a private key for `MeasurementConsumer` containing
  [`mc_cs_private.der`](../../../../../../../k8s/testing/secretfiles/mc_cs_private.tink)
  at `secretfiles/mc_cs_private.pb`.
  
  Given that the `Measurement` contains 2 `DataProviderEntries` of `dataProviders/1` 
  and `dataProviders/2`. `dataProviders/1` has two `EventGroups`
  `dataProviders/1/eventGroups/1` and `dataProviders/1/eventGroups/2` while 
  `dataProviders/2` contains `dataProviders/2/eventGroups/1`. The order of 
  options within a group does not matter.

  ```shell
  SimpleReport 
  --tls-cert-file=secretfiles/mc_tls.pem --tls-key-file=secretfiles/mc_tls.key
  --cert-collection-file=secretfiles/kingdom_root.pem
  --api-target=localhost:8443
  --api-cert-host=localhost
  create
  --measurement-consumer-name=measurementConsumers/777
  --private-key-der-file=secretfiles/mc_cs_private.der
  --measurement-ref-id=9999
  --data-provider-name=dataProviders/1
  --event-group-name=dataProviders/1/eventGroups/1 --event-filter-expression=abcd
  --event-filter-start-time=100 --event-filter-end-time=200
  --event-group-name=dataProviders/1/eventGroups/2 --event-filter-expression=efgh
  --event-filter-start-time=300 --event-filter-end-time=400
  --data-provider-name=dataProviders/2
  --event-group-name=dataProviders/2/eventGroups/1 --event-filter-expression=ijk
  --event-filter-start-time=400 --event-filter-end-time=500
  ```