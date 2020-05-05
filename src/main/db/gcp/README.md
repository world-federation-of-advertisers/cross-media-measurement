# Cloud Spanner

## Set-Up

### Install `gcloud`

```
$ sudo apt-get install -y \
     google-cloud-sdk \
     google-cloud-sdk-spanner-emulator
```

### Start the emulator

```
$ gcloud beta emulators spanner start
```

### Set up `gcloud`

```
$ gcloud config set auth/disable_credentials true
$ gcloud config set project test-project
$ gcloud config set 'api_endpoint_overrides/spanner' 'http://localhost:9010/'
```

### Make a Spanner instance

```
$ gcloud spanner instances create test-instance \
    --config=emulator-config \
    --description='Test Instance' \
    --nodes=3
```

### Make a Spanner database

Note that the emulator doesn't support foreign keys,
so we need to strip them out.

We also strip out comments.

```
$ gcloud spanner databases \
    create local_measurement_providers \
    --instance=test-instance \
    --ddl="$(cat src/main/db/gcp/measurement_provider.sdl \
             | sed -E 's/(--|CONSTRAINT|FOREIGN|REFERENCES).*$//g')"
```

### Using it

Now, you can run some sample operations with `gcloud spanner`
or connect to the database in code using `localhost:9010`.