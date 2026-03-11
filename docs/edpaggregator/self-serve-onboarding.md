# Self-Serve Onboarding Guide

This guide explains how EDP Aggregator operators link Measurement Consumers to Client Accounts, enabling automatic event group registration.

## Overview

The self-serve flow allows operators to map advertiser accounts (Client Account Reference IDs) to Measurement Consumers. Once linked, the EventGroupSync function automatically registers event groups for campaigns associated with those accounts.

### Flow Summary

1. EDPs upload event groups with `client_account_reference_id` field
2. Market Operator creates a `MeasurementConsumer` resource for the advertiser
3. Operator creates `ClientAccount` resources under the `MeasurementConsumer` via
   the `ClientAccounts.BatchCreateClientAccounts` API (the CLI provides a
   convenient wrapper for this)
4. Trigger EventGroupSync to register event groups to the Kingdom

## Prerequisites

- Access to the `MeasurementSystem` CLI
- TLS certificates for Kingdom API authentication
- API key for the Measurement Consumer
- GCS access to event groups blob (for brand-based operations)

## Step 1: Link Client Accounts to Measurement Consumer

Use the `client-accounts create` command to establish the link.

### Option A: Link by Client Account Reference ID(s)

```shell
MeasurementSystem \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  client-accounts \
  --api-key=<MC_API_KEY> \
  create \
  --parent=measurementConsumers/<MC_ID> \
  --data-provider=dataProviders/<DP_ID> \
  --client-account-reference-id=<ACCOUNT_ID_1> \
  --client-account-reference-id=<ACCOUNT_ID_2>
```

### Option B: Link All Accounts for a Brand

```shell
MeasurementSystem \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  client-accounts \
  --api-key=<MC_API_KEY> \
  create \
  --parent=measurementConsumers/<MC_ID> \
  --data-provider=dataProviders/<DP_ID> \
  --brand="<BRAND_NAME>" \
  --event-groups-blob-uri=gs://<BUCKET>/<PATH>/event-groups.binpb
```

## Step 2: Trigger EventGroupSync

After linking accounts, trigger the EventGroupSync Cloud Function to register event groups with the Kingdom.

### Using curl

```shell
# Step 1: Get ID token (audience must match function URL)
ID_TOKEN=$(gcloud auth print-identity-token \
  --audiences="https://<REGION>-<PROJECT>.cloudfunctions.net/event-group-sync")

# Step 2: Call the function
curl -X POST "https://<REGION>-<PROJECT>.cloudfunctions.net/event-group-sync" \
  -H "Authorization: Bearer $ID_TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-DataWatcher-Path: gs://<BUCKET>/<EDP>/event-groups/<FILE>.binpb" \
  -d '{
    "dataProvider": "dataProviders/<DP_ID>",
    "eventGroupsBlobUri": "gs://<BUCKET>/<EDP>/event-groups/<FILE>.binpb",
    "eventGroupMapBlobUri": "gs://<BUCKET>/<EDP>/event-groups-map/<FILE>.binpb",
    "cmmsConnection": {
      "certFilePath": "/secrets/cert/<EDP>_tls.pem",
      "privateKeyFilePath": "/secrets/key/<EDP>_tls.key",
      "certCollectionFilePath": "/secrets/ca/kingdom_root.pem"
    },
    "eventGroupStorage": {
      "gcs": {
        "projectId": "<PROJECT>",
        "bucketName": "<BUCKET>"
      }
    },
    "eventGroupMapStorage": {
      "gcs": {
        "projectId": "<PROJECT>",
        "bucketName": "<BUCKET>"
      }
    }
  }'
```

### Using gcloud

```shell
gcloud functions call event-group-sync \
  --project=<PROJECT> \
  --region=<REGION> \
  --data='{
    "dataProvider": "dataProviders/<DP_ID>",
    "eventGroupsBlobUri": "gs://<BUCKET>/<EDP>/event-groups/<FILE>.binpb",
    "eventGroupMapBlobUri": "gs://<BUCKET>/<EDP>/event-groups-map/<FILE>.binpb",
    "cmmsConnection": {
      "certFilePath": "/secrets/cert/<EDP>_tls.pem",
      "privateKeyFilePath": "/secrets/key/<EDP>_tls.key",
      "certCollectionFilePath": "/secrets/ca/kingdom_root.pem"
    },
    "eventGroupStorage": {
      "gcs": {
        "projectId": "<PROJECT>",
        "bucketName": "<BUCKET>"
      }
    },
    "eventGroupMapStorage": {
      "gcs": {
        "projectId": "<PROJECT>",
        "bucketName": "<BUCKET>"
      }
    }
  }'
```

## Step 3: Verify Event Groups

List event groups to verify registration:

```shell
MeasurementSystem \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  event-groups \
  --api-key=<MC_API_KEY> \
  list \
  --parent=dataProviders/<DP_ID> \
  --measurement-consumer=measurementConsumers/<MC_ID>
```

## Unlinking Client Accounts

To unlink accounts (e.g., when an advertiser removes consent):

### By Reference ID(s)

```shell
MeasurementSystem \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  client-accounts \
  --api-key=<MC_API_KEY> \
  delete \
  --parent=measurementConsumers/<MC_ID> \
  --data-provider=dataProviders/<DP_ID> \
  --client-account-reference-id=<ACCOUNT_ID_1> \
  --client-account-reference-id=<ACCOUNT_ID_2>
```

### By Brand

```shell
MeasurementSystem \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  client-accounts \
  --api-key=<MC_API_KEY> \
  delete \
  --parent=measurementConsumers/<MC_ID> \
  --data-provider=dataProviders/<DP_ID> \
  --brand="<BRAND_NAME>" \
  --event-groups-blob-uri=gs://<BUCKET>/<PATH>/event-groups.binpb
```

## Listing Client Accounts

View existing links:

```shell
MeasurementSystem \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  client-accounts \
  --api-key=<MC_API_KEY> \
  list \
  --parent=measurementConsumers/<MC_ID> \
  --data-provider=dataProviders/<DP_ID>
```

## Configuration Parameters Reference

| Parameter | Description |
|-----------|-------------|
| `--parent` | Measurement Consumer resource name (`measurementConsumers/<ID>`) |
| `--data-provider` | Data Provider resource name (`dataProviders/<ID>`) |
| `--client-account-reference-id` | External account ID (max 36 chars), can be repeated |
| `--brand` | Brand name to filter event groups by |
| `--event-groups-blob-uri` | GCS URI to event groups file (`.binpb` or `.json`) |

## EventGroupSync JSON Payload Reference

| Field | Description |
|-------|-------------|
| `dataProvider` | Data Provider resource name |
| `eventGroupsBlobUri` | GCS URI to event groups blob |
| `eventGroupMapBlobUri` | GCS URI to event group map blob |
| `cmmsConnection.certFilePath` | Path to EDP TLS certificate (mounted in Cloud Function) |
| `cmmsConnection.privateKeyFilePath` | Path to EDP TLS private key |
| `cmmsConnection.certCollectionFilePath` | Path to Kingdom root CA certificate |
| `eventGroupStorage` | GCS config for event group storage |
| `eventGroupMapStorage` | GCS config for event group map storage |
