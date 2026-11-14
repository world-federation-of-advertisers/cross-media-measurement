import json,sys,os,pathlib,subprocess,shutil,hashlib,urllib.parse,time,uuid,urllib.request
import suite_control as s
import ci_graph as graph
import ci_reporting as pg
P=s.P;R=pathlib.Path(s.R);MODE=os.environ['CI_CLEANUP_MODE'];OUT=P/'noncredential-receipts'
def read(path):return json.load(open(path))

def sha(path):return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()

def child(path,*args):subprocess.run([sys.executable,str(P/path),*args],check=True)

def snapshot():
 child('cleanup-inventory-review/capture-final-spanner-sdk.py','after-main');cp=graph.cp_refresh();e=read(P/'cleanup-final-after-main/edpa-owned-live-inventory.json');leaseids={x['synchronizationAttemptId'] for x in cp['dataAvailabilityLeaseOwnerClosure']};assert all(x['State']=='2' for x in e['leases_review_only'] if x['SynchronizationAttemptId'] in leaseids);assert not e['fences_review_only'];assert all(x['State']=='2' for x in e['healing']);child('cleanup-inventory-review/capture-final-kingdom.py','after-main')
 frozen=read(P/'cleanup-final-before-main/edpa-owned-live-inventory.json')
 for key,cols in [('raw_uploads',['DataProviderResourceId','RawImpressionUploadId']),('raw_models',['DataProviderResourceId','RawImpressionUploadId','RawImpressionUploadModelLineId']),('metadata',['DataProviderResourceId','ImpressionMetadataId']),('requisitions',['DataProviderResourceId','RequisitionMetadataId']),('routing',['DataProviderResourceId','Report']),('corrections',['DataProviderResourceId','RawImpressionUploadCorrectionCandidateId']),('healing',['DataProviderResourceId','UploadHealingOperationId']),('jobs',['DataProviderResourceId','RawImpressionUploadId','VidLabelingJobId'])]:
  assert {tuple(r[c] for c in cols) for r in e[key]}<={tuple(r[c] for c in cols) for r in frozen[key]},'Unknown new EDPA keys preserved: '+key
 # No new parent keys beyond the final recorded task scope.
 old=read(P/'cleanup-final-before-main/kingdom-owned-live-inventory.json');new=read(P/'cleanup-final-after-main/kingdom-owned-live-inventory.json')
 for key,cols in [('model_lines',['ModelProviderId','ModelSuiteId','ModelLineId']),('rollouts',['ModelProviderId','ModelSuiteId','ModelLineId','ModelRolloutId']),('availability',['DataProviderId','ModelProviderId','ModelSuiteId','ModelLineId']),('event_groups',['DataProviderId','EventGroupId']),('model_shard',['DataProviderId','ModelShardId']),('model_release',['ModelProviderId','ModelSuiteId','ModelReleaseId']),('measurements',['MeasurementConsumerId','MeasurementId'])]:
  oldkey='measurements' if 'measurements' in old else 'measurements_from_own_requisitions';prior=old[oldkey if key=='measurements' else key];assert {tuple(r[c] for c in cols) for r in new[key]}<={tuple(r[c] for c in cols) for r in prior},key
 for db in ['edp-aggregator','secure-computation','kingdom']:
  value=s.api('ci-final-'+db+'-ddl','GET','https://spanner.googleapis.com/v1/projects/halo-cmm-dev/instances/dev-instance/databases/'+db+'/ddl');s.save('cleanup-inventory-review/'+db+'-live-ddl.json',value)

def bootstrap_source_config():
 source=P/'cleanup-inventory-review/main-config-source';provenance=read(source/'source-provenance.json')
 assert provenance['mainSha']=='29502c6d9b97af1c7af2a46587d7d3265edfb1db'
 assert provenance['repository']=='world-federation-of-advertisers/cross-media-measurement' and provenance['sourceDirectory']=='src/main/terraform/gcloud/cmms/envs/dev'
 for item in provenance['files']:
  raw=(source/item['name']).read_bytes();assert sha(source/item['name'])==item['sha256']
  assert hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()==item['gitBlobSha']

def main_check():
 bootstrap_source_config();child('cleanup-inventory-review/verify-main-restoration.py')
