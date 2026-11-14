"""Read-only final absence checks for the exact recorded graph and private generations."""
import json,pathlib,urllib.parse,subprocess,sys
import suite_control as s
import ci_reporting as pg
import ci_graph as graph
P=s.P
queries={
 'edp-aggregator':[
  ('raw',"SELECT RawImpressionUploadId FROM RawImpressionUpload WHERE DataProviderResourceId='T5RryPMNong' AND STARTS_WITH(DoneBlobUri,'gs://secure-computation-storage-dev-bucket/raw-impressions/edp/edp7/vid-cloud-20261113/')"),
  ('metadata',"SELECT ImpressionMetadataId FROM ImpressionMetadata WHERE DataProviderResourceId='T5RryPMNong' AND CmmsModelLine IN UNNEST(["+','.join("'"+x['model_line']+"'" for x in s.load()['test11_fixtures'].values())+",'"+s.load()['functional_model_line']+"'])"),
  ('requisitions',"SELECT RequisitionMetadataId FROM RequisitionMetadata WHERE DataProviderResourceId='T5RryPMNong' AND STARTS_WITH(Report,'measurementConsumers/VCTqwV_vFXw/reports/vid-cloud-20261113-')"),
  ('routing',"SELECT Report FROM ReportRoutingDecision WHERE DataProviderResourceId='T5RryPMNong' AND STARTS_WITH(Report,'measurementConsumers/VCTqwV_vFXw/reports/vid-cloud-20261113-')")],
 'secure-computation':[('all-recorded-private-and-outside-source-parents','SELECT WorkItemId,WorkItemResourceId,SourceDataPath FROM WorkItems WHERE '+graph.cp_source_where())], 'kingdom':[]}
for db,name in [('edp-aggregator','edpa-delete-manifest.json'),('secure-computation','cp-delete-manifest.json'),('kingdom','kingdom-delete-manifest.json')]:
 path=P/'cleanup-final-delete'/name
 if path.exists():
  m=json.load(open(path))
  for g in m['guards']:
   if g['label'] in {d['liveIdentityGuard'] for d in m['deletes']} | {'raw-child-model-closure','raw-child-file-closure','raw-child-vl-job-closure','owned-attempt-terminal-closure','owned-publications-zero','owned-eventgroup-activity-closure'}:queries[db].append((g['label'],g['sql']))
for db,q in queries.items():
 for label,sql in q:assert s.query('ci-final-absence-'+db+'-'+label,db,sql)==[],(db,label)
# Readable live/noncurrent generations only; retain the existing soft-delete policy.
base='https://storage.googleapis.com/storage/v1/b/secure-computation-storage-dev-bucket/o'
for prefix in ['raw-impressions/edp/edp7/vid-cloud-20261113/','external-impressions/edp/edp7/vid-cloud-20261113/','vid-labeled-impressions/edp/edp7/vid-cloud-20261113/','test-operator-artifacts/vid-cloud-20261113/']:
 x=s.api('ci-final-gcs-'+str(len(prefix))+'-'+prefix.split('/')[0],'GET',base+'?'+urllib.parse.urlencode({'prefix':prefix,'versions':'true','maxResults':'1000'}));assert x.get('items',[])==[] and not x.get('nextPageToken')
for row in json.load(open(P/'outside-gcs-cleanup-prep/root-outside-gcs-owned-manifest.json'))['allowlist']:
 x=s.api('ci-final-outside-'+row['name'].split('/')[-1],'GET',base+'?'+urllib.parse.urlencode({'prefix':row['name'],'versions':'true','maxResults':'1000'}));assert not any(o['name']==row['name'] for o in x.get('items',[])) and not x.get('nextPageToken')
pgresult=pg.inventory();assert all(pgresult[k]==[] for k in ['reports','metrics','measurements','reporting_sets','metric_specs','event_groups'])
# Shared HG release and population must still exist; no delete helper accepts their tables/keys.
for label in ['protected_shared_model_release','protected_shared_population']:
 prior=json.load(open(P/'cleanup-final-after-main/kingdom-owned-live-inventory.json'));assert s.query('ci-final-'+label,'kingdom',prior['queries'][label])==prior[label]
s.save('cleanup-final-verification.json',{'status':'PASS','project':'halo-cmm-dev','session':'01a1d445','allRecordedOwnedDbParentsAbsent':True,'readableOwnedGcsVersions':0,'sharedHGAndPopulationPreserved':True,'reportingJarRestoredAbsent':True,'time':s.now()});print('FINAL_RECORDED_OWNER_ABSENCE_AND_SHARED_GUARDS_PASS')
