import sys,json,re,hashlib
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent));import suite_control as s
import ci_graph as graph
P=s.P/'cleanup-inventory-review';F=s.P/'cleanup-final-after-main';OUT=s.P/'cleanup-final-delete';OUT.mkdir(exist_ok=True)
def read(p):return json.load(open(p))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def lit(v):return "'"+str(v).replace("'","''")+"'"
def ary(v):return '['+','.join(lit(x) for x in v)+']'
frozenE=read(s.P/'cleanup-final-before-main/edpa-owned-live-inventory.json');frozenK=read(s.P/'cleanup-final-before-main/kingdom-owned-live-inventory.json');frozenCP=read(s.P/'cleanup-final-before-main/cp-owned-complete-union.json')
e=read(F/'edpa-owned-live-inventory.json');k=read(F/'kingdom-owned-live-inventory.json');cp=read(s.P/'outside-gcs-cleanup-prep/root-owned-complete-cp-union-fresh.json')
assert cp['unknownOwnership']==[] and cp['publicationCount']==0
models=e['models'];ledger=s.load();authoritativeModels={v['model_line'] for v in ledger['test11_fixtures'].values()}|{ledger['functional_model_line']};assert len(models)==19 and set(models)==authoritativeModels
anchors=read(P/'root-reporting-ownership-anchors.json');assert anchors['gaps']==[];reports={r['apiReport'] for r in anchors['reports']};assert all(r.startswith('measurementConsumers/VCTqwV_vFXw/reports/vid-cloud-20261113-') for r in reports)
DP="DataProviderResourceId='T5RryPMNong'";RAW='gs://secure-computation-storage-dev-bucket/raw-impressions/edp/edp7/vid-cloud-20261113/';REPORT='measurementConsumers/VCTqwV_vFXw/reports/vid-cloud-20261113-';ownraw='SELECT RawImpressionUploadResourceId FROM RawImpressionUpload WHERE '+DP+' AND STARTS_WITH(DoneBlobUri,'+lit(RAW)+')'
leases=[x['synchronizationAttemptId'] for x in cp['dataAvailabilityLeaseOwnerClosure']];assert len(leases)==len(set(leases));cpids=','.join(str(int(x['db']['WorkItemId'])) for x in cp['parents'])
restore=read(s.P/'cleanup-restoration-acceptance.json');rawabsence=read(s.P/'cleanup-raw-done-absence.json');assert restore['status']=='PASS' and rawabsence['readableRawDoneVersions']==0
pk={}
for db in ['edp-aggregator','secure-computation','kingdom']:
 assert (P/(db+'-live-ddl.json')).is_file(), 'Missing actual DDL '+db
 d=read(P/(db+'-live-ddl.json'))
 pk[db]={re.search(r'CREATE TABLE (\w+)',x).group(1):re.search(r'PRIMARY KEY\(([^)]*)\)',x).group(1).replace(' ','').split(',') for x in d['statements'] if x.startswith('CREATE TABLE ')}
def capture(db,label,sql):
 rows=s.query('cleanup-delete-'+db+'-'+label,db,sql);raw=read(s.P/('cleanup-delete-'+db+'-'+label+'.json'));return rows,{'label':label,'sql':sql,'expectedFields':raw['metadata']['rowType']['fields'],'expectedRows':raw.get('rows',[])}
def build(db):return {'project':'halo-cmm-dev','session':'01a1d445','kind':'EXACT_OWNED_SPANNER_GRAPH','database':db,'mainRestoreVerified':True,'mainRestoreProofSha256':sha(s.P/'cleanup-restoration-acceptance.json'),'rawDoneReadableAbsent':True,'rawDoneAbsenceProofSha256':sha(s.P/'cleanup-raw-done-absence.json'),'deletes':[],'guards':[],'capturedAt':s.now()}
def guard(m,label,sql,empty=False):
 rows,g=capture(m['database'],label,sql);assert not empty or not rows,(label,rows);m['guards'].append(g);return rows
catalog="SELECT TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE INDEX_NAME='PRIMARY_KEY' ORDER BY TABLE_NAME,ORDINAL_POSITION"
EDPA_FROZEN_TABLE={'DataAvailabilitySyncLease':'leases_review_only','RequisitionMetadata':'requisitions','ReportRoutingDecision':'routing','RawImpressionUploadCorrectionCandidate':'corrections','UploadHealingOperation':'healing','ImpressionMetadata':'metadata','RawImpressionUpload':'raw_uploads'}
KINGDOM_FROZEN_TABLE={'Measurements':'measurements','DataProviderAvailabilityIntervals':'availability','EventGroups':'event_groups','ModelRollouts':'rollouts','ModelLines':'model_lines','ModelShards':'model_shard','ModelReleases':'model_release'}
def frozen_subset(db,table,rows):
 cols=pk[db][table]
 if db=='edp-aggregator':
  if table=='VidLabelingEvictionFenceMutation':
   allowed={(str(r['DataProviderResourceId']),str(r['RequestId'])) for r in rows if r['RequestId'] in fenceids};assert {tuple(str(r[c]) for c in cols) for r in rows}<=allowed;return
  prior=frozenE[EDPA_FROZEN_TABLE[table]]
 elif db=='kingdom':prior=frozenK[KINGDOM_FROZEN_TABLE[table]]
 elif table=='WorkItems':prior=[x['db'] for x in frozenCP['parents']]
 else:raise AssertionError('No frozen exact ownership mapping '+table)
 assert {tuple(str(r[c]) for c in cols) for r in rows}<={tuple(str(r[c]) for c in cols) for r in prior},'Unknown new delete PK preserved: '+table
def add(m,table,label,sql,owner,proof):
 rows=guard(m,label,sql)
 for r in rows:assert owner(r),(label,r)
 frozen_subset(m['database'],table,rows)
 keys=[[str(r[c]) for c in pk[m['database']][table]] for r in rows]
 assert len(keys)==len({json.dumps(x) for x in keys});m['deletes'].append({'table':table,'primaryKeyColumns':pk[m['database']][table],'keys':keys,'expectedParentCount':len(keys),'ownershipProofSha256':sha(proof),'liveIdentityGuard':label});return rows
# EDPA graph. Exact task URI/model/report and typed-CP lease roots only.
m=build('edp-aggregator');guard(m,'live-primary-key-schema',catalog)
add(m,'DataAvailabilitySyncLease','owned-dsa-leases','SELECT DataProviderResourceId,SynchronizationAttemptId,CAST(State AS INT64) State,ExpireTime,MutationRequestIds FROM DataAvailabilitySyncLease WHERE '+DP+' AND SynchronizationAttemptId IN UNNEST('+ary(leases)+')',lambda r:r['DataProviderResourceId']=='T5RryPMNong' and r['SynchronizationAttemptId'] in leases,s.P/'outside-gcs-cleanup-prep/root-owned-complete-cp-union-fresh.json')
# Exact fence RequestIds are derived by production RequestIds, prepared separately.
fenceproof=read(OUT/'owned-fence-mutation-ids.json');fenceids=fenceproof['requestIds'];assert fenceproof['unknownOwnedOperations']==[]
add(m,'VidLabelingEvictionFenceMutation','owned-fence-mutation-history','SELECT DataProviderResourceId,RequestId,RequestFingerprint,ResultEtag,NewlyAcquired FROM VidLabelingEvictionFenceMutation WHERE '+DP+' AND RequestId IN UNNEST('+ary(fenceids)+')',lambda r:r['RequestId'] in fenceids,OUT/'owned-fence-mutation-ids.json')
add(m,'RequisitionMetadata','owned-requisitions','SELECT DataProviderResourceId,RequisitionMetadataId,RequisitionMetadataResourceId,CmmsRequisition,BlobUri,GroupId,Report,WorkItem,CAST(State AS INT64) State FROM RequisitionMetadata WHERE '+DP+' AND STARTS_WITH(Report,'+lit(REPORT)+')',lambda r:r['Report'] in reports and r['WorkItem'].split('/')[-1] in {p['db']['WorkItemResourceId'] for p in cp['parents']},F/'edpa-owned-live-inventory.json')
add(m,'ReportRoutingDecision','owned-routing-decisions','SELECT * FROM ReportRoutingDecision WHERE '+DP+' AND STARTS_WITH(Report,'+lit(REPORT)+')',lambda r:r['Report'] in reports and r['ExternalModelLine'] in models and r['SelectedModelLine'] in models,F/'edpa-owned-live-inventory.json')
add(m,'RawImpressionUploadCorrectionCandidate','owned-corrections','SELECT DataProviderResourceId,RawImpressionUploadCorrectionCandidateId,RawImpressionUploadResourceId,UploadHealingOperationId,CAST(State AS INT64) State FROM RawImpressionUploadCorrectionCandidate WHERE '+DP+' AND RawImpressionUploadResourceId IN ('+ownraw+')',lambda r:r['RawImpressionUploadResourceId'] in {x['RawImpressionUploadResourceId'] for x in e['raw_uploads']},F/'edpa-owned-live-inventory.json')
add(m,'UploadHealingOperation','owned-completed-heals','SELECT DataProviderResourceId,UploadHealingOperationId,LabeledImpressionsBlobPrefix,State,ResumeState,RawImpressionUploadCorrectionCandidateIds,MutationRequestIds FROM UploadHealingOperation WHERE '+DP+' AND STRPOS(LabeledImpressionsBlobPrefix,\'vid-cloud-20261113\')>0',lambda r:r['State']=='2' and r['LabeledImpressionsBlobPrefix'].startswith('gs://secure-computation-storage-dev-bucket/vid-labeled-impressions/edp/edp7/vid-cloud-20261113/'),F/'edpa-owned-live-inventory.json')
add(m,'ImpressionMetadata','owned-impression-metadata','SELECT DataProviderResourceId,ImpressionMetadataId,ImpressionMetadataResourceId,BlobUri,CmmsModelLine,RawImpressionUploadResourceId,OutputDoneBlobGeneration,MetadataBlobGeneration,ImpressionsBlobGeneration,CAST(State AS INT64) State FROM ImpressionMetadata WHERE '+DP+' AND CmmsModelLine IN UNNEST('+ary(models)+')',lambda r:r['CmmsModelLine'] in models and any(r['BlobUri'].startswith('gs://secure-computation-storage-dev-bucket/'+p) for p in ['external-impressions/edp/edp7/vid-cloud-20261113/','vid-labeled-impressions/edp/edp7/vid-cloud-20261113/']),F/'edpa-owned-live-inventory.json')
add(m,'RawImpressionUpload','owned-raw-registration-parents','SELECT DataProviderResourceId,RawImpressionUploadId,RawImpressionUploadResourceId,DoneBlobUri,DoneBlobGeneration,CAST(State AS INT64) State,RegistrationComplete,EvictionOperationId,CorrectionCandidateId FROM RawImpressionUpload WHERE '+DP+' AND STARTS_WITH(DoneBlobUri,'+lit(RAW)+')',lambda r:r['DoneBlobUri'].startswith(RAW),F/'edpa-owned-live-inventory.json')
rawids=','.join(str(int(r['RawImpressionUploadId'])) for r in e['raw_uploads']) or 'NULL';childscope=DP+' AND RawImpressionUploadId IN ('+rawids+')'
childmodels=guard(m,'raw-child-model-closure','SELECT DataProviderResourceId,RawImpressionUploadId,RawImpressionUploadModelLineId,CmmsModelLine FROM RawImpressionUploadModelLine WHERE '+childscope);assert all(r['CmmsModelLine'] in models for r in childmodels)
assert {tuple(str(r[c]) for c in pk['edp-aggregator']['RawImpressionUploadModelLine']) for r in childmodels}<={tuple(str(r[c]) for c in pk['edp-aggregator']['RawImpressionUploadModelLine']) for r in frozenE['raw_models']}
fileraw=read(s.P/'cleanup-final-before-main/raw-file-source-identity.json');filecols=[x['name'] for x in fileraw['metadata']['rowType']['fields']];frozenfiles=[dict(zip(filecols,x)) for x in fileraw['rows']];filekeys={(r['RawImpressionUploadResourceId'],r['FileResourceId'],r['BlobUri'],r['BlobGeneration']) for r in frozenfiles};parentresources={r['RawImpressionUploadId']:r['RawImpressionUploadResourceId'] for r in frozenE['raw_uploads']}
childfiles=guard(m,'raw-child-file-closure','SELECT DataProviderResourceId,RawImpressionUploadId,FileId,FileResourceId,BlobUri,BlobGeneration FROM RawImpressionUploadFile WHERE '+childscope)
assert all((parentresources[r['RawImpressionUploadId']],r['FileResourceId'],r['BlobUri'],r['BlobGeneration']) in filekeys for r in childfiles),'Unknown new raw file cascade row preserved'
childjobs=guard(m,'raw-child-vl-job-closure','SELECT DataProviderResourceId,RawImpressionUploadId,VidLabelingJobId,VidLabelingJobResourceId,CmmsModelLines,RawImpressionUploadFiles,CAST(State AS INT64) State FROM VidLabelingJob WHERE '+childscope)
assert {tuple(str(r[c]) for c in pk['edp-aggregator']['VidLabelingJob']) for r in childjobs}<={tuple(str(r[c]) for c in pk['edp-aggregator']['VidLabelingJob']) for r in frozenE['jobs']}
for table in ['PoolAssignmentJob','RankerJob','RankIndexBlob']:guard(m,'nonmemoized-'+table,'SELECT '+','.join(pk['edp-aggregator'][table])+' FROM '+table+' WHERE '+childscope,True)
guard(m,'no-active-provider-fence','SELECT DataProviderResourceId,EvictionOperationId,State,Etag FROM VidLabelingEvictionFence WHERE '+DP,True)
guard(m,'preserve-unowned-provider-leases','SELECT DataProviderResourceId,SynchronizationAttemptId,CAST(State AS INT64) State,ExpireTime,MutationRequestIds FROM DataAvailabilitySyncLease WHERE '+DP+' AND SynchronizationAttemptId NOT IN UNNEST('+ary(leases)+')')
(OUT/'edpa-delete-manifest.json').write_text(json.dumps(m,indent=2)+'\n')
# CP exact typed-verified parent union; the attempt/publication closure is fenced live.
m=build('secure-computation');guard(m,'live-primary-key-schema',catalog)
guard(m,'no-new-owned-source-parents','SELECT WorkItemId,WorkItemResourceId,SourceDataPath,SourceObjectGeneration FROM WorkItems WHERE '+graph.cp_source_where()+' AND WorkItemId NOT IN ('+cpids+')',True)
add(m,'WorkItems','typed-owned-cp-parents','SELECT WorkItemId,WorkItemResourceId,State,Generation,SourceDataPath,SourceObjectGeneration,TO_HEX(SHA256(WorkItemParams)) ImmutableParamsSha256 FROM WorkItems WHERE WorkItemId IN ('+cpids+')',lambda r:r['State'] not in ['1','2'] and next(p['db'] for p in cp['parents'] if p['db']['WorkItemId']==r['WorkItemId'])=={k:v for k,v in r.items() if k!='ImmutableParamsSha256'} and next(p['immutableParamsSha256'] for p in cp['parents'] if p['db']['WorkItemId']==r['WorkItemId'])==r['ImmutableParamsSha256'],s.P/'outside-gcs-cleanup-prep/root-owned-complete-cp-proof.json')
freshattempts=guard(m,'owned-attempt-terminal-closure','SELECT WorkItemId,WorkItemAttemptId,WorkItemAttemptResourceId,State,Generation FROM WorkItemAttempts WHERE WorkItemId IN ('+cpids+')');assert all(r['State']!='1' for r in freshattempts)
assert {tuple(str(r[c]) for c in ['WorkItemId','WorkItemAttemptId','WorkItemAttemptResourceId']) for r in freshattempts}<={tuple(str(r[c]) for c in ['WorkItemId','WorkItemAttemptId','WorkItemAttemptResourceId']) for r in frozenCP['attempts']}
guard(m,'owned-publications-zero','SELECT * FROM WorkItemPublications WHERE WorkItemId IN ('+cpids+')',True)
(OUT/'cp-delete-manifest.json').write_text(json.dumps(m,indent=2)+'\n')
# Kingdom actual API/ledger source-backed roots, no shared model or provider deletion.
m=build('kingdom');guard(m,'live-primary-key-schema',catalog)
for label,table in [('measurements','Measurements'),('availability','DataProviderAvailabilityIntervals'),('event_groups','EventGroups'),('rollouts','ModelRollouts'),('model_lines','ModelLines'),('model_shard','ModelShards'),('model_release','ModelReleases')]:
 old=k[label];pkcols=pk['kingdom'][table];expected={tuple(str(r[c]) for c in pkcols) for r in old}
 add(m,table,'owned-'+label,k['queries'][label],lambda r,expected=expected,pkcols=pkcols:tuple(str(r[c]) for c in pkcols) in expected,F/'kingdom-owned-live-inventory.json')
freshactivities=guard(m,'owned-eventgroup-activity-closure',k['queries']['activities']);activitycols=pk['kingdom']['EventGroupActivities'];assert {tuple(str(r[c]) for c in activitycols) for r in freshactivities}<={tuple(str(r[c]) for c in activitycols) for r in frozenK['activities']}
for label in ['external_holdback_refs','external_release_rollouts','external_shard_release_refs','external_previous_rollout_refs']:guard(m,'zero-'+label,k['queries'][label],True)
for label in ['protected_shared_model_release','protected_shared_population']:assert len(guard(m,label,k['queries'][label]))==1
assert all(r['ExternalComputationId'] is None for r in k['measurements'])
(OUT/'kingdom-delete-manifest.json').write_text(json.dumps(m,indent=2)+'\n')
print('EXACT_MANIFESTS_PREPARED_NO_MUTATION',flush=True)
