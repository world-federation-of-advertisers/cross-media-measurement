"""Fresh exact-key graph inventory; no opaque WorkItem parameters or credential exports."""
import json,hashlib,uuid,collections,urllib.parse,subprocess,shutil
from pathlib import Path
import suite_control as s
P=s.P;R=Path(s.R);BOOT='test-operator-artifacts/vid-cloud-20261113/generator/bootstrap-bundle.tar.gz'
def read(p):return json.load(open(p))
def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def cp_source_where():
 private=['gs://secure-computation-storage-dev-bucket/'+v for v in ['raw-impressions/edp/edp7/vid-cloud-20261113/','vid-labeled-impressions/edp/edp7/vid-cloud-20261113/','external-impressions/edp/edp7/vid-cloud-20261113/','test-operator-artifacts/vid-cloud-20261113/']]
 outside=read(P/'outside-gcs-cleanup-prep/root-outside-gcs-owned-manifest.json');names=[row['name'] for row in outside['allowlist']];assert len(names)==len(set(names))==19 and 'edp7/event-groups/vid-cloud-20261113-event-groups.json' in names
 uris=['gs://secure-computation-storage-dev-bucket/'+name for name in sorted(names)];assert all("'" not in uri for uri in uris)
 return '('+' OR '.join("STARTS_WITH(SourceDataPath,'"+uri+"')" for uri in private)+" OR SourceDataPath IN UNNEST(["+','.join("'"+uri+"'" for uri in uris)+']))'
def cp_refresh():
 frozen=read(P/'cleanup-final-before-main/cp-owned-complete-union.json');assert frozen['parentCount']==195 and frozen['attemptCount']==221
 source=P/'outside-gcs-cleanup-prep/root-owned-complete-cp-proof.json';proof=read(source);known={x['db']['WorkItemResourceId']:x for x in proof['verifiedWorkItems']};assert len(known)==195 and not proof['unknownWorkItemsPreserved']
 where="WorkItemResourceId IN UNNEST(["+','.join("'"+v+"'" for v in sorted(known))+']) OR '+cp_source_where()
 rows=s.query('ci-owned-cp-fresh-parents','secure-computation',"SELECT WorkItemId,WorkItemResourceId,State,Generation,SourceDataPath,SourceObjectGeneration,TO_HEX(SHA256(WorkItemParams)) ImmutableParamsSha256 FROM WorkItems WHERE "+where)
 assert len(rows)==195 and {str(x['WorkItemId']) for x in rows}=={str(x['db']['WorkItemId']) for x in frozen['parents']}
 parents=[]
 for row in rows:
  old=known[row['WorkItemResourceId']];paramsha=row.pop('ImmutableParamsSha256')
  assert all(row[k]==old['db'][k] for k in ['WorkItemId','WorkItemResourceId','SourceDataPath','SourceObjectGeneration']);assert row['State'] not in ['1','2']
  item=dict(old);item['db']=row;item['state']={'3':'SUCCEEDED','4':'FAILED'}[row['State']];item['generation']=row['Generation'];item['ownershipReuse']='Prior actual typed proof; same fresh internal parent/source identity, immutable params';item['immutableParamsSha256']=paramsha;parents.append(item)
 ids=','.join(str(int(p['db']['WorkItemId'])) for p in parents)
 attempts=s.query('ci-owned-cp-fresh-attempts','secure-computation','SELECT W.WorkItemResourceId,A.WorkItemId,A.WorkItemAttemptId,A.WorkItemAttemptResourceId,A.State,A.Generation,A.CreateTime,A.UpdateTime,A.LeaseExpirationTime FROM WorkItemAttempts A JOIN WorkItems W USING(WorkItemId) WHERE A.WorkItemId IN ('+ids+')');assert all(x['State']!='1' for x in attempts)
 attemptkeys=lambda data:{tuple(str(x[k]) for k in ['WorkItemId','WorkItemAttemptId','WorkItemAttemptResourceId']) for x in data}
 assert attemptkeys(attempts)<=attemptkeys(frozen['attempts']),'Unknown new CP attempt preserved; frozen closure exceeded'
 pubs=s.query('ci-owned-cp-fresh-publications','secure-computation','SELECT * FROM WorkItemPublications WHERE WorkItemId IN ('+ids+')');assert pubs==[]
 leases=[]
 for a in attempts:
  if a['WorkItemAttemptResourceId'].startswith('data-availability-'):
   rid=a['WorkItemAttemptResourceId'].removeprefix('data-availability-');uuid.UUID(rid);leases.append({'dataProvider':'T5RryPMNong','synchronizationAttemptId':rid,'workItem':a['WorkItemResourceId'],'attempt':a['WorkItemAttemptResourceId']})
 assert {x['synchronizationAttemptId'] for x in leases}=={x['synchronizationAttemptId'] for x in frozen['dataAvailabilityLeaseOwnerClosure']}
 return s.save('outside-gcs-cleanup-prep/root-owned-complete-cp-union-fresh.json',{'project':'halo-cmm-dev','session':'01a1d445','kind':'COMPLETE_OWNED_CP_UNION','parents':parents,'attempts':attempts,'publications':pubs,'dataAvailabilityLeaseOwnerClosure':leases,'parentCount':len(parents),'attemptCount':len(attempts),'publicationCount':0,'unknownOwnership':[],'sourceProofSha256':digest(source)})
def private_inventory(label):
 out=R/('cleanup-'+label+'-gcs.json');print(s.remote(['java','@'+s.R+'/java-capture-owned-gcs.args',str(out)]),flush=True);x=read(out);s.save('cleanup-final-'+label+'/gcs-versions.json',x)
 frozen=read(P/'cleanup-final-before-main/gcs-versions.json');keys={(o['bucket'],o['name'],o['generation'],o['metageneration']) for o in frozen['readable']};unknown=[o for o in x['readable'] if (o['bucket'],o['name'],o['generation'],o['metageneration']) not in keys];assert unknown==[], 'Unknown new versions preserved; no scope expansion';return x

def outside_inventory():
 old=read(P/'outside-gcs-cleanup-prep/root-outside-gcs-owned-manifest.json');new=[];base='https://storage.googleapis.com/storage/v1/b/secure-computation-storage-dev-bucket/o';pins={(r['name'],str(r['generation'])):r for r in old['objects']};assert len(old['allowlist'])==19
 for allow in old['allowlist']:
  q=urllib.parse.urlencode({'prefix':allow['name'],'versions':'true','maxResults':'1000'});listing=s.api('ci-outside-current-'+hashlib.sha256(allow['name'].encode()).hexdigest()[:10],'GET',base+'?'+q);assert not listing.get('nextPageToken')
  for current in listing.get('items',[]):
   if current['name']!=allow['name']:continue
   pin=pins[(current['name'],current['generation'])]
   for k in ['crc32c','generation','metageneration']:assert current[k]==pin[k]
   body=s.http('GET',base+'/'+urllib.parse.quote(current['name'],safe='')+'?alt=media&generation='+current['generation']);assert len(body)==int(current['size']) and hashlib.sha256(body).hexdigest()==pin['sha256']
   row=dict(pin);row['metageneration']=current['metageneration'];row['ownershipProofSha256']=allow['ownershipProofSha256'];new.append(row)
 old['objects']=new;old['finalAfterMain']=True;old['capturedAt']=s.now();old['requiresFinalRefreshAfter2B']=False;return s.save('outside-gcs-cleanup-prep/root-outside-gcs-owned-manifest.json',old)

def prepare_private_manifests(snapshot):
 for kind,name in [('RAW_DONE_FIRST','raw-done-first.json'),('REMAINING','remaining-private.json')]:
  rows=[o for o in snapshot['readable'] if (o['name'].startswith('raw-impressions/edp/edp7/vid-cloud-20261113/') and o['name'].endswith('/done'))==(kind=='RAW_DONE_FIRST') and o['name']!=BOOT]
  s.save('cleanup-final-delete/'+name,{'project':'halo-cmm-dev','session':'01a1d445','kind':kind,'objects':rows,'knownFrozenScopeSha256':digest(P/'cleanup-final-before-main/gcs-versions.json')})
 boot=[o for o in snapshot['readable'] if o['name']==BOOT];assert len(boot)==1 and boot[0]['generation']=='1794622993228908';s.save('cleanup-final-delete/held-bootstrap.json',{'project':'halo-cmm-dev','session':'01a1d445','kind':'REMAINING','objects':boot,'reason':'Only existing trusted libraries retained until final runner loads them'})
