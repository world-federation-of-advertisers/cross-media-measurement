"""Export only named cleanup receipts; never export credential or config delivery files."""
import hashlib,json,os,pathlib,re,shutil
import suite_control as s
P=s.P;MODE=os.environ['CI_CLEANUP_MODE'];OUT=P/'noncredential-receipts'
EXACT=[
 'cleanup-restoration-acceptance.json','cleanup-raw-done-absence.json','cleanup-final-verification.json',
 'cleanup-owned-template-removal.json','cleanup-owned-template-delete-intent.json','cleanup-owned-template-delete-operation.json','ci-config-bucket-policy-api-result.json',
 *['cleanup-final-'+phase+'/gcs-versions.json' for phase in ['after-main','after-gcs','after-raw-done','final']],
 *['cleanup-final-after-main/'+name for name in ['edpa-owned-live-inventory.json','kingdom-owned-live-inventory.json','cp-direct-source-RF-roots.json']],
 *['cleanup-final-delete/'+name for name in ['raw-done-first.json','remaining-private.json','held-bootstrap.json','cp-delete-manifest.json','edpa-delete-manifest.json','kingdom-delete-manifest.json','owned-fence-mutation-ids.json']],
 *['outside-gcs-cleanup-prep/'+name for name in ['root-owned-complete-cp-proof.json','root-owned-complete-cp-union-fresh.json','root-outside-gcs-owned-manifest.json','root-outside-gcs-helper-binding.json']],
 *['cleanup-inventory-review/'+name for name in ['root-reporting-live-inventory-closure.json','root-reporting-ownership-anchors.json','root-reporting-owned-key-manifest.json','root-reporting-scoped-delete.sql','ReportingCleanupDelete.java','root-reporting-complete-delivery-proof.json','root-reporting-complete-delivery-journal.json','root-reporting-delete-result.json','root-reporting-delete-delivery-journal.json','edp-aggregator-live-ddl.json','secure-computation-live-ddl.json','kingdom-live-ddl.json']]]
JOURNALS=[
 re.compile(r'cleanup-(?:gcs-reporting|spanner)-(?:raw-done-first|remaining-private|held-bootstrap)\.json-[0-9a-f]{32}\.json'),
 re.compile(r'cleanup-spanner-(?:cp|edpa|kingdom)-delete-manifest\.json-[0-9a-f]{32}\.json'),
 re.compile(r'root-outside-gcs-delete-journal-[0-9a-f]{32}\.json')]
FORBIDDEN={'accesstoken','refreshtoken','idtoken','authorization','clientsecret','privatekey','credential','credentials','credentialsource','kubeconfig','workitemparams','decodedparams','opaqueparams','binarydata'}
SECRET=[re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),re.compile(r'ya29\.[A-Za-z0-9_-]{15,}'),re.compile(r'1//[A-Za-z0-9_-]{25,}'),re.compile(r'eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}')]
def validate(value):
 if isinstance(value,dict):
  for key,item in value.items():
   assert isinstance(key,str) and re.sub('[^a-z]','',key.lower()) not in FORBIDDEN,'Excluded receipt field: '+key
   validate(item)
 elif isinstance(value,list):
  for item in value:validate(item)
 elif isinstance(value,str):assert not any(pattern.search(value) for pattern in SECRET),'Credential literal excluded'
 elif value is not None:assert isinstance(value,(int,float,bool))
def validate_type(rel,value):
 validate(value)
 if rel.endswith('gcs-versions.json'):assert isinstance(value['readable'],list) and isinstance(value['retainedSoftDeleted'],list)
 if rel.endswith('delete-manifest.json'):
  assert value['kind']=='EXACT_OWNED_SPANNER_GRAPH' and value['project']=='halo-cmm-dev' and value['session']=='01a1d445'
  for row in value['deletes']:assert isinstance(row['table'],str) and all(all(isinstance(part,str) for part in key) for key in row['keys'])
 if rel.endswith('root-reporting-live-inventory-closure.json'):
  assert value['read_only'] is True and value['scope']=='VCTqwV_vFXw/vid-cloud-20261113-'
  for rows in value.values():
   if isinstance(rows,list):
    for row in rows:
     if isinstance(row,dict):assert all(not key.endswith('id') or item is None or isinstance(item,str) for key,item in row.items())
 if 'delivery-journal' in rel:assert isinstance(value['events'],list) and all(isinstance(event.get('event'),str) for event in value['events'])
def root_artifacts():
 files=[P/name for name in EXACT if (P/name).is_file()]
 base=P/'cleanup-final-delete'
 if base.exists():
  files.extend(f for f in base.iterdir() if f.is_file() and any(pattern.fullmatch(f.name) for pattern in JOURNALS))
 index=[]
 for f in sorted(set(files)):
  rel=str(f.relative_to(P));raw=f.read_bytes();assert not any(pattern.search(raw.decode()) for pattern in SECRET)
  if f.suffix=='.json':validate_type(rel,json.loads(raw))
  else:assert f.name in ['root-reporting-scoped-delete.sql','ReportingCleanupDelete.java']
  dest=OUT/rel;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(raw);index.append({'path':rel,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)})
 OUT.mkdir(exist_ok=True);(OUT/'receipt-index.json').write_text(json.dumps({'project':'halo-cmm-dev','session':'01a1d445','mode':MODE,'files':index,'credentialsExported':False},indent=2)+'\n')
