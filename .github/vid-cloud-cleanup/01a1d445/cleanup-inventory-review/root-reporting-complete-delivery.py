import base64,datetime,hashlib,json,pathlib,subprocess,time,os
P=pathlib.Path('/tmp/vid-cloud-data-01a1d445');KEY='.vid-cloud-20261113-cleanup-inventory.jar';PROJECTION='/etc/halo-cmms/config-files/'+KEY
proof=json.load(open(P/'root-reporting-complete-delivery-proof.json'));pod=json.load(open(P/'root-reporting-target-pod.json'));MAP=proof['map'];POD=pod['metadata']['name'];CONTAINER='postgres-internal-reporting-server-container'
JOURNAL=P/'root-reporting-complete-delivery-journal.json';journal={'operation':'READ_ONLY_INVENTORY','scope':'only '+MAP+'/binaryData/'+KEY+' and read-only existing pod child JVM','events':[]}
def record(event,**data):
 journal['events'].append({'time':datetime.datetime.now(datetime.timezone.utc).isoformat(),'event':event,**data});JOURNAL.write_text(json.dumps(journal,indent=2)+'\n');JOURNAL.chmod(0o600)
 with JOURNAL.open('rb') as stream:os.fsync(stream.fileno())
 descriptor=os.open(str(JOURNAL.parent),os.O_RDONLY);os.fsync(descriptor);os.close(descriptor)
def kube(args,timeout=90):return subprocess.run(['kubectl','--kubeconfig',str(P/'root-reporting-kubeconfig.json')]+args,capture_output=True,text=True,timeout=timeout)
def getmap():
 r=kube(['get','configmap',MAP,'-o','json']);r.check_returncode();return json.loads(r.stdout)
def hashes(cm):
 out={'data/'+k:hashlib.sha256(v.encode()).hexdigest() for k,v in cm.get('data',{}).items()}
 out.update({'binaryData/'+k:hashlib.sha256(base64.b64decode(v)).hexdigest() for k,v in cm.get('binaryData',{}).items() if k!=KEY});return out
def same(cm):
 assert cm['metadata']['uid']==proof['uid'] and hashes(cm)==proof['preservedHashes'];assert not cm.get('immutable',False)
def patch(cm,op):
 f=P/('root-reporting-complete-'+op['op']+'-patch.json');body=[{'op':'test','path':'/metadata/resourceVersion','value':cm['metadata']['resourceVersion']},op];f.write_text(json.dumps(body));f.chmod(0o600)
 record('PATCH_INTENT',operation=op['op'],key=KEY,resourceVersion=cm['metadata']['resourceVersion'],jarSha256=proof['jarSha256'])
 r=kube(['patch','configmap',MAP,'--type=json','--patch-file',str(f),'-o','json']);
 (P/('root-reporting-complete-'+op['op']+'-stderr.log')).write_text(r.stderr)
 if r.returncode:
  record('PATCH_REJECTED',returncode=r.returncode,error=r.stderr);raise RuntimeError(r.stderr)
 out=json.loads(r.stdout);same(out);record('PATCH_SUCCEEDED',operation=op['op'],resourceVersion=out['metadata']['resourceVersion']);return out
def probe():return kube(['exec',POD,'-c',CONTAINER,'--','java','-cp',PROJECTION,'ReportingCleanupProjection'])
added=False;add_attempted=False
try:
 cm=getmap();same(cm);assert KEY not in cm.get('data',{}) and KEY not in cm.get('binaryData',{})
 freshpod=kube(['get','pod',POD,'-o','json']);freshpod.check_returncode();assert json.loads(freshpod.stdout)['metadata']['uid']==pod['metadata']['uid']
 jar=(P/'root-reporting-cleanup-inventory.jar').read_bytes();assert hashlib.sha256(jar).hexdigest()==proof['jarSha256']
 add_attempted=True;record('ADD_ATTEMPTED_BEFORE_REQUEST',key=KEY,jarSha256=proof['jarSha256'])
 cm=patch(cm,{'op':'add','path':'/binaryData/'+KEY,'value':base64.b64encode(jar).decode()});added=True
 deadline=time.monotonic()+180
 while True:
  r=probe()
  if r.returncode==0:assert r.stdout.strip()=='PROJECTION_AND_RUNTIME_PRESENT';break
  if time.monotonic()>=deadline:raise TimeoutError('ConfigMap projection/runtime unavailable: '+r.stderr)
  time.sleep(10)
 record('PROJECTION_VERIFIED',pod=POD,podUid=pod['metadata']['uid'],runtimeJar='/GCloudInternalReportingServer_deploy.jar')
 record('READ_ONLY_INVENTORY_INTENT',fixedScope='VCTqwV_vFXw/vid-cloud-20261113-',jvm='child-only',transaction='REPEATABLE READ READ ONLY; ROLLBACK; close; System.exit')
 try:
  r=kube(['exec',POD,'-c',CONTAINER,'--','java','-cp',PROJECTION+':/GCloudInternalReportingServer_deploy.jar','ReportingCleanupInventory'],timeout=300)
 except BaseException:
  if journal['operation']=='EXACT_REVIEWED_DELETION':record('RECONCILE_REQUIRED_NO_AUTO_RETRY',reason='Child execution transport failed; transaction outcome unknown')
  raise
 (P/'root-reporting-live-inventory-complete.stdout.json').write_text(r.stdout);(P/'root-reporting-live-inventory-complete.stderr.log').write_text(r.stderr)
 record('INVENTORY_EXIT',returncode=r.returncode)
 r.check_returncode();inventory=json.loads(r.stdout);assert inventory['read_only'] is True and inventory['scope']=='VCTqwV_vFXw/vid-cloud-20261113-'
 print('INVENTORY_SUCCESS',json.dumps({k:len(v) for k,v in inventory.items() if isinstance(v,list)}),flush=True)
finally:
 if add_attempted:
  if not added:record('ADD_OUTCOME_UNCERTAIN_RECONCILE',key=KEY)
  cm=getmap();same(cm)
  if KEY in cm.get('binaryData',{}) or KEY in cm.get('data',{}):
   if KEY not in cm.get('binaryData',{}) or hashlib.sha256(base64.b64decode(cm['binaryData'][KEY])).hexdigest()!=proof['jarSha256']:
    record('KEY_IDENTITY_MISMATCH_PRESERVED',key=KEY);raise AssertionError('Unexpected key content preserved; manual reconciliation required')
   cm=patch(cm,{'op':'remove','path':'/binaryData/'+KEY});assert KEY not in cm.get('binaryData',{}) and KEY not in cm.get('data',{})
  else:record('OWNED_KEY_ALREADY_ABSENT_AFTER_ADD_RECONCILE',key=KEY)
  deadline=time.monotonic()+180
  while True:
   r=probe()
   if r.returncode!=0 and 'ClassNotFoundException: ReportingCleanupProjection' in r.stderr:break
   if time.monotonic()>=deadline:raise TimeoutError('Projected key remains visible')
   time.sleep(10)
  after=getmap();same(after);assert KEY not in after.get('binaryData',{}) and KEY not in after.get('data',{})
  record('RESTORE_VERIFIED',keyAbsent=True,projectedKeyAbsent=True,allEightPreservedHashesMatch=True,resourceVersion=after['metadata']['resourceVersion'])
  print('RESTORE_VERIFIED',flush=True)
