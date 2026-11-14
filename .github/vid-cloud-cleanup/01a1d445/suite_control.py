"""Fixed DEV existing-WIF transport. Tokens stay only in process memory and kubectl's auth pipe."""
import json,subprocess,shutil,os,datetime,time,urllib.request,urllib.parse,urllib.error
from pathlib import Path
P=Path(__file__).resolve().parent;R='/tmp/vid-cloud-data-01a1d445';REPO=os.environ['GITHUB_WORKSPACE'];assert os.environ.get('GOOGLE_CLOUD_PROJECT')=='halo-cmm-dev';_token=None;_token_time=0

def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def run(args):
 x=subprocess.run(args,text=True,capture_output=True)
 if x.returncode:raise RuntimeError(x.stderr[-3500:]+x.stdout[-1500:])
 return x.stdout
def token():
 global _token,_token_time
 if _token is None or time.monotonic()-_token_time>240:
  _token=run(['gcloud','auth','print-access-token','--project=halo-cmm-dev']).strip();_token_time=time.monotonic();assert _token
 return _token
def http(method,url,body=None,content_type='application/json'):
 u=urllib.parse.urlparse(url);assert u.scheme=='https';host=u.hostname;path=u.path;allowed=False
 if method=='GET':
  allowed=host in ['run.googleapis.com','compute.googleapis.com','cloudscheduler.googleapis.com','container.googleapis.com','spanner.googleapis.com'] and '/projects/halo-cmm-dev/' in path
  if host=='storage.googleapis.com':allowed=any(path==('/storage/v1/b/'+b) or path.startswith('/storage/v1/b/'+b+'/') for b in ['secure-computation-storage-dev-bucket','edpa-configs-storage-dev-bucket'])
 if host=='compute.googleapis.com' and method=='POST':allowed=path.startswith('/compute/v1/projects/halo-cmm-dev/') and path.endswith('/listManagedInstances')
 if host=='spanner.googleapis.com' and path.startswith('/v1/projects/halo-cmm-dev/instances/dev-instance/databases/'):
  if method=='POST' and path.endswith('/sessions'):allowed=True
  if method=='POST' and path.endswith(':executeSql'):
   parsed=json.loads(body);allowed=parsed['transaction']=={'singleUse':{'readOnly':{}}} and parsed['sql'].strip().upper().startswith('SELECT ')
  if method=='DELETE' and '/sessions/' in path:allowed=True
 assert allowed,(method,host,path)
 req=urllib.request.Request(url,data=body,headers={'Authorization':'Bearer '+token(),'Content-Type':content_type},method=method)
 try:
  with urllib.request.urlopen(req,timeout=90) as response:return response.read()
 except urllib.error.HTTPError as e:raise RuntimeError('API HTTP '+str(e.code)+': '+e.read().decode()) from None
def remote(args):
 args=[str(x) for x in args]
 if args[:2]==['java','@'+R+'/java-gcp-http.args']:
  method,url,path,ctype=args[2:];body=None if path=='-' else Path(path).read_bytes();return http(method,url,body,ctype).decode()
 assert args[0] in ['java','javac','jar','python3','kubectl','gcloud'];return run(args)
def scp(local,dest):
 dest=Path(dest);assert dest.resolve().is_relative_to(Path(R).resolve());dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(local,dest);return ''
def save(name,obj):
 p=P/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(obj,indent=2)+'\n');return obj
def load():return json.load(open(P/'ledger.json'))
def edit(fn):value=load();fn(value);save('ledger.json',value)
def intent(label,data):edit(lambda x:x['mutation_intents'].append({'label':label,'time':now(),**data}))
def api(label,method,url,body=None,content_type='application/json'):
 raw=http(method,url,None if body is None else json.dumps(body).encode(),content_type);return save(label+'-api-result.json',json.loads(raw or b'{}'))
def query(label,db,sql):
 assert db in ['edp-aggregator','secure-computation','kingdom','reporting'];assert sql.strip().upper().startswith('SELECT ')
 base='https://spanner.googleapis.com/v1/projects/halo-cmm-dev/instances/dev-instance/databases/'+db;session=json.loads(http('POST',base+'/sessions',b'{}'))['name']
 try:raw=json.loads(http('POST','https://spanner.googleapis.com/v1/'+session+':executeSql',json.dumps({'sql':sql,'transaction':{'singleUse':{'readOnly':{}}}}).encode()))
 finally:http('DELETE','https://spanner.googleapis.com/v1/'+session)
 save(label+'.json',raw);fields=[x['name'] for x in raw['metadata']['rowType']['fields']];return [dict(zip(fields,row+[None]*(len(fields)-len(row)))) for row in raw.get('rows',[])]
