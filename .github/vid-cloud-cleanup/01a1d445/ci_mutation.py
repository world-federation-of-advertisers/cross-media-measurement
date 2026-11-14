import json,sys,os,pathlib,subprocess,shutil,hashlib,urllib.parse,time,uuid,urllib.request
import suite_control as s
import ci_graph as graph
import ci_reporting as pg
P=s.P;R=pathlib.Path(s.R);MODE=os.environ['CI_CLEANUP_MODE'];OUT=P/'noncredential-receipts'
from ci_inventory import read,sha,child
def exact_delete_private(name):
 path=P/'cleanup-final-delete'/name;before=read(path);assert before['project']=='halo-cmm-dev' and before['session']=='01a1d445';digest=sha(path);journal=R/('cleanup-'+MODE+'-'+name+'-'+uuid.uuid4().hex+'.json')
 try:print(s.remote(['java','@'+s.R+'/java-delete-owned-gcs.args',str(path),digest,'--execute',str(journal)]),flush=True)
 finally:
  if journal.exists():s.save('cleanup-final-delete/'+journal.name,read(journal))

def raw_barrier(snapshot):
 count=sum(x['name'].startswith('raw-impressions/edp/edp7/vid-cloud-20261113/') and x['name'].endswith('/done') for x in snapshot['readable']);assert count==0;s.save('cleanup-raw-done-absence.json',{'project':'halo-cmm-dev','session':'01a1d445','readableRawDoneVersions':0,'time':s.now(),'inventorySha256':sha(P/'cleanup-final-after-raw-done/gcs-versions.json')})

def plan():
 value=read(P/('execution-plan-'+MODE+'.json'));assert sha(P/('execution-plan-'+MODE+'.json'))==os.environ['CI_EXECUTION_PLAN_SHA'] and value['reviewedByRoot'];return value

def execute_outside():
 path=P/'outside-gcs-cleanup-prep/root-outside-gcs-owned-manifest.json';src=P/'outside-gcs-cleanup-prep/DeleteOwnedOutsideGcsVersions.java';classpath=(R/'trusted-classpath.txt').read_text();subprocess.run(['javac','--release','17','-cp',classpath,'-d',str(R/'compiled'),str(src)],check=True);args=R/'java-delete-outside.args';args.write_text('-Xmx512m\n-cp\n"'+classpath+'"\nDeleteOwnedOutsideGcsVersions\n');journal=R/('root-outside-gcs-delete-journal-'+uuid.uuid4().hex+'.json');
 try:print(s.remote(['java','@'+str(args),str(path),sha(path),'--execute',str(journal)]),flush=True)
 finally:
  if journal.exists():s.save('cleanup-final-delete/'+journal.name,read(journal))
