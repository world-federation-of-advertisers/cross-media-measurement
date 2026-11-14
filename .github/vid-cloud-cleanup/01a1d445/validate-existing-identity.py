"""Verify the existing action's WIF override in memory; never copy/export its credentials."""
import json,os,pathlib,subprocess,urllib.parse
assert os.environ['EXPECTED_PROJECT']==os.environ['GOOGLE_CLOUD_PROJECT']==os.environ['CLOUDSDK_CORE_PROJECT']=='halo-cmm-dev'
expected=os.environ['EXPECTED_SERVICE_ACCOUNT'];assert expected=='terraform@halo-cmm-dev.iam.gserviceaccount.com' and expected.endswith('@halo-cmm-dev.iam.gserviceaccount.com') and 'prod' not in expected
paths=[pathlib.Path(os.environ[name]).resolve(strict=True) for name in ['GOOGLE_GHA_CREDS_PATH','GOOGLE_APPLICATION_CREDENTIALS','CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE']];assert len(set(paths))==1 and paths[0].is_file()
assert paths[0].is_relative_to(pathlib.Path(os.environ['GITHUB_WORKSPACE']).resolve()) and paths[0].name.startswith('gha-creds-') and paths[0].suffix=='.json'
credential=json.loads(paths[0].read_text());assert credential['type']=='external_account' and credential['token_url']=='https://sts.googleapis.com/v1/token' and credential['subject_token_type']=='urn:ietf:params:oauth:token-type:jwt'
provider=os.environ['EXPECTED_WIF_PROVIDER'];assert provider.startswith('projects/') and credential['audience']=='//iam.googleapis.com/'+provider
url=urllib.parse.urlparse(credential['service_account_impersonation_url']);assert url.scheme=='https' and url.netloc=='iamcredentials.googleapis.com' and not url.query and not url.fragment and urllib.parse.unquote(url.path)=='/v1/projects/-/serviceAccounts/'+expected+':generateAccessToken'
assert credential.get('universe_domain','googleapis.com')=='googleapis.com' and credential.get('quota_project_id','halo-cmm-dev')=='halo-cmm-dev'
assert os.environ.get('CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT',expected)==expected
active=subprocess.run(['gcloud','auth','list','--filter=status:ACTIVE','--format=value(account)'],capture_output=True,text=True,check=True).stdout.strip();assert active in ['',expected]
result=subprocess.run(['gcloud','auth','print-access-token','--project=halo-cmm-dev'],capture_output=True,text=True)
if result.returncode or not result.stdout.strip():raise RuntimeError('Validated existing WIF token retrieval failed; no credential data exported')
P=pathlib.Path(__file__).resolve().parent;out=P/'noncredential-receipts';out.mkdir(exist_ok=True);(out/'existing-identity.json').write_text(json.dumps({'project':'halo-cmm-dev','identity':expected,'existingMainWif':True,'verification':'Exact action external-account audience/TF impersonation target and working normal override','credentialsExported':False},indent=2)+'\n');print('EXACT_EXISTING_DEV_WIF_IDENTITY_VALIDATED',expected)
