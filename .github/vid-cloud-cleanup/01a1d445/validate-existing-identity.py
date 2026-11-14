import subprocess,os,json,pathlib
assert os.environ['EXPECTED_PROJECT']=='halo-cmm-dev';expected=os.environ['EXPECTED_SERVICE_ACCOUNT'];assert expected.endswith('@halo-cmm-dev.iam.gserviceaccount.com') and 'prod' not in expected
actual=subprocess.check_output(['gcloud','auth','list','--filter=status:ACTIVE','--format=value(account)'],text=True).strip();assert actual==expected,(actual,expected)
P=pathlib.Path(__file__).resolve().parent;out=P/'noncredential-receipts';out.mkdir(exist_ok=True);(out/'existing-identity.json').write_text(json.dumps({'project':'halo-cmm-dev','identity':actual,'existingMainWif':True,'credentialsExported':False},indent=2)+'\n');print('EXISTING_DEV_WIF_IDENTITY_VALIDATED',actual)
